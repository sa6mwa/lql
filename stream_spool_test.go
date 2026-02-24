package lql

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestNormalizeStreamSpoolConfigDefaults(t *testing.T) {
	cfg := normalizeStreamSpoolConfig(0, "", "")
	if cfg.memoryBytes != defaultStreamSpoolMemoryBytes {
		t.Fatalf("memory default mismatch: got %d want %d", cfg.memoryBytes, defaultStreamSpoolMemoryBytes)
	}
	if cfg.tempDir != defaultStreamSpoolTempDir {
		t.Fatalf("temp dir default mismatch: got %q want %q", cfg.tempDir, defaultStreamSpoolTempDir)
	}
	if cfg.filePattern != defaultStreamSpoolPattern {
		t.Fatalf("pattern default mismatch: got %q want %q", cfg.filePattern, defaultStreamSpoolPattern)
	}
}

func TestStreamCandidateSpoolInMemoryLifecycle(t *testing.T) {
	cfg := normalizeStreamSpoolConfig(1024, t.TempDir(), "spool-*.json")
	s := newStreamCandidateSpool(cfg, 32)
	defer func() {
		_ = s.Cleanup()
	}()

	payload := []byte(`{"id":"a"}`)
	if _, err := s.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := s.PayloadBytes(); string(got) != string(payload) {
		t.Fatalf("payload mismatch: got %q want %q", string(got), string(payload))
	}

	rc, err := s.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("open payload mismatch: got %q want %q", string(got), string(payload))
	}
}

func TestStreamCandidateSpoolSpillToDiskAndCleanup(t *testing.T) {
	tempDir := t.TempDir()
	cfg := normalizeStreamSpoolConfig(8, tempDir, "spool-*.json")
	s := newStreamCandidateSpool(cfg, 0)

	payload := []byte(`{"id":"a","payload":"abcdefghijklmnopqrstuvwxyz"}`)
	if _, err := s.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if s.PayloadBytes() != nil {
		t.Fatalf("expected nil in-memory payload after spill")
	}
	if s.SpillCount() != 1 {
		t.Fatalf("expected one spill, got %d", s.SpillCount())
	}
	if s.SpillBytes() != int64(len(payload)) {
		t.Fatalf("spill bytes mismatch: got %d want %d", s.SpillBytes(), len(payload))
	}

	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 spool file, got %d", len(entries))
	}
	filePath := filepath.Join(tempDir, entries[0].Name())
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("expected mode 0600, got %o", info.Mode().Perm())
	}

	rc, err := s.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("spooled payload mismatch: got %q want %q", string(got), string(payload))
	}

	if err := s.Cleanup(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if s.SpillCount() != 0 || s.SpillBytes() != 0 {
		t.Fatalf("expected counters reset after cleanup: spills=%d bytes=%d", s.SpillCount(), s.SpillBytes())
	}
	if err := s.Cleanup(); err != nil {
		t.Fatalf("second cleanup should be idempotent: %v", err)
	}
	if _, err := os.Stat(filePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected removed spool file, stat err=%v", err)
	}
}

func TestStreamCandidateSpoolSpillFailure(t *testing.T) {
	cfg := normalizeStreamSpoolConfig(4, filepath.Join(t.TempDir(), "missing"), "spool-*.json")
	s := newStreamCandidateSpool(cfg, 0)
	defer func() {
		_ = s.Cleanup()
	}()

	_, err := s.Write([]byte(`{"id":"a"}`))
	if err == nil {
		t.Fatalf("expected spill failure")
	}
}

func TestStreamCandidateSpoolReusesTempFileAcrossCandidates(t *testing.T) {
	tempDir := t.TempDir()
	cfg := normalizeStreamSpoolConfig(8, tempDir, "spool-*.json")
	s := newStreamCandidateSpool(cfg, 0)
	defer func() {
		_ = s.Cleanup()
	}()

	first := []byte(`{"blob":"abcdefghijklmnopqrstuvwxyz"}`)
	if _, err := s.Write(first); err != nil {
		t.Fatalf("write first: %v", err)
	}
	if err := s.cleanup(false); err != nil {
		t.Fatalf("cleanup first candidate: %v", err)
	}
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("readdir first: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one reusable temp file after first candidate, got %d", len(entries))
	}
	fileName := entries[0].Name()

	s.resetForCandidate(cfg, 0)
	second := []byte(`{"blob":"0123456789abcdefghijklmnopqrstuvwxyz"}`)
	if _, err := s.Write(second); err != nil {
		t.Fatalf("write second: %v", err)
	}
	if err := s.cleanup(false); err != nil {
		t.Fatalf("cleanup second candidate: %v", err)
	}
	entries, err = os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("readdir second: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one reusable temp file after second candidate, got %d", len(entries))
	}
	if entries[0].Name() != fileName {
		t.Fatalf("expected file reuse, got %q then %q", fileName, entries[0].Name())
	}

	if err := s.Cleanup(); err != nil {
		t.Fatalf("final cleanup: %v", err)
	}
	entries, err = os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("readdir final: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no temp files after final cleanup, got %d", len(entries))
	}
}

func TestStreamCandidateSpoolPostSpillCanReturnToInMemoryCandidate(t *testing.T) {
	tempDir := t.TempDir()
	cfg := normalizeStreamSpoolConfig(16, tempDir, "spool-*.json")
	s := newStreamCandidateSpool(cfg, 0)
	defer func() {
		_ = s.Cleanup()
	}()

	if _, err := s.Write([]byte(`{"blob":"abcdefghijklmnopqrstuvwxyz"}`)); err != nil {
		t.Fatalf("write spill candidate: %v", err)
	}
	if err := s.cleanup(false); err != nil {
		t.Fatalf("cleanup spill candidate: %v", err)
	}

	s.resetForCandidate(cfg, 0)
	small := []byte(`{"ok":1}`)
	if _, err := s.Write(small); err != nil {
		t.Fatalf("write in-memory candidate: %v", err)
	}
	if got := s.PayloadBytes(); string(got) != string(small) {
		t.Fatalf("expected in-memory payload %q, got %q", string(small), string(got))
	}
}
