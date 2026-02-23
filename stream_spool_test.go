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
