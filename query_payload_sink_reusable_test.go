package lql

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReusableQueryPayloadSinkFactoryInMemory(t *testing.T) {
	factory := NewReusableQueryPayloadSinkFactory(ReusableQueryPayloadSinkFactoryOptions{
		SpoolMemoryBytes: 1024,
	})
	t.Cleanup(func() {
		if err := factory.Close(); err != nil {
			t.Fatalf("close factory: %v", err)
		}
	})

	sink, err := factory.NewSink(QueryStreamPayloadSinkRequest{})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	if _, err := sink.Write([]byte(`{"id":"a"}`)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := sink.Finalize(); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if got := string(sink.Bytes()); got != `{"id":"a"}` {
		t.Fatalf("unexpected bytes: %q", got)
	}
	rc, err := sink.Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer rc.Close()
	payload, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read open payload: %v", err)
	}
	if got := string(payload); got != `{"id":"a"}` {
		t.Fatalf("unexpected open payload: %q", got)
	}
	if err := sink.Cleanup(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
}

func TestReusableQueryPayloadSinkFactorySpillReuseAndClose(t *testing.T) {
	tempDir := t.TempDir()
	factory := NewReusableQueryPayloadSinkFactory(ReusableQueryPayloadSinkFactoryOptions{
		SpoolMemoryBytes: 8,
		SpoolTempDir:     tempDir,
		SpoolFilePattern: "reusable-*.json",
	})

	sinkA, err := factory.NewSink(QueryStreamPayloadSinkRequest{})
	if err != nil {
		t.Fatalf("new sink A: %v", err)
	}
	typedA, ok := sinkA.(*reusableQueryPayloadSink)
	if !ok {
		t.Fatalf("unexpected sink type %T", sinkA)
	}
	if _, err := sinkA.Write([]byte(`{"blob":"` + strings.Repeat("x", 128) + `"}`)); err != nil {
		t.Fatalf("write A: %v", err)
	}
	if err := sinkA.Finalize(); err != nil {
		t.Fatalf("finalize A: %v", err)
	}
	pathA := typedA.file.Name()
	infoA, err := os.Stat(pathA)
	if err != nil {
		t.Fatalf("stat A: %v", err)
	}
	if infoA.Mode().Perm() != 0o600 {
		t.Fatalf("expected mode 0600, got %o", infoA.Mode().Perm())
	}
	if err := sinkA.Cleanup(); err != nil {
		t.Fatalf("cleanup A: %v", err)
	}

	sinkB, err := factory.NewSink(QueryStreamPayloadSinkRequest{})
	if err != nil {
		t.Fatalf("new sink B: %v", err)
	}
	typedB, ok := sinkB.(*reusableQueryPayloadSink)
	if !ok {
		t.Fatalf("unexpected sink type %T", sinkB)
	}
	if typedA != typedB {
		t.Fatalf("expected sink reuse from factory pool")
	}
	if _, err := sinkB.Write([]byte(`{"blob":"` + strings.Repeat("y", 96) + `"}`)); err != nil {
		t.Fatalf("write B: %v", err)
	}
	if err := sinkB.Finalize(); err != nil {
		t.Fatalf("finalize B: %v", err)
	}
	if typedB.file.Name() != pathA {
		t.Fatalf("expected reusable spill file path, got %q want %q", typedB.file.Name(), pathA)
	}
	if err := sinkB.Cleanup(); err != nil {
		t.Fatalf("cleanup B: %v", err)
	}

	if err := factory.Close(); err != nil {
		t.Fatalf("close factory: %v", err)
	}
	if _, err := os.Stat(pathA); !os.IsNotExist(err) {
		t.Fatalf("expected reusable spill file removed on close, stat err=%v", err)
	}
}

func TestReusableQueryPayloadSinkFactoryIntegrationWithQueryStream(t *testing.T) {
	tempDir := t.TempDir()
	factory := NewReusableQueryPayloadSinkFactory(ReusableQueryPayloadSinkFactoryOptions{
		SpoolMemoryBytes: 8,
		SpoolTempDir:     tempDir,
		SpoolFilePattern: "query-integration-*.json",
	})
	defer func() {
		if err := factory.Close(); err != nil {
			t.Fatalf("close factory: %v", err)
		}
	}()

	selector, err := ParseSelectorString(`/event="tabs_update"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}

	input := strings.NewReader(
		`{"event":"tabs_update","blob":"` + strings.Repeat("x", 2048) + `"}` + "\n" +
			`{"event":"other","blob":"` + strings.Repeat("y", 2048) + `"}`,
	)
	var callbacks int
	err = QueryStream(QueryStreamRequest{
		Reader:               input,
		Selector:             selector,
		IncludeJSON:          true,
		MatchedOnly:          true,
		DisableInternalSpool: true,
		PayloadSinkFactory:   factory.Factory(),
		OnValue: func(value QueryStreamValue) error {
			callbacks++
			rc, openErr := value.OpenJSON()
			if openErr != nil {
				return openErr
			}
			defer rc.Close()
			_, readErr := io.Copy(io.Discard, rc)
			return readErr
		},
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
	if callbacks != 1 {
		t.Fatalf("expected one matched callback, got %d", callbacks)
	}

	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("read temp dir: %v", err)
	}
	found := false
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".json" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected reusable factory spill file in temp dir")
	}
}
