package lql

import (
	"bufio"
	"bytes"
	"io"
	"path/filepath"
	"testing"
)

func TestMutateStreamWriterHotPathZeroAllocs(t *testing.T) {
	small := []byte("true\n")
	large := bytes.Repeat([]byte("true\n"), 1000)
	reader := bufio.NewReaderSize(bytes.NewReader(nil), 64*1024)

	run := func(payload []byte) (float64, error) {
		var runErr error
		allocs := testing.AllocsPerRun(200, func() {
			reader.Reset(bytes.NewReader(payload))
			runErr = MutateStream(MutateStreamRequest{
				Reader: reader,
				Writer: io.Discard,
			})
		})
		return allocs, runErr
	}

	smallAllocs, err := run(small)
	if err != nil {
		t.Fatalf("mutate stream small: %v", err)
	}
	largeAllocs, err := run(large)
	if err != nil {
		t.Fatalf("mutate stream large: %v", err)
	}
	if largeAllocs > smallAllocs {
		t.Fatalf("expected zero per-candidate allocations in hot path, got small=%.2f large=%.2f", smallAllocs, largeAllocs)
	}
}

func TestMutateStreamCallbackHotPathZeroAllocs(t *testing.T) {
	small := []byte("true\n")
	large := bytes.Repeat([]byte("true\n"), 1000)
	reader := bufio.NewReaderSize(bytes.NewReader(nil), 64*1024)

	run := func(payload []byte) (float64, error) {
		var runErr error
		allocs := testing.AllocsPerRun(200, func() {
			reader.Reset(bytes.NewReader(payload))
			runErr = MutateStream(MutateStreamRequest{
				Reader: reader,
				OnValue: func(MutateStreamValue) error {
					return nil
				},
			})
		})
		return allocs, runErr
	}

	smallAllocs, err := run(small)
	if err != nil {
		t.Fatalf("mutate stream callback small: %v", err)
	}
	largeAllocs, err := run(large)
	if err != nil {
		t.Fatalf("mutate stream callback large: %v", err)
	}
	if largeAllocs > smallAllocs {
		t.Fatalf("expected zero per-candidate allocations in callback hot path, got small=%.2f large=%.2f", smallAllocs, largeAllocs)
	}
}

func TestMutateStreamFileBackedTextEmitterHotPathZeroAllocs(t *testing.T) {
	path := filepath.Join("/virtual", "blob.txt")
	resolver := &nonAllocMutateFileResolver{
		payloads: map[string][]byte{
			path: []byte("hello world"),
		},
	}
	state := mutateStreamState{}
	value := &mutationFileValue{
		path:     path,
		mode:     mutationFileValueModeText,
		resolver: resolver,
	}
	if err := state.emitTextFileMutationValue(value, mutateDiscardWriter{}); err != nil {
		t.Fatalf("emit warmup: %v", err)
	}

	var runErr error
	allocs := testing.AllocsPerRun(200, func() {
		runErr = state.emitTextFileMutationValue(value, mutateDiscardWriter{})
	})
	if runErr != nil {
		t.Fatalf("emit run: %v", runErr)
	}
	if allocs != 0 {
		t.Fatalf("expected zero allocations for file-backed text emitter hot path, got %.2f", allocs)
	}
}

func TestMutateStreamFileBackedBase64EmitterHotPathZeroAllocs(t *testing.T) {
	path := filepath.Join("/virtual", "blob.bin")
	resolver := &nonAllocMutateFileResolver{
		payloads: map[string][]byte{
			path: []byte{0x00, 0x01, 0x02, 'a', 'b', 'c'},
		},
	}
	state := mutateStreamState{}
	value := &mutationFileValue{
		path:     path,
		mode:     mutationFileValueModeBase64,
		resolver: resolver,
	}
	if err := state.emitBase64FileMutationValue(value, mutateDiscardWriter{}); err != nil {
		t.Fatalf("emit warmup: %v", err)
	}

	var runErr error
	allocs := testing.AllocsPerRun(200, func() {
		runErr = state.emitBase64FileMutationValue(value, mutateDiscardWriter{})
	})
	if runErr != nil {
		t.Fatalf("emit run: %v", runErr)
	}
	if allocs != 0 {
		t.Fatalf("expected zero allocations for file-backed base64 emitter hot path, got %.2f", allocs)
	}
}
