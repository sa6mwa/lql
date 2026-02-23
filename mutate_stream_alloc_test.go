package lql

import (
	"bufio"
	"bytes"
	"io"
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
