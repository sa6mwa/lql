package lql

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestMutateStreamWithResultParity(t *testing.T) {
	muts, err := ParseMutations([]string{`/status=ready`}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}
	input := `{"id":"a","status":"new"}{"id":"b","status":"new"}`

	var baseline bytes.Buffer
	err = MutateStream(MutateStreamRequest{
		Reader:    strings.NewReader(input),
		Writer:    &baseline,
		Mutations: muts,
	})
	if err != nil {
		t.Fatalf("mutate stream baseline: %v", err)
	}

	var got bytes.Buffer
	result, err := MutateStreamWithResult(MutateStreamRequest{
		Reader:    strings.NewReader(input),
		Writer:    &got,
		Mutations: muts,
	})
	if err != nil {
		t.Fatalf("mutate stream with result: %v", err)
	}
	if got.String() != baseline.String() {
		t.Fatalf("writer output mismatch:\n got=%q\nwant=%q", got.String(), baseline.String())
	}
	if result.CandidatesSeen != 2 || result.CandidatesWritten != 2 {
		t.Fatalf("unexpected mutate summary: %+v", result)
	}
	if result.BytesWritten != int64(got.Len()) {
		t.Fatalf("bytes written mismatch: got=%d want=%d", result.BytesWritten, got.Len())
	}
	if result.BytesCaptured != 0 || result.SpillCount != 0 || result.SpillBytes != 0 {
		t.Fatalf("expected no capture counters without callback, got %+v", result)
	}
	if result.StoppedEarly || result.StopReason != MutateStreamStopNone {
		t.Fatalf("expected full run result, got %+v", result)
	}
}

func TestMutateStreamWithResultErrStreamStop(t *testing.T) {
	input := `{"id":"a","status":"new"}{"id":"b","status":"new"}`
	var output bytes.Buffer
	var callbacks int

	result, err := MutateStreamWithResult(MutateStreamRequest{
		Reader: strings.NewReader(input),
		Writer: &output,
		OnValue: func(MutateStreamValue) error {
			callbacks++
			return errors.Join(errors.New("stop"), ErrStreamStop)
		},
	})
	if err != nil {
		t.Fatalf("expected graceful stop, got %v", err)
	}
	if callbacks != 1 {
		t.Fatalf("expected one callback before stop, got %d", callbacks)
	}
	if !result.StoppedEarly || result.StopReason != MutateStreamStopCallbackStop {
		t.Fatalf("expected callback stop summary, got %+v", result)
	}
	if result.CandidatesSeen != 1 || result.CandidatesWritten != 1 {
		t.Fatalf("expected first-candidate stop counts, got %+v", result)
	}
	if result.BytesCaptured <= 0 {
		t.Fatalf("expected captured bytes for callback mode, got %+v", result)
	}
	if result.SpillCount != 0 || result.SpillBytes != 0 {
		t.Fatalf("expected no spill in small callback stop case, got %+v", result)
	}
}

func TestMutateStreamWithResultCallbackErrorPropagates(t *testing.T) {
	sentinel := errors.New("callback failed")
	_, err := MutateStreamWithResult(MutateStreamRequest{
		Reader: strings.NewReader(`{"id":"a"}`),
		OnValue: func(MutateStreamValue) error {
			return sentinel
		},
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected callback error, got %v", err)
	}
	var streamErr *StreamError
	if errors.As(err, &streamErr) {
		t.Fatalf("expected raw callback error, got stream error %+v", streamErr)
	}
}

func TestMutateStreamWithResultIncludesSpillCounters(t *testing.T) {
	payload := `{"id":"a","blob":"` + strings.Repeat("x", 4096) + `"}`
	input := payload + payload

	result, err := MutateStreamWithResult(MutateStreamRequest{
		Reader:           strings.NewReader(input),
		SpoolMemoryBytes: 8,
		OnValue: func(v MutateStreamValue) error {
			if v.OpenJSON == nil {
				t.Fatalf("expected OpenJSON callback handle")
			}
			rc, openErr := v.OpenJSON()
			if openErr != nil {
				return openErr
			}
			defer rc.Close()
			_, readErr := io.Copy(io.Discard, rc)
			return readErr
		},
	})
	if err != nil {
		t.Fatalf("mutate stream with result: %v", err)
	}
	if result.CandidatesSeen != 2 || result.CandidatesWritten != 2 {
		t.Fatalf("unexpected candidate counters: %+v", result)
	}
	if result.BytesCaptured < int64(len(payload))*2 {
		t.Fatalf("expected captured bytes to include both candidates, got %+v", result)
	}
	if result.SpillCount < 2 {
		t.Fatalf("expected spill count for both candidates, got %+v", result)
	}
	if result.SpillBytes <= 0 {
		t.Fatalf("expected spill bytes > 0, got %+v", result)
	}
}
