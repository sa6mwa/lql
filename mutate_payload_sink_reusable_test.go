package lql

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

func TestMutateStreamDisableInternalSpoolRequiresFactory(t *testing.T) {
	err := MutateStream(MutateStreamRequest{
		Reader:               strings.NewReader(`{"id":"a"}`),
		DisableInternalSpool: true,
		OnValue: func(MutateStreamValue) error {
			return nil
		},
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorInvalidBody {
		t.Fatalf("expected invalid_body, got %s", streamErr.Code)
	}
}

func TestMutateStreamDisableInternalSpoolIgnoredWithoutCallback(t *testing.T) {
	err := MutateStream(MutateStreamRequest{
		Reader:               strings.NewReader(`{"id":"a"}`),
		Writer:               io.Discard,
		DisableInternalSpool: true,
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestMutateStreamReusableCallerManagedSinkFactory(t *testing.T) {
	tempDir := t.TempDir()
	factory := NewReusableMutatePayloadSinkFactory(ReusableMutatePayloadSinkFactoryOptions{
		SpoolMemoryBytes: 8,
		SpoolTempDir:     tempDir,
		SpoolFilePattern: "mutate-reusable-*.json",
	})
	defer func() {
		if err := factory.Close(); err != nil {
			t.Fatalf("close factory: %v", err)
		}
	}()

	var callbacks int
	err := MutateStream(MutateStreamRequest{
		Reader: strings.NewReader(
			`{"id":"a","blob":"` + strings.Repeat("x", 2048) + `"}` + "\n" +
				`{"id":"b","blob":"` + strings.Repeat("y", 2048) + `"}`,
		),
		DisableInternalSpool: true,
		PayloadSinkFactory:   factory.Factory(),
		OnValue: func(v MutateStreamValue) error {
			callbacks++
			if v.OpenJSON == nil {
				t.Fatalf("expected OpenJSON")
			}
			rc, err := v.OpenJSON()
			if err != nil {
				return err
			}
			defer rc.Close()
			_, err = io.Copy(io.Discard, rc)
			return err
		},
	})
	if err != nil {
		t.Fatalf("mutate stream: %v", err)
	}
	if callbacks != 2 {
		t.Fatalf("expected 2 callbacks, got %d", callbacks)
	}
}

type mutateTestSinkState struct {
	finalized bool
	cleaned   bool
	buf       bytes.Buffer
}

type mutateTestSink struct {
	state *mutateTestSinkState
}

func (s *mutateTestSink) Write(p []byte) (int, error) { return s.state.buf.Write(p) }
func (s *mutateTestSink) Finalize() error {
	s.state.finalized = true
	return nil
}
func (s *mutateTestSink) Open() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.state.buf.Bytes())), nil
}
func (s *mutateTestSink) Bytes() []byte { return s.state.buf.Bytes() }
func (s *mutateTestSink) SizeHint() int { return s.state.buf.Len() }
func (s *mutateTestSink) Cleanup() error {
	s.state.cleaned = true
	return nil
}

func TestMutateStreamCallerManagedSinkCleanupOnCallbackError(t *testing.T) {
	states := make([]*mutateTestSinkState, 0, 2)
	sentinel := errors.New("callback failed")
	err := MutateStream(MutateStreamRequest{
		Reader:               strings.NewReader(`{"id":"a"}` + "\n" + `{"id":"b"}`),
		DisableInternalSpool: true,
		PayloadSinkFactory: func(MutateStreamPayloadSinkRequest) (MutateStreamPayloadSink, error) {
			state := &mutateTestSinkState{}
			states = append(states, state)
			return &mutateTestSink{state: state}, nil
		},
		OnValue: func(MutateStreamValue) error {
			return sentinel
		},
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel callback error, got %v", err)
	}
	if len(states) != 1 {
		t.Fatalf("expected one sink created before callback error, got %d", len(states))
	}
	if !states[0].finalized || !states[0].cleaned {
		t.Fatalf("expected finalize and cleanup on callback error, got %+v", states[0])
	}
}

func TestReusableMutatePayloadSinkFactoryCloseRemovesSpoolFiles(t *testing.T) {
	tempDir := t.TempDir()
	factory := NewReusableMutatePayloadSinkFactory(ReusableMutatePayloadSinkFactoryOptions{
		SpoolMemoryBytes: 8,
		SpoolTempDir:     tempDir,
		SpoolFilePattern: "mutate-reusable-close-*.json",
	})

	var seenSpool bool
	err := MutateStream(MutateStreamRequest{
		Reader:               strings.NewReader(`{"id":"a","blob":"` + strings.Repeat("x", 1024) + `"}`),
		DisableInternalSpool: true,
		PayloadSinkFactory:   factory.Factory(),
		OnValue: func(MutateStreamValue) error {
			entries, err := os.ReadDir(tempDir)
			if err != nil {
				return err
			}
			seenSpool = len(entries) > 0
			return nil
		},
	})
	if err != nil {
		t.Fatalf("mutate stream: %v", err)
	}
	if !seenSpool {
		t.Fatalf("expected spool file during streaming")
	}
	if err := factory.Close(); err != nil {
		t.Fatalf("close factory: %v", err)
	}
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("read temp dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected spool cleanup on close, found %d entries", len(entries))
	}
}
