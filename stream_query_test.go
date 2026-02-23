package lql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestQueryStreamNDJSONMatches(t *testing.T) {
	selector, err := ParseSelectorString(`/status="open"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}

	input := strings.NewReader(`{"id":"a","status":"open"}
{"id":"b","status":"closed"}
{"id":"c","status":"open"}`)

	var matches []bool
	err = QueryStream(QueryStreamRequest{
		Reader:      input,
		Selector:    selector,
		IncludeJSON: true,
		OnValue: func(value QueryStreamValue) error {
			matches = append(matches, value.Matched)
			var decoded map[string]any
			if err := json.Unmarshal(value.JSON, &decoded); err != nil {
				return err
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}

	if len(matches) != 3 {
		t.Fatalf("expected 3 candidates, got %d", len(matches))
	}
	if !matches[0] || matches[1] || !matches[2] {
		t.Fatalf("unexpected matches: %+v", matches)
	}
}

func TestQueryStreamFlattensNestedTopLevelArrays(t *testing.T) {
	selector, err := ParseSelectorString(`/id="b"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}

	input := strings.NewReader(`[{"id":"a"},[{"id":"b"}],{"id":"c"}]`)
	var ids []string
	err = QueryStream(QueryStreamRequest{
		Reader:      input,
		Selector:    selector,
		IncludeJSON: true,
		OnValue: func(value QueryStreamValue) error {
			if !value.Matched {
				return nil
			}
			var decoded map[string]any
			if err := json.Unmarshal(value.JSON, &decoded); err != nil {
				return err
			}
			ids = append(ids, decoded["id"].(string))
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
	if len(ids) != 1 || ids[0] != "b" {
		t.Fatalf("unexpected ids: %+v", ids)
	}
}

func TestQueryStreamNonObjectDoesNotMatchNonEmptySelector(t *testing.T) {
	selector, err := ParseSelectorString(`/id="x"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}

	input := strings.NewReader(`"x"
{"id":"x"}
123`)

	var matches []bool
	err = QueryStream(QueryStreamRequest{
		Reader:      input,
		Selector:    selector,
		IncludeJSON: false,
		OnValue: func(value QueryStreamValue) error {
			matches = append(matches, value.Matched)
			if value.JSON != nil {
				t.Fatalf("expected nil json when IncludeJSON is false")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
	if len(matches) != 3 {
		t.Fatalf("expected 3 candidates, got %d", len(matches))
	}
	if matches[0] || !matches[1] || matches[2] {
		t.Fatalf("unexpected matches: %+v", matches)
	}
}

func TestQueryStreamWildcardAndExists(t *testing.T) {
	selector, err := ParseSelectorString(`and.eq{field=/items[]/sku,value=B},and.exists{/meta/etag}`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}

	input := strings.NewReader(`{"items":[{"sku":"A"},{"sku":"B"}],"meta":{"etag":"x"}}`)
	matched := false
	err = QueryStream(QueryStreamRequest{
		Reader:      input,
		Selector:    selector,
		IncludeJSON: true,
		OnValue: func(value QueryStreamValue) error {
			matched = value.Matched
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
	if !matched {
		t.Fatalf("expected match")
	}
}

func TestQueryStreamDecisionOnlyModeOverridesIncludeJSON(t *testing.T) {
	input := strings.NewReader(`{"id":"a"}`)
	called := 0
	err := QueryStream(QueryStreamRequest{
		Reader:      input,
		Selector:    Selector{},
		Mode:        QueryDecisionOnly,
		IncludeJSON: true,
		OnValue: func(value QueryStreamValue) error {
			called++
			if value.JSON != nil {
				t.Fatalf("expected no json payload in decision-only mode")
			}
			if value.OpenJSON != nil {
				t.Fatalf("expected no payload reader in decision-only mode")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
	if called != 1 {
		t.Fatalf("expected 1 callback, got %d", called)
	}
}

func TestQueryStreamPlanParity(t *testing.T) {
	selector, err := ParseSelectorString(`/status="open"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	plan, err := NewQueryStreamPlan(selector)
	if err != nil {
		t.Fatalf("new query plan: %v", err)
	}
	input := `{"id":"a","status":"open"}
{"id":"b","status":"closed"}
{"id":"c","status":"open"}`

	runMatches := func(req QueryStreamRequest) ([]bool, error) {
		matches := make([]bool, 0, 4)
		req.Reader = strings.NewReader(input)
		req.OnValue = func(value QueryStreamValue) error {
			matches = append(matches, value.Matched)
			return nil
		}
		if err := QueryStream(req); err != nil {
			return nil, err
		}
		return matches, nil
	}

	withSelector, err := runMatches(QueryStreamRequest{Selector: selector})
	if err != nil {
		t.Fatalf("query stream with selector: %v", err)
	}
	withPlan, err := runMatches(QueryStreamRequest{Plan: plan})
	if err != nil {
		t.Fatalf("query stream with plan: %v", err)
	}
	if len(withSelector) != len(withPlan) {
		t.Fatalf("parity length mismatch: selector=%d plan=%d", len(withSelector), len(withPlan))
	}
	for i := range withSelector {
		if withSelector[i] != withPlan[i] {
			t.Fatalf("parity mismatch at %d: selector=%v plan=%v", i, withSelector[i], withPlan[i])
		}
	}
}

func TestNewStreamSelectorEngineBuildsFastTopLevelProgram(t *testing.T) {
	gte := 10.0
	selector := Selector{
		Or: []Selector{
			{Eq: &Term{Field: "/status", Value: "open"}},
			{Eq: &Term{Field: "/status", Value: "closed"}},
		},
		Not:    &Selector{Eq: &Term{Field: "/region", Value: "apac"}},
		Range:  &RangeTerm{Field: "/latency", GTE: &gte},
		In:     &InTerm{Field: "/env", Any: []string{"prod", "stage"}},
		Exists: "/meta",
	}
	engine, err := newStreamSelectorEngine(selector)
	if err != nil {
		t.Fatalf("new stream selector engine: %v", err)
	}
	if engine.fastTopLevel == nil {
		t.Fatalf("expected fast top-level program to be compiled")
	}
}

func TestNewStreamSelectorEngineSkipsFastTopLevelProgramForNestedPaths(t *testing.T) {
	selector := Selector{
		Eq: &Term{Field: "/meta/etag", Value: "x"},
	}
	engine, err := newStreamSelectorEngine(selector)
	if err != nil {
		t.Fatalf("new stream selector engine: %v", err)
	}
	if engine.fastTopLevel != nil {
		t.Fatalf("expected fast top-level program to be disabled for nested paths")
	}
}

func TestQueryStreamPlanAndSelectorRejected(t *testing.T) {
	selector, err := ParseSelectorString(`/status="open"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	plan, err := NewQueryStreamPlan(selector)
	if err != nil {
		t.Fatalf("new query plan: %v", err)
	}

	err = QueryStream(QueryStreamRequest{
		Reader:   strings.NewReader(`{"status":"open"}`),
		Selector: selector,
		Plan:     plan,
		OnValue: func(QueryStreamValue) error {
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

func TestNewQueryStreamPlanInvalidSelector(t *testing.T) {
	_, err := NewQueryStreamPlan(Selector{
		Eq: &Term{Field: "", Value: "x"},
	})
	if err == nil {
		t.Fatalf("expected invalid selector plan error")
	}
}

func TestQueryStreamZeroPlanIsIgnored(t *testing.T) {
	err := QueryStream(QueryStreamRequest{
		Reader: strings.NewReader(`{"status":"open"}`),
		Plan:   QueryStreamPlan{},
		OnValue: func(QueryStreamValue) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("unexpected error for zero plan treated as unset: %v", err)
	}
}

func TestQueryStreamContextCanceledReturnsTypedError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := QueryStream(QueryStreamRequest{
		Ctx:      ctx,
		Reader:   strings.NewReader(`{"id":"a"}`),
		Selector: Selector{},
		OnValue: func(value QueryStreamValue) error {
			return nil
		},
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorContextCanceled {
		t.Fatalf("expected context_canceled, got %s", streamErr.Code)
	}
}

func TestQueryStreamInvalidJSONReturnsTypedError(t *testing.T) {
	err := QueryStream(QueryStreamRequest{
		Reader:   strings.NewReader(`{"id":1`),
		Selector: Selector{},
		OnValue: func(value QueryStreamValue) error {
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
	if streamErr.Offset < 0 {
		t.Fatalf("expected non-negative error offset, got %d", streamErr.Offset)
	}
}

func TestQueryStreamSpoolsLargeCandidateToDisk(t *testing.T) {
	tempDir := t.TempDir()
	pattern := "query-spool-*.json"
	input := strings.NewReader(`{"id":"a","payload":"abcdefghijklmnopqrstuvwxyz0123456789"}`)

	var sawCallback bool
	err := QueryStream(QueryStreamRequest{
		Reader:           input,
		Selector:         Selector{},
		IncludeJSON:      true,
		SpoolMemoryBytes: 16,
		SpoolTempDir:     tempDir,
		SpoolFilePattern: pattern,
		OnValue: func(value QueryStreamValue) error {
			sawCallback = true
			if value.JSON != nil {
				t.Fatalf("expected spooled payload to avoid in-memory JSON bytes")
			}
			if value.OpenJSON == nil {
				t.Fatalf("expected OpenJSON for spooled payload")
			}

			files, err := filepath.Glob(filepath.Join(tempDir, "query-spool-*.json"))
			if err != nil {
				return err
			}
			if len(files) == 0 {
				t.Fatalf("expected spool file during callback")
			}
			info, err := os.Stat(files[0])
			if err != nil {
				return err
			}
			if info.Mode().Perm() != 0o600 {
				t.Fatalf("expected spool file mode 0600, got %o", info.Mode().Perm())
			}

			rc, err := value.OpenJSON()
			if err != nil {
				return err
			}
			defer rc.Close()
			payload, err := io.ReadAll(rc)
			if err != nil {
				return err
			}
			var decoded map[string]any
			if err := json.Unmarshal(payload, &decoded); err != nil {
				return err
			}
			if decoded["id"] != "a" {
				t.Fatalf("unexpected decoded payload: %#v", decoded)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
	if !sawCallback {
		t.Fatalf("expected callback to be invoked")
	}
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("readdir spool dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected spool files to be cleaned up, got %d entries", len(entries))
	}
}

func TestQueryStreamInMemoryPayloadStillSupportsOpenJSON(t *testing.T) {
	input := strings.NewReader(`{"id":"a"}`)

	err := QueryStream(QueryStreamRequest{
		Reader:           input,
		Selector:         Selector{},
		IncludeJSON:      true,
		SpoolMemoryBytes: 1024,
		OnValue: func(value QueryStreamValue) error {
			if value.JSON == nil {
				t.Fatalf("expected in-memory payload bytes")
			}
			if value.OpenJSON == nil {
				t.Fatalf("expected OpenJSON")
			}
			rc, err := value.OpenJSON()
			if err != nil {
				return err
			}
			defer rc.Close()
			fromReader, err := io.ReadAll(rc)
			if err != nil {
				return err
			}
			if !bytes.Equal(fromReader, value.JSON) {
				t.Fatalf("OpenJSON payload mismatch")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
}

func TestQueryStreamSpoolCreateFailureReturnsTypedError(t *testing.T) {
	invalidDir := filepath.Join(t.TempDir(), "missing")
	err := QueryStream(QueryStreamRequest{
		Reader:           strings.NewReader(`{"id":"a","payload":"abcdefghijklmnopqrstuvwxyz"}`),
		Selector:         Selector{},
		IncludeJSON:      true,
		SpoolMemoryBytes: 8,
		SpoolTempDir:     invalidDir,
		OnValue: func(QueryStreamValue) error {
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

func TestQueryStreamSpoolCleanupOnCallbackError(t *testing.T) {
	tempDir := t.TempDir()
	sentinel := errors.New("stop")
	err := QueryStream(QueryStreamRequest{
		Reader:           strings.NewReader(`{"id":"a","payload":"abcdefghijklmnopqrstuvwxyz"}`),
		Selector:         Selector{},
		IncludeJSON:      true,
		SpoolMemoryBytes: 8,
		SpoolTempDir:     tempDir,
		SpoolFilePattern: "cb-*.json",
		OnValue: func(value QueryStreamValue) error {
			if value.OpenJSON == nil {
				return fmt.Errorf("expected OpenJSON")
			}
			matches, globErr := filepath.Glob(filepath.Join(tempDir, "cb-*.json"))
			if globErr != nil {
				return globErr
			}
			if len(matches) == 0 {
				return fmt.Errorf("expected spool file while callback is active")
			}
			return sentinel
		},
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected callback error, got %v", err)
	}
	entries, readErr := os.ReadDir(tempDir)
	if readErr != nil {
		t.Fatalf("readdir spool dir: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("expected spool cleanup on callback error, got %d entries", len(entries))
	}
}

func TestQueryStreamSpoolCleanupOnMaxCandidateBytes(t *testing.T) {
	tempDir := t.TempDir()
	err := QueryStream(QueryStreamRequest{
		Reader:            strings.NewReader(`{"id":"a","payload":"abcdefghijklmnopqrstuvwxyz"}`),
		Selector:          Selector{},
		IncludeJSON:       true,
		SpoolMemoryBytes:  8,
		SpoolTempDir:      tempDir,
		SpoolFilePattern:  "max-*.json",
		MaxCandidateBytes: 16,
		OnValue: func(QueryStreamValue) error {
			return nil
		},
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorDocumentTooLarge {
		t.Fatalf("expected document_too_large, got %s", streamErr.Code)
	}
	entries, readErr := os.ReadDir(tempDir)
	if readErr != nil {
		t.Fatalf("readdir spool dir: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("expected spool cleanup on size failure, got %d entries", len(entries))
	}
}

func TestQueryStreamMatchedOnlyInvokesCallbackForMatches(t *testing.T) {
	selector, err := ParseSelectorString(`/status="open"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	input := strings.NewReader(`{"id":"a","status":"open"}
{"id":"b","status":"closed"}
{"id":"c","status":"open"}`)
	var calls int
	err = QueryStream(QueryStreamRequest{
		Reader:      input,
		Selector:    selector,
		MatchedOnly: true,
		OnValue: func(value QueryStreamValue) error {
			calls++
			if !value.Matched {
				t.Fatalf("matched-only callback received unmatched candidate")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 matched callbacks, got %d", calls)
	}
}

func TestQueryStreamDisableInternalSpoolRequiresFactory(t *testing.T) {
	err := QueryStream(QueryStreamRequest{
		Reader:               strings.NewReader(`{"id":"a"}`),
		Selector:             Selector{},
		IncludeJSON:          true,
		DisableInternalSpool: true,
		OnValue: func(QueryStreamValue) error {
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

func TestQueryStreamDisableInternalSpoolIgnoredInDecisionOnlyMode(t *testing.T) {
	calls := 0
	err := QueryStream(QueryStreamRequest{
		Reader:               strings.NewReader(`{"id":"a"}`),
		Selector:             Selector{},
		Mode:                 QueryDecisionOnly,
		IncludeJSON:          true,
		DisableInternalSpool: true,
		OnValue: func(value QueryStreamValue) error {
			calls++
			if value.JSON != nil || value.OpenJSON != nil {
				t.Fatalf("expected no payload in decision-only mode")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected one callback, got %d", calls)
	}
}

func TestQueryStreamCallerManagedPayloadSink(t *testing.T) {
	var states []*sinkState
	factoryCalls := 0
	err := QueryStream(QueryStreamRequest{
		Reader: strings.NewReader(`{"id":"a"}
{"id":"b"}`),
		Selector:             Selector{},
		IncludeJSON:          true,
		DisableInternalSpool: true,
		PayloadSinkFactory: func(QueryStreamPayloadSinkRequest) (QueryStreamPayloadSink, error) {
			factoryCalls++
			state := &sinkState{}
			states = append(states, state)
			return &queryStreamMemorySink{state: state}, nil
		},
		OnValue: func(value QueryStreamValue) error {
			if value.OpenJSON == nil {
				return fmt.Errorf("expected OpenJSON from caller sink")
			}
			rc, err := value.OpenJSON()
			if err != nil {
				return err
			}
			defer rc.Close()
			_, err = io.ReadAll(rc)
			return err
		},
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
	if factoryCalls != 2 {
		t.Fatalf("expected 2 factory calls, got %d", factoryCalls)
	}
	if len(states) != 2 {
		t.Fatalf("expected 2 sink states, got %d", len(states))
	}
	for idx, state := range states {
		if !state.finalized {
			t.Fatalf("sink %d was not finalized", idx)
		}
		if !state.cleaned {
			t.Fatalf("sink %d was not cleaned", idx)
		}
	}
}

func TestQueryStreamMatchedOnlyStillCleansCallerManagedSinks(t *testing.T) {
	var states []*sinkState
	callbacks := 0
	err := QueryStream(QueryStreamRequest{
		Reader:               strings.NewReader(`{"status":"closed"}`),
		Selector:             mustParseQuerySelector(t, `/status="open"`),
		IncludeJSON:          true,
		MatchedOnly:          true,
		DisableInternalSpool: true,
		PayloadSinkFactory: func(QueryStreamPayloadSinkRequest) (QueryStreamPayloadSink, error) {
			state := &sinkState{}
			states = append(states, state)
			return &queryStreamMemorySink{state: state}, nil
		},
		OnValue: func(QueryStreamValue) error {
			callbacks++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
	if callbacks != 0 {
		t.Fatalf("expected no callbacks, got %d", callbacks)
	}
	if len(states) != 1 || !states[0].cleaned {
		t.Fatalf("expected sink cleanup on unmatched candidate, states=%+v", states)
	}
}

func TestQueryStreamCallerManagedPayloadSinkFactoryErrors(t *testing.T) {
	sentinel := errors.New("factory failed")
	err := QueryStream(QueryStreamRequest{
		Reader:               strings.NewReader(`{"id":"a"}`),
		Selector:             Selector{},
		IncludeJSON:          true,
		DisableInternalSpool: true,
		PayloadSinkFactory: func(QueryStreamPayloadSinkRequest) (QueryStreamPayloadSink, error) {
			return nil, sentinel
		},
		OnValue: func(QueryStreamValue) error { return nil },
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected factory error, got %v", err)
	}
}

func TestQueryStreamCallerManagedPayloadSinkNil(t *testing.T) {
	err := QueryStream(QueryStreamRequest{
		Reader:               strings.NewReader(`{"id":"a"}`),
		Selector:             Selector{},
		IncludeJSON:          true,
		DisableInternalSpool: true,
		PayloadSinkFactory: func(QueryStreamPayloadSinkRequest) (QueryStreamPayloadSink, error) {
			return nil, nil
		},
		OnValue: func(QueryStreamValue) error { return nil },
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorInvalidBody {
		t.Fatalf("expected invalid_body, got %s", streamErr.Code)
	}
}

func TestQueryStreamCallerManagedPayloadSinkFinalizeError(t *testing.T) {
	sentinel := errors.New("finalize failed")
	err := QueryStream(QueryStreamRequest{
		Reader:               strings.NewReader(`{"id":"a"}`),
		Selector:             Selector{},
		IncludeJSON:          true,
		DisableInternalSpool: true,
		PayloadSinkFactory: func(QueryStreamPayloadSinkRequest) (QueryStreamPayloadSink, error) {
			return &queryStreamFaultySink{finalizeErr: sentinel}, nil
		},
		OnValue: func(QueryStreamValue) error { return nil },
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected finalize error, got %v", err)
	}
}

func TestQueryStreamCallerManagedPayloadSinkCleanupErrorIsInternal(t *testing.T) {
	sentinel := errors.New("cleanup failed")
	err := QueryStream(QueryStreamRequest{
		Reader:               strings.NewReader(`{"id":"a"}`),
		Selector:             Selector{},
		IncludeJSON:          true,
		DisableInternalSpool: true,
		PayloadSinkFactory: func(QueryStreamPayloadSinkRequest) (QueryStreamPayloadSink, error) {
			return &queryStreamFaultySink{cleanupErr: sentinel}, nil
		},
		OnValue: func(QueryStreamValue) error { return nil },
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorInternal {
		t.Fatalf("expected internal, got %s", streamErr.Code)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected cleanup sentinel, got %v", err)
	}
}

func TestQueryStreamCallerManagedPayloadSinkWriteErrorStillCleans(t *testing.T) {
	state := &sinkState{}
	sentinel := errors.New("write failed")
	err := QueryStream(QueryStreamRequest{
		Reader:               strings.NewReader(`{"id":"a","payload":"abcdef"}`),
		Selector:             Selector{},
		IncludeJSON:          true,
		DisableInternalSpool: true,
		PayloadSinkFactory: func(QueryStreamPayloadSinkRequest) (QueryStreamPayloadSink, error) {
			return &queryStreamWriteFailSink{
				state:       state,
				writeErr:    sentinel,
				allowedByte: 2,
			}, nil
		},
		OnValue: func(QueryStreamValue) error { return nil },
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected write sentinel, got %v", err)
	}
	if !state.cleaned {
		t.Fatalf("expected sink cleanup after write failure")
	}
}

func TestQueryStreamCallbackVsCleanupErrorPrecedence(t *testing.T) {
	callbackErr := errors.New("callback failed")
	cleanupErr := errors.New("cleanup failed")
	err := QueryStream(QueryStreamRequest{
		Reader:               strings.NewReader(`{"id":"a"}`),
		Selector:             Selector{},
		IncludeJSON:          true,
		DisableInternalSpool: true,
		PayloadSinkFactory: func(QueryStreamPayloadSinkRequest) (QueryStreamPayloadSink, error) {
			return &queryStreamFaultySink{cleanupErr: cleanupErr}, nil
		},
		OnValue: func(QueryStreamValue) error { return callbackErr },
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorInternal {
		t.Fatalf("expected internal from cleanup precedence, got %s", streamErr.Code)
	}
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("expected cleanup error precedence, got %v", err)
	}
	if errors.Is(err, callbackErr) {
		t.Fatalf("did not expect callback error to win precedence")
	}
}

func TestQueryStreamCandidateSizeContract(t *testing.T) {
	candidateA := `{"a":1}`
	candidateB := `{"b":[1, 2]}`
	input := " \t" + candidateA + "\n\n  " + candidateB + " \n"
	var sizes []int64
	err := QueryStream(QueryStreamRequest{
		Reader:   strings.NewReader(input),
		Selector: Selector{},
		OnValue: func(value QueryStreamValue) error {
			sizes = append(sizes, value.Size)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
	if len(sizes) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(sizes))
	}
	if sizes[0] != int64(len(candidateA)) || sizes[1] != int64(len(candidateB)) {
		t.Fatalf("candidate size contract mismatch: got=%v want=[%d %d]", sizes, len(candidateA), len(candidateB))
	}
}

func TestQueryStreamMaxCandidateBytesContractBoundary(t *testing.T) {
	candidate := `{"a":1}`
	err := QueryStream(QueryStreamRequest{
		Reader:            strings.NewReader(" \n\t" + candidate + " \n"),
		Selector:          Selector{},
		MaxCandidateBytes: int64(len(candidate)),
		OnValue: func(QueryStreamValue) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("expected boundary pass, got %v", err)
	}

	err = QueryStream(QueryStreamRequest{
		Reader:            strings.NewReader(" \n\t" + candidate + " \n"),
		Selector:          Selector{},
		MaxCandidateBytes: int64(len(candidate) - 1),
		OnValue: func(QueryStreamValue) error {
			return nil
		},
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorDocumentTooLarge {
		t.Fatalf("expected document_too_large, got %s", streamErr.Code)
	}
}

type queryStreamMemorySink struct {
	buffer bytes.Buffer
	state  *sinkState
}

func mustParseQuerySelector(t *testing.T, expr string) Selector {
	t.Helper()
	selector, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	return selector
}

type sinkState struct {
	finalized bool
	cleaned   bool
}

func (s *queryStreamMemorySink) Write(p []byte) (int, error) {
	return s.buffer.Write(p)
}

func (s *queryStreamMemorySink) Finalize() error {
	s.state.finalized = true
	return nil
}

func (s *queryStreamMemorySink) Open() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.buffer.Bytes())), nil
}

func (s *queryStreamMemorySink) Bytes() []byte {
	return s.buffer.Bytes()
}

func (s *queryStreamMemorySink) SizeHint() int {
	return s.buffer.Len()
}

func (s *queryStreamMemorySink) Cleanup() error {
	s.state.cleaned = true
	return nil
}

type queryStreamFaultySink struct {
	buffer      bytes.Buffer
	finalizeErr error
	cleanupErr  error
}

func (s *queryStreamFaultySink) Write(p []byte) (int, error) {
	return s.buffer.Write(p)
}

func (s *queryStreamFaultySink) Finalize() error {
	return s.finalizeErr
}

func (s *queryStreamFaultySink) Open() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.buffer.Bytes())), nil
}

func (s *queryStreamFaultySink) Bytes() []byte {
	return s.buffer.Bytes()
}

func (s *queryStreamFaultySink) SizeHint() int {
	return s.buffer.Len()
}

func (s *queryStreamFaultySink) Cleanup() error {
	return s.cleanupErr
}

type queryStreamWriteFailSink struct {
	buffer      bytes.Buffer
	state       *sinkState
	writeErr    error
	allowedByte int
	written     int
}

func (s *queryStreamWriteFailSink) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if s.written >= s.allowedByte {
		return 0, s.writeErr
	}
	allowed := s.allowedByte - s.written
	if allowed > len(p) {
		allowed = len(p)
	}
	n, _ := s.buffer.Write(p[:allowed])
	s.written += n
	if n < len(p) {
		return n, s.writeErr
	}
	return n, nil
}

func (s *queryStreamWriteFailSink) Finalize() error {
	return nil
}

func (s *queryStreamWriteFailSink) Open() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.buffer.Bytes())), nil
}

func (s *queryStreamWriteFailSink) Bytes() []byte {
	return s.buffer.Bytes()
}

func (s *queryStreamWriteFailSink) SizeHint() int {
	return s.buffer.Len()
}

func (s *queryStreamWriteFailSink) Cleanup() error {
	if s.state != nil {
		s.state.cleaned = true
	}
	return nil
}
