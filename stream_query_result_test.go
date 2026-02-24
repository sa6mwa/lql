package lql

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestQueryStreamWithResultParity(t *testing.T) {
	selector := mustParseQuerySelector(t, `/status="open"`)
	input := `{"id":"a","status":"open"}{"id":"b","status":"closed"}{"id":"c","status":"open"}`

	var baseline []bool
	err := QueryStream(QueryStreamRequest{
		Reader:   strings.NewReader(input),
		Selector: selector,
		OnValue: func(v QueryStreamValue) error {
			baseline = append(baseline, v.Matched)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream baseline: %v", err)
	}

	var got []bool
	result, err := QueryStreamWithResult(QueryStreamRequest{
		Reader:   strings.NewReader(input),
		Selector: selector,
		OnValue: func(v QueryStreamValue) error {
			got = append(got, v.Matched)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream with result: %v", err)
	}
	if len(got) != len(baseline) {
		t.Fatalf("callback count mismatch: got=%d want=%d", len(got), len(baseline))
	}
	for i := range got {
		if got[i] != baseline[i] {
			t.Fatalf("callback parity mismatch at %d: got=%v want=%v", i, got[i], baseline[i])
		}
	}
	if result.CandidatesSeen != 3 || result.CandidatesMatched != 2 {
		t.Fatalf("unexpected summary: %+v", result)
	}
	if result.StoppedEarly || result.StopReason != QueryStreamStopNone {
		t.Fatalf("expected full scan result: %+v", result)
	}
}

func TestQueryStreamWithResultSupportsDecisionOnlyCallback(t *testing.T) {
	input := `{"id":"a"}{"id":"b"}`
	var decisions int
	result, err := QueryStreamWithResult(QueryStreamRequest{
		Reader: strings.NewReader(input),
		OnDecision: func(d QueryStreamDecision) error {
			decisions++
			if d.Size <= 0 {
				t.Fatalf("expected positive candidate size")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream with decision callback: %v", err)
	}
	if decisions != 2 {
		t.Fatalf("expected 2 decisions, got %d", decisions)
	}
	if result.CandidatesSeen != 2 {
		t.Fatalf("expected 2 candidates, got %+v", result)
	}
}

func TestQueryStreamWithResultPlusValueModeRequiresOnValue(t *testing.T) {
	_, err := QueryStreamWithResult(QueryStreamRequest{
		Reader:     strings.NewReader(`{"id":"a"}`),
		Mode:       QueryDecisionPlusValue,
		OnDecision: func(QueryStreamDecision) error { return nil },
	})
	if err == nil {
		t.Fatalf("expected validation error")
	}
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected stream error, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorInvalidBody {
		t.Fatalf("expected invalid_body, got %s", streamErr.Code)
	}
}

func TestQueryStreamWithResultStopControls(t *testing.T) {
	input := `{"id":"a"}{"id":"b"}{"id":"c"}`

	matchResult, err := QueryStreamWithResult(QueryStreamRequest{
		Reader:     strings.NewReader(input),
		MaxMatches: 2,
		OnValue:    func(QueryStreamValue) error { return nil },
	})
	if err != nil {
		t.Fatalf("match-limit run: %v", err)
	}
	if !matchResult.StoppedEarly || matchResult.StopReason != QueryStreamStopMatchLimit {
		t.Fatalf("expected match-limit stop, got %+v", matchResult)
	}
	if matchResult.CandidatesSeen != 2 || matchResult.CandidatesMatched != 2 {
		t.Fatalf("unexpected match-limit counts: %+v", matchResult)
	}

	candidateResult, err := QueryStreamWithResult(QueryStreamRequest{
		Reader:        strings.NewReader(input),
		MaxCandidates: 2,
		OnValue:       func(QueryStreamValue) error { return nil },
	})
	if err != nil {
		t.Fatalf("candidate-limit run: %v", err)
	}
	if !candidateResult.StoppedEarly || candidateResult.StopReason != QueryStreamStopCandidateLimit {
		t.Fatalf("expected candidate-limit stop, got %+v", candidateResult)
	}
	if candidateResult.CandidatesSeen != 2 {
		t.Fatalf("unexpected candidate-limit counts: %+v", candidateResult)
	}

	byteResult, err := QueryStreamWithResult(QueryStreamRequest{
		Reader:       strings.NewReader(input),
		MaxBytesRead: int64(len(`{"id":"a"}{"id":"b"}`)),
		OnValue:      func(QueryStreamValue) error { return nil },
	})
	if err != nil {
		t.Fatalf("byte-limit run: %v", err)
	}
	if !byteResult.StoppedEarly || byteResult.StopReason != QueryStreamStopByteLimit {
		t.Fatalf("expected byte-limit stop, got %+v", byteResult)
	}
	if byteResult.BytesRead < int64(len(`{"id":"a"}{"id":"b"}`)) {
		t.Fatalf("unexpected byte-limit offset: %+v", byteResult)
	}
}

func TestQueryStreamWithResultStopControlPrecedence(t *testing.T) {
	result, err := QueryStreamWithResult(QueryStreamRequest{
		Reader:        strings.NewReader(`{"id":"a"}{"id":"b"}`),
		MaxMatches:    1,
		MaxCandidates: 1,
		MaxBytesRead:  1,
		OnValue:       func(QueryStreamValue) error { return nil },
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
	if !result.StoppedEarly || result.StopReason != QueryStreamStopMatchLimit {
		t.Fatalf("expected match-limit precedence, got %+v", result)
	}
}

func TestQueryStreamWithResultStopControlPrecedenceCandidateOverByte(t *testing.T) {
	selector := mustParseQuerySelector(t, `/id="missing"`)
	result, err := QueryStreamWithResult(QueryStreamRequest{
		Reader:        strings.NewReader(`{"id":"a"}{"id":"b"}`),
		Selector:      selector,
		MaxCandidates: 1,
		MaxBytesRead:  1,
		OnValue:       func(QueryStreamValue) error { return nil },
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
	if !result.StoppedEarly || result.StopReason != QueryStreamStopCandidateLimit {
		t.Fatalf("expected candidate-limit precedence, got %+v", result)
	}
}

func TestQueryStreamErrStreamStopFromOnValue(t *testing.T) {
	result, err := QueryStreamWithResult(QueryStreamRequest{
		Reader: strings.NewReader(`{"id":"a"}{"id":"b"}`),
		OnValue: func(QueryStreamValue) error {
			return errors.Join(errors.New("stop"), ErrStreamStop)
		},
	})
	if err != nil {
		t.Fatalf("expected graceful stop, got %v", err)
	}
	if !result.StoppedEarly || result.StopReason != QueryStreamStopCallbackStop {
		t.Fatalf("expected callback stop result, got %+v", result)
	}
	if result.CandidatesSeen != 1 {
		t.Fatalf("expected stop after first candidate, got %+v", result)
	}
}

func TestQueryStreamErrStreamStopFromOnDecision(t *testing.T) {
	onValueCalls := 0
	result, err := QueryStreamWithResult(QueryStreamRequest{
		Reader: strings.NewReader(`{"id":"a"}{"id":"b"}`),
		OnDecision: func(QueryStreamDecision) error {
			return errors.Join(errors.New("decision-stop"), ErrStreamStop)
		},
		OnValue: func(QueryStreamValue) error {
			onValueCalls++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("expected graceful stop, got %v", err)
	}
	if onValueCalls != 0 {
		t.Fatalf("expected OnValue to be skipped after decision stop, got %d calls", onValueCalls)
	}
	if !result.StoppedEarly || result.StopReason != QueryStreamStopCallbackStop {
		t.Fatalf("expected callback stop result, got %+v", result)
	}
}

func TestQueryStreamOnDecisionOrderAndCoverageWithMatchedOnly(t *testing.T) {
	selector := mustParseQuerySelector(t, `/status="open"`)
	input := `{"id":"a","status":"open"}{"id":"b","status":"closed"}`
	events := make([]string, 0, 4)
	idx := 0

	result, err := QueryStreamWithResult(QueryStreamRequest{
		Reader:      strings.NewReader(input),
		Selector:    selector,
		MatchedOnly: true,
		OnDecision: func(d QueryStreamDecision) error {
			events = append(events, "d:"+boolToString(d.Matched))
			return nil
		},
		OnValue: func(v QueryStreamValue) error {
			events = append(events, "v:"+boolToString(v.Matched))
			idx++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
	if idx != 1 {
		t.Fatalf("expected one matched callback, got %d", idx)
	}
	want := []string{"d:true", "v:true", "d:false"}
	if len(events) != len(want) {
		t.Fatalf("event length mismatch: got=%v want=%v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("event mismatch at %d: got=%s want=%s", i, events[i], want[i])
		}
	}
	if result.CandidatesSeen != 2 || result.CandidatesMatched != 1 {
		t.Fatalf("unexpected result summary: %+v", result)
	}
}

func TestQueryStreamOnDecisionWithMatchedOnlyWithoutOnValue(t *testing.T) {
	selector := mustParseQuerySelector(t, `/status="open"`)
	seen := 0
	result, err := QueryStreamWithResult(QueryStreamRequest{
		Reader:      strings.NewReader(`{"status":"open"}{"status":"closed"}`),
		Selector:    selector,
		MatchedOnly: true,
		OnDecision: func(QueryStreamDecision) error {
			seen++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
	if seen != 2 {
		t.Fatalf("expected two decisions, got %d", seen)
	}
	if result.CandidatesSeen != 2 || result.CandidatesMatched != 1 {
		t.Fatalf("unexpected summary: %+v", result)
	}
}

func TestQueryStreamCallbackErrorsPropagateWithoutStreamWrap(t *testing.T) {
	sentinel := errors.New("callback failed")
	_, err := QueryStreamWithResult(QueryStreamRequest{
		Reader: strings.NewReader(`{"id":"a"}`),
		OnDecision: func(QueryStreamDecision) error {
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

	_, err = QueryStreamWithResult(QueryStreamRequest{
		Reader: strings.NewReader(`{"id":"a"}`),
		OnValue: func(QueryStreamValue) error {
			return sentinel
		},
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected callback error, got %v", err)
	}
	if errors.As(err, &streamErr) {
		t.Fatalf("expected raw callback error, got stream error %+v", streamErr)
	}
}

func TestQueryStreamCapturePolicyMatchesOnlyBestEffort(t *testing.T) {
	selector := mustParseQuerySelector(t, `/id="match"`)
	var builder strings.Builder
	largeString := strings.Repeat("x", 32*1024)
	for i := 0; i < 4; i++ {
		builder.WriteByte('"')
		builder.WriteString(largeString)
		builder.WriteByte('"')
	}
	builder.WriteString(`{"id":"match","status":"open"}`)
	for i := 0; i < 4; i++ {
		builder.WriteByte('"')
		builder.WriteString(largeString)
		builder.WriteByte('"')
	}
	input := builder.String()

	run := func(policy QueryStreamCapturePolicy) (QueryStreamResult, []byte, error) {
		var payload []byte
		res, err := QueryStreamWithResult(QueryStreamRequest{
			Reader:        strings.NewReader(input),
			Selector:      selector,
			Mode:          QueryDecisionPlusValue,
			MatchedOnly:   true,
			CapturePolicy: policy,
			OnValue: func(v QueryStreamValue) error {
				if !v.Matched {
					t.Fatalf("expected matched-only payload")
				}
				p := v.JSON
				if p == nil && v.OpenJSON != nil {
					rc, err := v.OpenJSON()
					if err != nil {
						return err
					}
					defer rc.Close()
					p, err = io.ReadAll(rc)
					if err != nil {
						return err
					}
				}
				payload = append(payload[:0], p...)
				return nil
			},
		})
		return res, payload, err
	}

	allResult, allPayload, err := run(QueryCaptureAllCandidates)
	if err != nil {
		t.Fatalf("all-candidates run: %v", err)
	}
	bestResult, bestPayload, err := run(QueryCaptureMatchesOnlyBestEffort)
	if err != nil {
		t.Fatalf("best-effort run: %v", err)
	}
	if !bytes.Equal(allPayload, bestPayload) {
		t.Fatalf("payload mismatch between capture policies")
	}
	if allResult.CandidatesMatched != bestResult.CandidatesMatched || allResult.CandidatesSeen != bestResult.CandidatesSeen {
		t.Fatalf("summary mismatch between policies: all=%+v best=%+v", allResult, bestResult)
	}
	if bestResult.BytesCaptured >= allResult.BytesCaptured {
		t.Fatalf("expected reduced capture bytes for best-effort policy: all=%d best=%d", allResult.BytesCaptured, bestResult.BytesCaptured)
	}
}

func TestQueryStreamCapturePolicyObjectRootEarlyNonMatchPrunesCapture(t *testing.T) {
	selector := mustParseQuerySelector(t, `not.eq{field=/status,value=closed}`)
	input := `{"status":"closed","blob":"` + strings.Repeat("x", 256*1024) + `"}`

	run := func(policy QueryStreamCapturePolicy) (QueryStreamResult, int, error) {
		callbacks := 0
		result, err := QueryStreamWithResult(QueryStreamRequest{
			Reader:        strings.NewReader(input),
			Selector:      selector,
			Mode:          QueryDecisionPlusValue,
			MatchedOnly:   true,
			CapturePolicy: policy,
			OnValue: func(QueryStreamValue) error {
				callbacks++
				return nil
			},
		})
		return result, callbacks, err
	}

	allResult, allCallbacks, err := run(QueryCaptureAllCandidates)
	if err != nil {
		t.Fatalf("all-candidates run: %v", err)
	}
	bestResult, bestCallbacks, err := run(QueryCaptureMatchesOnlyBestEffort)
	if err != nil {
		t.Fatalf("best-effort run: %v", err)
	}
	if allCallbacks != 0 || bestCallbacks != 0 {
		t.Fatalf("expected no callbacks for non-match, all=%d best=%d", allCallbacks, bestCallbacks)
	}
	if allResult.CandidatesSeen != 1 || bestResult.CandidatesSeen != 1 {
		t.Fatalf("unexpected seen counts all=%+v best=%+v", allResult, bestResult)
	}
	if allResult.CandidatesMatched != 0 || bestResult.CandidatesMatched != 0 {
		t.Fatalf("unexpected match counts all=%+v best=%+v", allResult, bestResult)
	}
	if bestResult.BytesCaptured >= allResult.BytesCaptured {
		t.Fatalf("expected object-root pruning to reduce capture bytes: all=%d best=%d", allResult.BytesCaptured, bestResult.BytesCaptured)
	}
}

func TestQueryStreamCapturePolicyObjectRootPreservesLateMatchPayload(t *testing.T) {
	selector := mustParseQuerySelector(t, `/status="open"`)
	input := `{"blob":"` + strings.Repeat("x", 128*1024) + `","status":"open"}`

	readPayload := func(policy QueryStreamCapturePolicy) (QueryStreamResult, []byte, error) {
		var payload []byte
		result, err := QueryStreamWithResult(QueryStreamRequest{
			Reader:        strings.NewReader(input),
			Selector:      selector,
			Mode:          QueryDecisionPlusValue,
			MatchedOnly:   true,
			CapturePolicy: policy,
			OnValue: func(v QueryStreamValue) error {
				if !v.Matched {
					t.Fatalf("expected matched payload")
				}
				if v.JSON != nil {
					payload = append(payload[:0], v.JSON...)
					return nil
				}
				rc, err := v.OpenJSON()
				if err != nil {
					return err
				}
				defer rc.Close()
				p, err := io.ReadAll(rc)
				if err != nil {
					return err
				}
				payload = append(payload[:0], p...)
				return nil
			},
		})
		return result, payload, err
	}

	allResult, allPayload, err := readPayload(QueryCaptureAllCandidates)
	if err != nil {
		t.Fatalf("all-candidates run: %v", err)
	}
	bestResult, bestPayload, err := readPayload(QueryCaptureMatchesOnlyBestEffort)
	if err != nil {
		t.Fatalf("best-effort run: %v", err)
	}
	if !bytes.Equal(allPayload, bestPayload) {
		t.Fatalf("payload mismatch for late-match object root")
	}
	if allResult.CandidatesMatched != 1 || bestResult.CandidatesMatched != 1 {
		t.Fatalf("expected one matched candidate, all=%+v best=%+v", allResult, bestResult)
	}
}

func TestQueryStreamWithResultIncludesSpillCounters(t *testing.T) {
	payload := `{"id":"a","blob":"` + strings.Repeat("x", 8*1024) + `"}`
	result, err := QueryStreamWithResult(QueryStreamRequest{
		Reader:           strings.NewReader(payload),
		Mode:             QueryDecisionPlusValue,
		SpoolMemoryBytes: 64,
		OnValue: func(QueryStreamValue) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("query stream: %v", err)
	}
	if result.SpillCount < 1 {
		t.Fatalf("expected at least one spill, got %+v", result)
	}
	if result.SpillBytes < int64(len(payload)) {
		t.Fatalf("expected spill bytes to include payload size, got %+v", result)
	}
	if result.BytesCaptured < int64(len(payload)) {
		t.Fatalf("expected captured bytes to include payload size, got %+v", result)
	}
}

func boolToString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
