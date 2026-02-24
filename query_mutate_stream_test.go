package lql

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestQueryMutateStreamParityWithTwoStagePipeline(t *testing.T) {
	input := []byte(
		`{"event":"tabs_update","component":"host","id":1}` + "\n" +
			`{"event":"noop","component":"host","id":2}` + "\n" +
			`{"event":"tabs_update","component":"host","id":3}` + "\n" +
			`{"event":"tabs_update","component":"host","id":4}`,
	)
	selector, err := ParseSelectorString(`/event="tabs_update"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	queryPlan, err := NewQueryStreamPlan(selector)
	if err != nil {
		t.Fatalf("new query plan: %v", err)
	}
	muts, err := ParseMutations([]string{`/processed=true`, `time:/processed_at=NOW`}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}
	mutatePlan, err := NewMutateStreamPlan(muts)
	if err != nil {
		t.Fatalf("new mutate plan: %v", err)
	}

	var fusedOut bytes.Buffer
	fused, err := QueryMutateStreamWithResult(QueryMutateStreamRequest{
		Reader:     bytes.NewReader(input),
		Writer:     &fusedOut,
		QueryPlan:  queryPlan,
		MutatePlan: mutatePlan,
	})
	if err != nil {
		t.Fatalf("query mutate stream: %v", err)
	}

	legacyOut, legacyQuery, legacyMutate, err := runTwoStageQueryMutatePipeline(input, queryPlan, mutatePlan, 0)
	if err != nil {
		t.Fatalf("legacy pipeline: %v", err)
	}

	if fusedOut.String() != legacyOut {
		t.Fatalf("mutated output mismatch:\n--- fused ---\n%s\n--- legacy ---\n%s", fusedOut.String(), legacyOut)
	}
	if fused.Query.CandidatesSeen != legacyQuery.CandidatesSeen ||
		fused.Query.CandidatesMatched != legacyQuery.CandidatesMatched {
		t.Fatalf("query summary mismatch: fused=%+v legacy=%+v", fused.Query, legacyQuery)
	}
	if fused.Mutate.CandidatesWritten != legacyMutate.CandidatesWritten ||
		fused.Mutate.CandidatesSeen != legacyMutate.CandidatesSeen {
		t.Fatalf("mutate summary mismatch: fused=%+v legacy=%+v", fused.Mutate, legacyMutate)
	}
}

func TestQueryMutateStreamMaxMatchesStopsEarly(t *testing.T) {
	input := []byte(
		`{"event":"tabs_update","id":1}` + "\n" +
			`{"event":"tabs_update","id":2}` + "\n" +
			`{"event":"tabs_update","id":3}`,
	)
	selector, err := ParseSelectorString(`/event="tabs_update"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	queryPlan, err := NewQueryStreamPlan(selector)
	if err != nil {
		t.Fatalf("new query plan: %v", err)
	}
	muts, err := ParseMutations([]string{`/processed=true`}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}
	mutatePlan, err := NewMutateStreamPlan(muts)
	if err != nil {
		t.Fatalf("new mutate plan: %v", err)
	}

	var out bytes.Buffer
	result, err := QueryMutateStreamWithResult(QueryMutateStreamRequest{
		Reader:     bytes.NewReader(input),
		Writer:     &out,
		QueryPlan:  queryPlan,
		MutatePlan: mutatePlan,
		MaxMatches: 1,
	})
	if err != nil {
		t.Fatalf("query mutate stream: %v", err)
	}
	if !result.Query.StoppedEarly || result.Query.StopReason != QueryStreamStopMatchLimit {
		t.Fatalf("expected match-limit stop, got %+v", result.Query)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected exactly one mutated document, got %d", len(lines))
	}
	if result.Mutate.CandidatesWritten != 1 {
		t.Fatalf("expected one mutated candidate, got %+v", result.Mutate)
	}
}

func TestQueryMutateStreamValidationErrors(t *testing.T) {
	_, err := QueryMutateStreamWithResult(QueryMutateStreamRequest{})
	if err == nil || !IsStreamInvalidBody(err) {
		t.Fatalf("expected invalid body for missing reader/writer, got %v", err)
	}

	selector, err := ParseSelectorString(`/event="tabs_update"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	queryPlan, err := NewQueryStreamPlan(selector)
	if err != nil {
		t.Fatalf("query plan: %v", err)
	}
	_, err = QueryMutateStreamWithResult(QueryMutateStreamRequest{
		Reader:    bytes.NewReader(nil),
		Writer:    io.Discard,
		Selector:  selector,
		QueryPlan: queryPlan,
		Mutations: []Mutation{{Kind: MutationSet, Path: []string{"x"}, Value: "1"}},
	})
	if err == nil || !IsStreamInvalidBody(err) {
		t.Fatalf("expected invalid body for selector+plan conflict, got %v", err)
	}
}

func TestQueryMutateStreamUsesInlineJSONBeforeOpenJSON(t *testing.T) {
	input := []byte(`{"event":"tabs_update","id":1}`)
	selector, err := ParseSelectorString(`/event="tabs_update"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	queryPlan, err := NewQueryStreamPlan(selector)
	if err != nil {
		t.Fatalf("new query plan: %v", err)
	}
	muts, err := ParseMutations([]string{`/processed=true`}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}
	mutatePlan, err := NewMutateStreamPlan(muts)
	if err != nil {
		t.Fatalf("new mutate plan: %v", err)
	}

	openErr := errors.New("open should not be called for inline json")
	sinkFactory := func(QueryStreamPayloadSinkRequest) (QueryStreamPayloadSink, error) {
		return &queryMutateInlinePayloadSink{openErr: openErr}, nil
	}

	var out bytes.Buffer
	result, err := QueryMutateStreamWithResult(QueryMutateStreamRequest{
		Reader:                  bytes.NewReader(input),
		Writer:                  &out,
		QueryPlan:               queryPlan,
		MutatePlan:              mutatePlan,
		QueryDisableSpool:       true,
		QueryPayloadSinkFactory: sinkFactory,
	})
	if err != nil {
		t.Fatalf("query mutate stream: %v", err)
	}
	if result.Query.CandidatesMatched != 1 || result.Mutate.CandidatesWritten != 1 {
		t.Fatalf("unexpected summary: query=%+v mutate=%+v", result.Query, result.Mutate)
	}
	if !strings.Contains(out.String(), `"processed":true`) {
		t.Fatalf("expected processed mutation in output, got %q", out.String())
	}
}

type queryMutateInlinePayloadSink struct {
	buf     []byte
	openErr error
}

func (s *queryMutateInlinePayloadSink) Write(p []byte) (int, error) {
	s.buf = append(s.buf, p...)
	return len(p), nil
}

func (s *queryMutateInlinePayloadSink) Finalize() error {
	return nil
}

func (s *queryMutateInlinePayloadSink) Open() (io.ReadCloser, error) {
	if s.openErr != nil {
		return nil, s.openErr
	}
	return io.NopCloser(bytes.NewReader(s.buf)), nil
}

func (s *queryMutateInlinePayloadSink) Bytes() []byte {
	return s.buf
}

func (s *queryMutateInlinePayloadSink) SizeHint() int {
	return len(s.buf)
}

func (s *queryMutateInlinePayloadSink) Cleanup() error {
	s.buf = s.buf[:0]
	return nil
}

func runTwoStageQueryMutatePipeline(
	input []byte,
	queryPlan QueryStreamPlan,
	mutatePlan MutateStreamPlan,
	maxMatches int64,
) (string, QueryStreamResult, MutateStreamResult, error) {
	pipeR, pipeW := io.Pipe()
	type queryOutcome struct {
		result QueryStreamResult
		err    error
	}
	queryDone := make(chan queryOutcome, 1)
	go func() {
		res, err := QueryStreamWithResult(QueryStreamRequest{
			Reader:      bytes.NewReader(input),
			Plan:        queryPlan,
			Mode:        QueryDecisionPlusValue,
			MatchedOnly: true,
			MaxMatches:  maxMatches,
			OnValue: func(v QueryStreamValue) error {
				rc, err := v.OpenJSON()
				if err != nil {
					return err
				}
				defer rc.Close()
				if _, err := io.Copy(pipeW, rc); err != nil {
					return err
				}
				_, err = pipeW.Write(jsonNewline)
				return err
			},
		})
		if err != nil {
			_ = pipeW.CloseWithError(err)
		} else {
			_ = pipeW.Close()
		}
		queryDone <- queryOutcome{result: res, err: err}
	}()

	var out bytes.Buffer
	mutateRes, mutateErr := MutateStreamWithResult(MutateStreamRequest{
		Reader: pipeR,
		Writer: &out,
		Plan:   mutatePlan,
	})
	query := <-queryDone
	if query.err != nil {
		return "", QueryStreamResult{}, MutateStreamResult{}, query.err
	}
	if mutateErr != nil {
		return "", QueryStreamResult{}, MutateStreamResult{}, mutateErr
	}
	return out.String(), query.result, mutateRes, nil
}
