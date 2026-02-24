package lql

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
)

// QueryMutateStreamRequest configures fused query+mutate streaming.
//
// It selects matched candidates from Reader and applies mutations to each
// matched candidate in streaming order, writing mutated output to Writer.
type QueryMutateStreamRequest struct {
	Ctx    context.Context
	Reader io.Reader
	Writer io.Writer

	Selector  Selector
	QueryPlan QueryStreamPlan

	Mutations  []Mutation
	MutatePlan MutateStreamPlan

	// Query-side limits/capture options.
	QueryCapturePolicy      QueryStreamCapturePolicy
	QuerySpoolMemoryBytes   int64
	QuerySpoolTempDir       string
	QuerySpoolFilePattern   string
	QueryDisableSpool       bool
	QueryPayloadSinkFactory QueryStreamPayloadSinkFactory
	QueryMaxCandidateBytes  int64
	MaxMatches              int64
	MaxCandidates           int64
	MaxBytesRead            int64

	// Mutate-side options.
	MutateMode               MutateStreamMode
	MutateSpoolMemoryBytes   int64
	MutateSpoolTempDir       string
	MutateSpoolFilePattern   string
	MutateDisableSpool       bool
	MutatePayloadSinkFactory MutateStreamPayloadSinkFactory
	MutateMaxCandidateBytes  int64

	OnDecision func(QueryStreamDecision) error
}

// QueryMutateStreamResult reports fused query+mutate run summaries.
type QueryMutateStreamResult struct {
	Query  QueryStreamResult
	Mutate MutateStreamResult
}

type queryMutateAsyncState struct {
	done   chan struct{}
	result MutateStreamResult
	err    error
}

var queryMutateAsyncStatePool = sync.Pool{
	New: func() any {
		return &queryMutateAsyncState{
			done: make(chan struct{}, 1),
		}
	},
}

func acquireQueryMutateAsyncState() *queryMutateAsyncState {
	state := queryMutateAsyncStatePool.Get().(*queryMutateAsyncState)
	state.result = MutateStreamResult{}
	state.err = nil
	select {
	case <-state.done:
	default:
	}
	return state
}

func releaseQueryMutateAsyncState(state *queryMutateAsyncState) {
	if state == nil {
		return
	}
	state.result = MutateStreamResult{}
	state.err = nil
	queryMutateAsyncStatePool.Put(state)
}

// QueryMutateStreamWithResult selects matched candidates from Reader and
// applies mutations to each matched candidate, writing mutated output to
// Writer without pipe handoff orchestration.
func QueryMutateStreamWithResult(req QueryMutateStreamRequest) (QueryMutateStreamResult, error) {
	out := QueryMutateStreamResult{
		Query:  QueryStreamResult{LastOffset: -1},
		Mutate: MutateStreamResult{LastOffset: -1},
	}
	if req.Reader == nil {
		return out, &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: "query mutate stream reader required",
			Offset: -1,
		}
	}
	if req.Writer == nil {
		return out, &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: "query mutate stream writer required",
			Offset: -1,
		}
	}
	if !req.QueryPlan.IsZero() && !req.Selector.IsEmpty() {
		return out, &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: "query mutate stream request must set either selector or query plan, not both",
			Offset: -1,
		}
	}
	if !req.MutatePlan.IsZero() && len(req.Mutations) > 0 {
		return out, &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: "query mutate stream request must set either mutations or mutate plan, not both",
			Offset: -1,
		}
	}
	if req.MutatePlan.IsZero() && len(req.Mutations) == 0 {
		return out, &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: "query mutate stream requires mutations or mutate plan",
			Offset: -1,
		}
	}
	switch req.MutateMode {
	case MutateModeAuto, MutateSingleValueOnly, MutateObjectRootOnly, MutateSingleObjectOnly:
	default:
		return out, &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: fmt.Sprintf("unknown mutate stream mode %d", req.MutateMode),
			Offset: -1,
		}
	}

	mutatePlan := req.MutatePlan
	if mutatePlan.IsZero() {
		compiled, err := NewMutateStreamPlan(req.Mutations)
		if err != nil {
			return out, &StreamError{
				Code:   StreamErrorInvalidBody,
				Detail: "invalid mutation program",
				Offset: -1,
				Err:    err,
			}
		}
		mutatePlan = compiled
	}
	if mutatePlan.template == nil {
		return out, &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: "query mutate stream mutate plan is invalid",
			Offset: -1,
		}
	}

	pipeR, pipeW := io.Pipe()
	mutateAsync := acquireQueryMutateAsyncState()
	defer releaseQueryMutateAsyncState(mutateAsync)
	var inlineReader bytes.Reader
	go func() {
		mutateAsync.result, mutateAsync.err = MutateStreamWithResult(MutateStreamRequest{
			Ctx:                  req.Ctx,
			Reader:               pipeR,
			Writer:               req.Writer,
			Plan:                 mutatePlan,
			Mode:                 req.MutateMode,
			SpoolMemoryBytes:     req.MutateSpoolMemoryBytes,
			SpoolTempDir:         req.MutateSpoolTempDir,
			SpoolFilePattern:     req.MutateSpoolFilePattern,
			DisableInternalSpool: req.MutateDisableSpool,
			PayloadSinkFactory:   req.MutatePayloadSinkFactory,
			MaxCandidateBytes:    req.MutateMaxCandidateBytes,
		})
		mutateAsync.done <- struct{}{}
	}()

	queryResult, queryErr := QueryStreamWithResult(QueryStreamRequest{
		Ctx:                  req.Ctx,
		Reader:               req.Reader,
		Selector:             req.Selector,
		Plan:                 req.QueryPlan,
		Mode:                 QueryDecisionPlusValue,
		MatchedOnly:          true,
		CapturePolicy:        req.QueryCapturePolicy,
		SpoolMemoryBytes:     req.QuerySpoolMemoryBytes,
		SpoolTempDir:         req.QuerySpoolTempDir,
		SpoolFilePattern:     req.QuerySpoolFilePattern,
		DisableInternalSpool: req.QueryDisableSpool,
		PayloadSinkFactory:   req.QueryPayloadSinkFactory,
		MaxCandidateBytes:    req.QueryMaxCandidateBytes,
		MaxMatches:           req.MaxMatches,
		MaxCandidates:        req.MaxCandidates,
		MaxBytesRead:         req.MaxBytesRead,
		OnDecision:           req.OnDecision,
		OnValue: func(v QueryStreamValue) error {
			if v.JSON != nil {
				inlineReader.Reset(v.JSON)
				if _, err := io.Copy(pipeW, &inlineReader); err != nil {
					return err
				}
				_, err := pipeW.Write(jsonNewline)
				return err
			}

			open := v.OpenJSON
			if open == nil {
				return fmt.Errorf("query mutate stream matched payload unavailable")
			}
			rc, err := open()
			if err != nil {
				return err
			}
			_, err = io.Copy(pipeW, rc)
			closeErr := rc.Close()
			if closeErr != nil {
				return closeErr
			}
			if err != nil {
				return err
			}
			_, err = pipeW.Write(jsonNewline)
			return err
		},
	})
	if queryErr != nil {
		_ = pipeW.CloseWithError(queryErr)
	} else {
		_ = pipeW.Close()
	}

	<-mutateAsync.done
	if queryErr != nil {
		return out, queryErr
	}
	if mutateAsync.err != nil {
		return out, mutateAsync.err
	}

	out.Query = queryResult
	out.Mutate = mutateAsync.result
	return out, nil
}

// QueryMutateStream runs QueryMutateStreamWithResult and discards summary.
func QueryMutateStream(req QueryMutateStreamRequest) error {
	_, err := QueryMutateStreamWithResult(req)
	return err
}
