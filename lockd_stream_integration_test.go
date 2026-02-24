package lql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLockdInlineQueryThenMutateHandoff(t *testing.T) {
	selector, err := ParseSelectorString(`/event="tabs_update"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	plan, err := NewQueryStreamPlan(selector)
	if err != nil {
		t.Fatalf("query plan: %v", err)
	}

	tempDir := t.TempDir()
	sinkFactory := NewReusableQueryPayloadSinkFactory(ReusableQueryPayloadSinkFactoryOptions{
		SpoolMemoryBytes: 16,
		SpoolTempDir:     tempDir,
		SpoolFilePattern: "lockd-inline-*.json",
	})
	defer func() {
		if err := sinkFactory.Close(); err != nil {
			t.Fatalf("close sink factory: %v", err)
		}
	}()

	input := strings.NewReader(
		`{"event":"tabs_update","component":"host","blob":"` + strings.Repeat("x", 2048) + `"}` + "\n" +
			`{"event":"noop","component":"host","blob":"` + strings.Repeat("y", 1024) + `"}` + "\n" +
			`{"event":"tabs_update","component":"host","blob":"` + strings.Repeat("z", 1536) + `"}` + "\n" +
			`{"event":"tabs_update","component":"host","blob":"` + strings.Repeat("w", 1536) + `"}`,
	)

	var decisions []QueryStreamDecision
	matchesSeen := 0
	muts, err := ParseMutations([]string{`/processed=true`, `time:/processed_at=NOW`}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}
	pipeR, pipeW := io.Pipe()
	type queryOutcome struct {
		result QueryStreamResult
		err    error
	}
	queryDone := make(chan queryOutcome, 1)
	go func() {
		queryResult, err := QueryStreamWithResult(QueryStreamRequest{
			Reader:               input,
			Plan:                 plan,
			Mode:                 QueryDecisionPlusValue,
			MatchedOnly:          true,
			CapturePolicy:        QueryCaptureMatchesOnlyBestEffort,
			DisableInternalSpool: true,
			PayloadSinkFactory:   sinkFactory.Factory(),
			OnDecision: func(d QueryStreamDecision) error {
				decisions = append(decisions, d)
				return nil
			},
			OnValue: func(v QueryStreamValue) error {
				if !v.Matched {
					return fmt.Errorf("matched-only callback received unmatched value")
				}
				matchesSeen++
				if v.OpenJSON == nil {
					return fmt.Errorf("expected OpenJSON handle")
				}
				rc, openErr := v.OpenJSON()
				if openErr != nil {
					return openErr
				}
				defer rc.Close()
				if _, copyErr := io.Copy(pipeW, rc); copyErr != nil {
					return copyErr
				}
				if _, writeErr := pipeW.Write([]byte{'\n'}); writeErr != nil {
					return writeErr
				}
				if matchesSeen >= 2 {
					return ErrStreamStop
				}
				return nil
			},
		})
		if err != nil {
			_ = pipeW.CloseWithError(err)
		} else {
			_ = pipeW.Close()
		}
		queryDone <- queryOutcome{result: queryResult, err: err}
	}()

	var mutateOut bytes.Buffer
	mutateResult, err := MutateStreamWithResult(MutateStreamRequest{
		Reader:    pipeR,
		Writer:    &mutateOut,
		Mutations: muts,
	})
	if err != nil {
		t.Fatalf("mutate stream with result: %v", err)
	}
	query := <-queryDone
	if query.err != nil {
		t.Fatalf("query stream with result: %v", query.err)
	}
	if !query.result.StoppedEarly || query.result.StopReason != QueryStreamStopCallbackStop {
		t.Fatalf("expected callback-stop query result, got %+v", query.result)
	}
	if query.result.CandidatesMatched != 2 {
		t.Fatalf("expected 2 matched candidates before stop, got %+v", query.result)
	}
	if len(decisions) != int(query.result.CandidatesSeen) {
		t.Fatalf("decision coverage mismatch: decisions=%d candidates_seen=%d", len(decisions), query.result.CandidatesSeen)
	}
	if mutateResult.CandidatesSeen != query.result.CandidatesMatched {
		t.Fatalf("expected mutate candidates to match query matches: mutate=%d query=%d", mutateResult.CandidatesSeen, query.result.CandidatesMatched)
	}
	if mutateResult.CandidatesWritten != query.result.CandidatesMatched {
		t.Fatalf("expected mutate writes to match query matches: mutate=%d query=%d", mutateResult.CandidatesWritten, query.result.CandidatesMatched)
	}

	lines := strings.Split(strings.TrimSpace(mutateOut.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 mutated docs, got %d", len(lines))
	}
	for i := range lines {
		var doc map[string]any
		if err := json.Unmarshal([]byte(lines[i]), &doc); err != nil {
			t.Fatalf("decode mutated doc %d: %v", i, err)
		}
		if doc["processed"] != true {
			t.Fatalf("expected processed=true in doc %d, got %#v", i, doc["processed"])
		}
		if _, ok := doc["processed_at"].(string); !ok {
			t.Fatalf("expected processed_at timestamp in doc %d, got %#v", i, doc["processed_at"])
		}
	}

	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("read temp dir: %v", err)
	}
	foundSpoolFile := false
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".json" {
			foundSpoolFile = true
			break
		}
	}
	if !foundSpoolFile {
		t.Fatalf("expected reusable sink factory spill file in %s", tempDir)
	}
}

func TestLockdFusedQueryMutateHandoff(t *testing.T) {
	selector, err := ParseSelectorString(`/event="tabs_update"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	queryPlan, err := NewQueryStreamPlan(selector)
	if err != nil {
		t.Fatalf("query plan: %v", err)
	}
	muts, err := ParseMutations([]string{`/processed=true`, `time:/processed_at=NOW`}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}
	mutatePlan, err := NewMutateStreamPlan(muts)
	if err != nil {
		t.Fatalf("mutate plan: %v", err)
	}

	var out bytes.Buffer
	result, err := QueryMutateStreamWithResult(QueryMutateStreamRequest{
		Reader: strings.NewReader(
			`{"event":"tabs_update","component":"host","blob":"` + strings.Repeat("x", 2048) + `"}` + "\n" +
				`{"event":"noop","component":"host","blob":"` + strings.Repeat("y", 1024) + `"}` + "\n" +
				`{"event":"tabs_update","component":"host","blob":"` + strings.Repeat("z", 1536) + `"}` + "\n" +
				`{"event":"tabs_update","component":"host","blob":"` + strings.Repeat("w", 1536) + `"}`,
		),
		Writer:             &out,
		QueryPlan:          queryPlan,
		MutatePlan:         mutatePlan,
		MaxMatches:         2,
		QueryCapturePolicy: QueryCaptureMatchesOnlyBestEffort,
	})
	if err != nil {
		t.Fatalf("query mutate stream with result: %v", err)
	}
	if !result.Query.StoppedEarly || result.Query.StopReason != QueryStreamStopMatchLimit {
		t.Fatalf("expected match-limit query stop, got %+v", result.Query)
	}
	if result.Query.CandidatesMatched != 2 {
		t.Fatalf("expected 2 query matches, got %+v", result.Query)
	}
	if result.Mutate.CandidatesSeen != 2 || result.Mutate.CandidatesWritten != 2 {
		t.Fatalf("expected 2 mutated docs, got %+v", result.Mutate)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 mutated docs, got %d", len(lines))
	}
	for i := range lines {
		var doc map[string]any
		if err := json.Unmarshal([]byte(lines[i]), &doc); err != nil {
			t.Fatalf("decode mutated doc %d: %v", i, err)
		}
		if doc["processed"] != true {
			t.Fatalf("expected processed=true in doc %d, got %#v", i, doc["processed"])
		}
		if _, ok := doc["processed_at"].(string); !ok {
			t.Fatalf("expected processed_at timestamp in doc %d, got %#v", i, doc["processed_at"])
		}
	}
}
