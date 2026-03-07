package lql

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
)

const (
	selectorFuzzAllocReplayRepeats         = 64
	selectorFuzzAllocBudgetPerCandidate    = 128
	selectorFuzzAllocDecisionOnlyBenchMode = QueryDecisionOnly
)

func TestQueryStreamSelectorAllocBudgetFuzzReplayTemporal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping selector fuzz alloc guard in short mode")
	}

	selectors := []struct {
		name string
		expr string
	}{
		{name: "temporal_eq_date_only", expr: `/timestamp="2026-03-05"`},
		{name: "temporal_range_shorthand_gte", expr: `/timestamp>=2026-03-05T10:28:21Z`},
		{name: "temporal_range_selector", expr: `range{field=/timestamp,gte=2026-03-05T10:28:21Z,lt=2026-03-05T10:30:00Z}`},
		{name: "date_selector_after_before", expr: `date{field=/timestamp,after=2026-03-05T10:28:21Z,before=2026-03-05T10:30:00Z}`},
		{name: "date_selector_since_macro", expr: `date{f=/timestamp,since=yesterday}`},
	}

	seedInputs := []struct {
		name     string
		seed     []byte
		topArray bool
	}{
		{name: "alpha_ndjson", seed: []byte("alpha"), topArray: false},
		{name: "beta_array", seed: []byte("beta"), topArray: true},
		{name: "gamma_ndjson", seed: []byte("gamma"), topArray: false},
		{name: "binary_array", seed: []byte{0, 1, 2, 3, 4}, topArray: true},
	}

	onDecision := func(QueryStreamDecision) error { return nil }

	for _, selectorCase := range selectors {
		selectorCase := selectorCase
		selector := mustParseQuerySelector(t, selectorCase.expr)
		plan, err := NewQueryStreamPlan(selector)
		if err != nil {
			t.Fatalf("new query plan for %q: %v", selectorCase.expr, err)
		}

		for _, seedCase := range seedInputs {
			seedCase := seedCase
			name := fmt.Sprintf("%s/%s", selectorCase.name, seedCase.name)
			t.Run(name, func(t *testing.T) {
				payload := buildSelectorFuzzReplayPayload(seedCase.seed, seedCase.topArray, selectorFuzzAllocReplayRepeats)
				candidateCount := countQueryStreamCandidates(t, payload)
				if candidateCount <= 0 {
					t.Fatalf("invalid candidate count %d", candidateCount)
				}

				var src bytes.Reader
				reader := bufio.NewReaderSize(&src, 64*1024)
				result := testing.Benchmark(func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						src.Reset(payload)
						reader.Reset(&src)
						_, runErr := QueryStreamWithResult(QueryStreamRequest{
							Reader:     reader,
							Plan:       plan,
							Mode:       selectorFuzzAllocDecisionOnlyBenchMode,
							OnDecision: onDecision,
						})
						if runErr != nil {
							b.Fatalf("query stream: %v", runErr)
						}
					}
				})

				assertAllocBudgetPerCandidate(t, name, result, candidateCount, selectorFuzzAllocBudgetPerCandidate)
			})
		}
	}
}

func buildSelectorFuzzReplayPayload(seed []byte, topArray bool, repeats int) []byte {
	if repeats <= 0 {
		repeats = 1
	}
	chunk := synthesizeParityStream(seed, topArray)
	if len(chunk) == 0 {
		return chunk
	}
	var buf bytes.Buffer
	buf.Grow((len(chunk) + 1) * repeats)
	for i := 0; i < repeats; i++ {
		buf.Write(chunk)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

func countQueryStreamCandidates(t *testing.T, payload []byte) int {
	t.Helper()
	result, err := QueryStreamWithResult(QueryStreamRequest{
		Reader: bytes.NewReader(payload),
		Mode:   QueryDecisionOnly,
		OnDecision: func(QueryStreamDecision) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("count candidates: %v", err)
	}
	return int(result.CandidatesSeen)
}
