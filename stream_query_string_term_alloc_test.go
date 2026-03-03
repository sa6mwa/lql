package lql

import (
	"bufio"
	"bytes"
	"testing"
)

func TestQueryStreamStringTermHotPathZeroAllocs(t *testing.T) {
	cases := []string{
		`contains{f=/hello/world}`,
		`icontains{f=/hello/world}`,
		`prefix{f=/hello/world}`,
		`iprefix{f=/hello/world}`,
		`contains{f=/hello/world,v=needle}`,
		`icontains{f=/hello/world,v=needle}`,
		`contains{f=/hello/world,a=needle|other}`,
	}

	small := []byte(`{"hello":{"world":"needle"}}` + "\n")
	var largeBuf bytes.Buffer
	for i := 0; i < 2000; i++ {
		largeBuf.Write(small)
	}
	large := largeBuf.Bytes()

	for _, expr := range cases {
		expr := expr
		t.Run(expr, func(t *testing.T) {
			selector := mustParseQuerySelector(t, expr)
			plan, err := NewQueryStreamPlan(selector)
			if err != nil {
				t.Fatalf("new query plan: %v", err)
			}

			var src bytes.Reader
			reader := bufio.NewReaderSize(&src, 64*1024)
			onDecision := func(QueryStreamDecision) error { return nil }
			run := func(payload []byte) float64 {
				return testing.AllocsPerRun(200, func() {
					src.Reset(payload)
					reader.Reset(&src)
					_, runErr := QueryStreamWithResult(QueryStreamRequest{
						Reader:     reader,
						Plan:       plan,
						Mode:       QueryDecisionOnly,
						OnDecision: onDecision,
					})
					if runErr != nil {
						t.Fatalf("query stream: %v", runErr)
					}
				})
			}

			smallAllocs := run(small)
			largeAllocs := run(large)
			if largeAllocs > smallAllocs {
				t.Fatalf("expected zero per-candidate allocations, got small=%.2f large=%.2f", smallAllocs, largeAllocs)
			}
		})
	}
}
