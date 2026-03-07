package lql

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

const (
	selectorAllocCandidateCount                    = 6
	selectorAllocMessageSize                       = 256 * 1024
	selectorAllocArrayEntries                      = 16 * 1024
	selectorAllocDecisionBudgetPerCandidate        = 192 * 1024
	selectorAllocPlusValueBudgetPerCandidate       = 256 * 1024
	selectorAllocPlusValueSpoolMemoryBytes   int64 = 32 * 1024
)

func TestQueryStreamSelectorAllocBudgetDecisionOnlyLargeFields(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping selector alloc budget test in short mode")
	}
	payload := buildSelectorAllocPayload(t, selectorAllocCandidateCount, selectorAllocMessageSize, selectorAllocArrayEntries)
	arrayTailPath := fmt.Sprintf("/payload/%d", selectorAllocArrayEntries-1)

	cases := []struct {
		name string
		expr string
	}{
		{name: "eq_top_level_large_string", expr: `/message="no-match"`},
		{name: "eq_nested_large_string", expr: `/records/0/message="no-match"`},
		{name: "eq_top_level_temporal_date", expr: `/timestamp="2026-03-05"`},
		{name: "eq_nested_temporal_date", expr: `/records/0/timestamp="2026-03-05"`},
		{name: "contains_top_level_large_string", expr: `contains{field=/message,value=service}`},
		{name: "contains_any_top_level_large_string", expr: `contains{field=/message,any=service|timeout}`},
		{name: "icontains_nested_large_string", expr: `icontains{field=/records/0/message,value=timeout}`},
		{name: "icontains_any_nested_large_string", expr: `icontains{field=/records/0/message,any=timeout|degraded}`},
		{name: "prefix_top_level_large_string", expr: `prefix{field=/message,value=Timeout}`},
		{name: "iprefix_nested_large_string", expr: `iprefix{field=/records/0/message,value=timeout}`},
		{name: "in_top_level_large_string", expr: `in{field=/message,any=foo|bar|baz}`},
		{name: "in_nested_large_string", expr: `in{field=/records/0/message,any=foo|bar}`},
		{name: "range_top_level_scalar", expr: `range{field=/count,gte=40}`},
		{name: "range_top_level_temporal", expr: `/timestamp>=2026-03-05T10:28:21Z`},
		{name: "range_nested_temporal", expr: `/records/0/timestamp>=2026-03-05T10:28:21Z`},
		{name: "date_top_level_temporal", expr: `date{field=/timestamp,after=2026-03-05T10:28:21Z,before=2026-03-05T10:29:59Z}`},
		{name: "date_nested_temporal", expr: `date{field=/records/0/timestamp,after=2026-03-05T10:28:21Z,before=2026-03-05T10:29:59Z}`},
		{name: "range_large_array_tail", expr: fmt.Sprintf("range{field=%s,gte=0}", arrayTailPath)},
		{name: "exists_large_array", expr: `exists{/payload}`},
		{name: "and_combo_large_fields", expr: `and.contains{field=/message,value=Timeout},and.range{field=/count,gte=40}`},
		{name: "or_combo_large_fields", expr: `or.contains{field=/records/0/message,value=Timeout},or.eq{field=/kind,value=missing}`},
		{name: "not_combo", expr: `not.eq{field=/kind,value=missing}`},
	}

	var src bytes.Reader
	reader := bufio.NewReaderSize(&src, 64*1024)
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			selector := mustParseQuerySelector(t, tc.expr)
			result := testing.Benchmark(func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					src.Reset(payload)
					reader.Reset(&src)
					if err := QueryStream(QueryStreamRequest{
						Reader:   reader,
						Selector: selector,
						Mode:     QueryDecisionOnly,
						OnValue:  func(QueryStreamValue) error { return nil },
					}); err != nil {
						b.Fatalf("query stream: %v", err)
					}
				}
			})
			assertAllocBudgetPerCandidate(t, tc.name, result, selectorAllocCandidateCount, selectorAllocDecisionBudgetPerCandidate)
		})
	}
}

func TestQueryStreamSelectorAllocBudgetPlusValueLargeStrings(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping selector alloc budget test in short mode")
	}
	payload := buildSelectorAllocPayload(t, selectorAllocCandidateCount, selectorAllocMessageSize, selectorAllocArrayEntries)

	cases := []struct {
		name string
		expr string
	}{
		{name: "eq_top_level_large_string", expr: `/message="no-match"`},
		{name: "eq_top_level_temporal_date", expr: `/timestamp="2026-03-05"`},
		{name: "contains_top_level_large_string", expr: `contains{field=/message,value=service}`},
		{name: "contains_any_top_level_large_string", expr: `contains{field=/message,any=service|timeout}`},
		{name: "icontains_top_level_large_string", expr: `icontains{field=/message,value=timeout}`},
		{name: "icontains_any_top_level_large_string", expr: `icontains{field=/message,any=timeout|degraded}`},
		{name: "iprefix_top_level_large_string", expr: `iprefix{field=/message,value=timeout}`},
		{name: "contains_nested_large_string", expr: `contains{field=/records/0/message,value=service}`},
		{name: "in_top_level_large_string", expr: `in{field=/message,any=foo|bar|baz}`},
		{name: "range_top_level_temporal", expr: `/timestamp>=2026-03-05T10:28:21Z`},
		{name: "date_top_level_temporal", expr: `date{field=/timestamp,after=2026-03-05T10:28:21Z,before=2026-03-05T10:29:59Z}`},
	}

	var src bytes.Reader
	reader := bufio.NewReaderSize(&src, 64*1024)
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			selector := mustParseQuerySelector(t, tc.expr)
			result := testing.Benchmark(func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					src.Reset(payload)
					reader.Reset(&src)
					if err := QueryStream(QueryStreamRequest{
						Reader:           reader,
						Selector:         selector,
						Mode:             QueryDecisionPlusValue,
						SpoolMemoryBytes: selectorAllocPlusValueSpoolMemoryBytes,
						OnValue:          func(QueryStreamValue) error { return nil },
					}); err != nil {
						b.Fatalf("query stream: %v", err)
					}
				}
			})
			assertAllocBudgetPerCandidate(t, tc.name, result, selectorAllocCandidateCount, selectorAllocPlusValueBudgetPerCandidate)
		})
	}
}

func buildSelectorAllocPayload(t *testing.T, candidateCount, messageSize, arrayEntries int) []byte {
	t.Helper()
	if candidateCount <= 0 {
		t.Fatalf("invalid candidate count %d", candidateCount)
	}
	prefix := "Timeout service "
	suffix := " degraded"
	if messageSize <= len(prefix)+len(suffix) {
		t.Fatalf("message size too small: %d", messageSize)
	}
	message := prefix + strings.Repeat("x", messageSize-len(prefix)-len(suffix)) + suffix
	payloadArray := buildSelectorAllocArray(arrayEntries)

	var builder strings.Builder
	for i := 0; i < candidateCount; i++ {
		timestamp := fmt.Sprintf("2026-03-05T10:28:%02dZ", 21+(i%30))
		builder.WriteString(`{"id":"`)
		builder.WriteString(strconv.Itoa(i))
		builder.WriteString(`","kind":"target","count":42,"message":"`)
		builder.WriteString(message)
		builder.WriteString(`","timestamp":"`)
		builder.WriteString(timestamp)
		builder.WriteString(`","payload":[`)
		builder.WriteString(payloadArray)
		builder.WriteString(`],"records":[{"message":"`)
		builder.WriteString(message)
		builder.WriteString(`","timestamp":"`)
		builder.WriteString(timestamp)
		builder.WriteString(`","kind":"target","count":42}]}`)
		builder.WriteByte('\n')
	}
	return []byte(builder.String())
}

func buildSelectorAllocArray(entries int) string {
	if entries <= 0 {
		return ""
	}
	var builder strings.Builder
	for i := 0; i < entries; i++ {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(strconv.Itoa(i % 251))
	}
	return builder.String()
}
