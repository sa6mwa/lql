package lql

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type queryRealworldDataset struct {
	name    string
	payload []byte
}

const (
	querySyntheticEventSparseTarget     = "session_sync"
	querySyntheticComponentDenseTarget  = "edge"
	querySyntheticHashSparseTarget      = "c5d2460186f7233c927e7db2dcc703c0a3a8e0d5f0d8a3c5b4f1e2d3c4b5a697"
	querySyntheticSessionIDSparseTarget = "sid-0a3f-target"
)

var (
	queryRealworldOnce sync.Once
	queryRealworldData []queryRealworldDataset
	queryRealworldErr  error
)

func BenchmarkQueryStreamRealworld(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping realworld query benchmark in short mode")
	}
	datasets, err := loadQueryRealworldDatasets()
	if err != nil {
		b.Skipf("skip realworld benchmark: %v", err)
	}

	cases := []struct {
		name string
		expr string
	}{
		{name: "eq_sparse", expr: `/event="session_sync"`},
		{name: "eq_dense", expr: `/component="edge"`},
		{name: "eq_none", expr: `/event="__nope__"`},
		{name: "range_sparse", expr: `/code>=11`},
		{name: "nested_eq_sparse", expr: `/query/hash="c5d2460186f7233c927e7db2dcc703c0a3a8e0d5f0d8a3c5b4f1e2d3c4b5a697"`},
		{name: "array_eq_sparse", expr: `/session_ids[]="sid-0a3f-target"`},
		{name: "recursive_eq_sparse", expr: `/.../event="session_sync"`},
		{name: "recursive_nested_eq_sparse", expr: `/.../hash="c5d2460186f7233c927e7db2dcc703c0a3a8e0d5f0d8a3c5b4f1e2d3c4b5a697"`},
		{name: "contains_event_sparse", expr: `contains{field=/event,value=sync}`},
		{name: "icontains_component_dense", expr: `icontains{field=/component,value=EDGE}`},
		{name: "contains_any_event_sparse", expr: `contains{field=/event,any=sync|__nope__}`},
		{name: "icontains_any_component_dense", expr: `icontains{field=/component,any=EDGE|__nope__}`},
		{name: "multi_clause_and", expr: `/component="edge",/event="session_sync",/active_idx=0,/tab_count=1,exists{/session_ids},/code>=10`},
	}

	for _, ds := range datasets {
		ds := ds
		b.Run(ds.name, func(b *testing.B) {
			b.SetBytes(int64(len(ds.payload)))
			for _, tc := range cases {
				tc := tc
				selector, err := ParseSelectorString(tc.expr)
				if err != nil {
					b.Fatalf("parse selector %s: %v", tc.name, err)
				}
				plan, err := NewQueryStreamPlan(selector)
				if err != nil {
					b.Fatalf("new plan %s: %v", tc.name, err)
				}
				b.Run(tc.name, func(b *testing.B) {
					b.Run("decision_only_plan", func(b *testing.B) {
						var src bytes.Reader
						reader := bufio.NewReaderSize(&src, 64*1024)
						onValue := func(QueryStreamValue) error { return nil }
						runBenchmarkModes(b, func() error {
							src.Reset(ds.payload)
							reader.Reset(&src)
							return QueryStream(QueryStreamRequest{
								Reader:  reader,
								Plan:    plan,
								Mode:    QueryDecisionOnly,
								OnValue: onValue,
							})
						})
					})
					b.Run("decision_only_selector", func(b *testing.B) {
						var src bytes.Reader
						reader := bufio.NewReaderSize(&src, 64*1024)
						onValue := func(QueryStreamValue) error { return nil }
						runBenchmarkModes(b, func() error {
							src.Reset(ds.payload)
							reader.Reset(&src)
							return QueryStream(QueryStreamRequest{
								Reader:   reader,
								Selector: selector,
								Mode:     QueryDecisionOnly,
								OnValue:  onValue,
							})
						})
					})
					b.Run("plus_value_plan", func(b *testing.B) {
						var src bytes.Reader
						reader := bufio.NewReaderSize(&src, 64*1024)
						onValue := func(QueryStreamValue) error { return nil }
						runBenchmarkModes(b, func() error {
							src.Reset(ds.payload)
							reader.Reset(&src)
							return QueryStream(QueryStreamRequest{
								Reader:  reader,
								Plan:    plan,
								Mode:    QueryDecisionPlusValue,
								OnValue: onValue,
							})
						})
					})
					b.Run("plus_value_selector", func(b *testing.B) {
						var src bytes.Reader
						reader := bufio.NewReaderSize(&src, 64*1024)
						onValue := func(QueryStreamValue) error { return nil }
						runBenchmarkModes(b, func() error {
							src.Reset(ds.payload)
							reader.Reset(&src)
							return QueryStream(QueryStreamRequest{
								Reader:   reader,
								Selector: selector,
								Mode:     QueryDecisionPlusValue,
								OnValue:  onValue,
							})
						})
					})
				})
			}
		})
	}
}

func loadQueryRealworldDatasets() ([]queryRealworldDataset, error) {
	queryRealworldOnce.Do(func() {
		profiles := []struct {
			name          string
			count         int
			prettyStream  bool
			nestedPayload bool
		}{
			{name: "synthetic_ndjson_compact", count: 4096, prettyStream: false, nestedPayload: false},
			{name: "synthetic_pretty_stream_nested", count: 1536, prettyStream: true, nestedPayload: true},
		}
		for _, profile := range profiles {
			payload, err := buildSyntheticQueryRealworldPayload(profile.count, profile.prettyStream, profile.nestedPayload)
			if err != nil {
				queryRealworldErr = fmt.Errorf("build %s: %w", profile.name, err)
				return
			}
			queryRealworldData = append(queryRealworldData, queryRealworldDataset{
				name:    profile.name,
				payload: payload,
			})
		}
	})
	if queryRealworldErr != nil {
		return nil, queryRealworldErr
	}
	return queryRealworldData, nil
}

func buildSyntheticQueryRealworldPayload(count int, prettyStream, nestedPayload bool) ([]byte, error) {
	if count <= 0 {
		return nil, fmt.Errorf("invalid synthetic realworld count %d", count)
	}
	if prettyStream {
		var buf bytes.Buffer
		for i := 0; i < count; i++ {
			doc := buildSyntheticQueryRealworldRecord(i, nestedPayload)
			payload, err := json.MarshalIndent(doc, "", "  ")
			if err != nil {
				return nil, err
			}
			if _, err := buf.Write(payload); err != nil {
				return nil, err
			}
			buf.WriteByte('\n')
		}
		return buf.Bytes(), nil
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for i := 0; i < count; i++ {
		if err := enc.Encode(buildSyntheticQueryRealworldRecord(i, nestedPayload)); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func buildSyntheticQueryRealworldRecord(i int, nestedPayload bool) map[string]any {
	event := syntheticQueryRealworldEvent(i)
	component := syntheticQueryRealworldComponent(i)
	code := 3 + (i % 11)
	activeIdx := i % 4
	tabCount := 2 + (i % 4)
	if event == querySyntheticEventSparseTarget {
		activeIdx = 0
		tabCount = 1
		if code < 10 {
			code = 10 + (i % 4)
		}
	}

	hash := syntheticQueryRealworldHash(i)
	sessionIDs := syntheticQueryRealworldSessionIDs(i)
	if len(sessionIDs) == 0 && event == querySyntheticEventSparseTarget {
		sessionIDs = []string{
			fmt.Sprintf("sid-%04d-a", i),
			fmt.Sprintf("sid-%04d-b", i),
		}
	}

	record := map[string]any{
		"event":      event,
		"component":  component,
		"code":       code,
		"active_idx": activeIdx,
		"tab_count":  tabCount,
		"query": map[string]any{
			"hash":        hash,
			"latency_ms":  12 + (i % 180),
			"fingerprint": fmt.Sprintf("fp-%04d", (i*17)%4096),
		},
		"payload":   syntheticQueryRealworldPayloadValue(i, nestedPayload),
		"timestamp": fmt.Sprintf("2026-03-10T12:%02d:%02dZ", i%60, (i*7)%60),
		"meta": map[string]any{
			"zone":      [...]string{"eu-north", "us-east", "ap-south"}[i%3],
			"build":     fmt.Sprintf("build-%03d", i%200),
			"retryable": i%5 == 0,
		},
	}
	if len(sessionIDs) > 0 {
		record["session_ids"] = sessionIDs
	}
	return record
}

func syntheticQueryRealworldEvent(i int) string {
	if i%61 == 0 {
		return querySyntheticEventSparseTarget
	}
	return [...]string{"heartbeat", "cache_refresh", "ui_render", "snapshot_emit"}[i%4]
}

func syntheticQueryRealworldComponent(i int) string {
	if i%5 != 0 {
		return querySyntheticComponentDenseTarget
	}
	return [...]string{"worker", "ingest", "scheduler"}[i%3]
}

func syntheticQueryRealworldHash(i int) string {
	if i%97 == 0 {
		return querySyntheticHashSparseTarget
	}
	return fmt.Sprintf("%08x%08x%08x%08x%08x%08x%08x%08x",
		i*17+3,
		i*19+5,
		i*23+7,
		i*29+11,
		i*31+13,
		i*37+17,
		i*41+19,
		i*43+23,
	)
}

func syntheticQueryRealworldSessionIDs(i int) []string {
	switch {
	case i%73 == 0:
		return []string{
			querySyntheticSessionIDSparseTarget,
			fmt.Sprintf("sid-%04d-extra", i),
		}
	case i%2 == 0:
		return []string{
			fmt.Sprintf("sid-%04d-a", i),
			fmt.Sprintf("sid-%04d-b", i),
		}
	default:
		return nil
	}
}

func syntheticQueryRealworldPayloadValue(i int, nested bool) any {
	if !nested {
		return map[string]any{
			"blob":   syntheticQueryRealworldBlob(i, 96+(i%32)),
			"status": [...]string{"ok", "warm", "cold"}[i%3],
			"frames": []any{
				map[string]any{"id": i % 8, "kind": "header"},
				map[string]any{"id": (i + 3) % 8, "kind": "body"},
			},
			"lookup": map[string]any{
				"active": i%2 == 0,
				"score":  (i * 29) % 1000,
			},
		}
	}
	return []any{
		map[string]any{
			"kind": "segment",
			"meta": map[string]any{
				"label": fmt.Sprintf("seg-%03d", i%128),
				"rank":  i % 9,
			},
			"blob": syntheticQueryRealworldBlob(i, 72+(i%24)),
		},
		map[string]any{
			"kind": "summary",
			"children": []any{
				map[string]any{
					"id":      fmt.Sprintf("child-%03d", i%64),
					"enabled": i%3 != 0,
				},
				map[string]any{
					"id":      fmt.Sprintf("child-%03d", (i+7)%64),
					"enabled": i%4 != 0,
				},
			},
			"notes": []string{
				"synthetic",
				"anonymous",
				"shape-" + strconv.Itoa(i%11),
			},
		},
	}
}

func syntheticQueryRealworldBlob(i, n int) string {
	if n <= 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(n)
	alphabet := "abcdefghijklmnopqrstuvwxyz0123456789"
	for j := 0; j < n; j++ {
		b.WriteByte(alphabet[(i+j*7)%len(alphabet)])
	}
	return b.String()
}
