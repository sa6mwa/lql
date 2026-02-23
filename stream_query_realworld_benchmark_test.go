package lql

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type queryRealworldDataset struct {
	name    string
	payload []byte
}

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
		{name: "eq_sparse", expr: `/event="tabs_update"`},
		{name: "eq_dense", expr: `/component="host"`},
		{name: "eq_none", expr: `/event="__nope__"`},
		{name: "range_sparse", expr: `/code>=11`},
		{name: "nested_eq_sparse", expr: `/query/hash="86dabea3b684cbc7d287ffb741ece4ef859771d4bbd78cb010c23a17adb96728"`},
		{name: "array_eq_sparse", expr: `/session_ids[]="woprWEoK-1"`},
		{name: "recursive_eq_sparse", expr: `/.../event="tabs_update"`},
		{name: "recursive_nested_eq_sparse", expr: `/.../hash="86dabea3b684cbc7d287ffb741ece4ef859771d4bbd78cb010c23a17adb96728"`},
		{name: "multi_clause_and", expr: `/component="host",/event="tabs_update",/active_idx=0,/tab_count=1,exists{/session_ids},/code>=10`},
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
				})
			}
		})
	}
}

func loadQueryRealworldDatasets() ([]queryRealworldDataset, error) {
	queryRealworldOnce.Do(func() {
		paths := []struct {
			name string
			path string
		}{
			{name: "stash_jsonl", path: filepath.Join("stash", "testdata.jsonl")},
			{name: "stash_pretty_stream", path: filepath.Join("stash", "testdatapretty.jsonstream")},
		}
		for _, item := range paths {
			payload, err := os.ReadFile(item.path)
			if err != nil {
				queryRealworldErr = fmt.Errorf("read %s: %w", item.path, err)
				return
			}
			queryRealworldData = append(queryRealworldData, queryRealworldDataset{
				name:    item.name,
				payload: payload,
			})
		}
	})
	if queryRealworldErr != nil {
		return nil, queryRealworldErr
	}
	return queryRealworldData, nil
}
