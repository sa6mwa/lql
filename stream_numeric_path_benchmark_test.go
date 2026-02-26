package lql

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type numericPathBenchmarkDataset struct {
	name           string
	payload        []byte
	queryExpr      string
	mutateExprs    []string
	spoolLikelyHit bool
}

var (
	numericPathBenchOnce sync.Once
	numericPathBenchData []numericPathBenchmarkDataset
	numericPathBenchErr  error
)

func BenchmarkQueryStreamNumericPathSynthetic(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping numeric-path query benchmark in short mode")
	}
	datasets, err := loadNumericPathBenchmarkDatasets()
	if err != nil {
		b.Fatalf("load numeric-path datasets: %v", err)
	}

	for _, ds := range datasets {
		ds := ds
		selector, err := ParseSelectorString(ds.queryExpr)
		if err != nil {
			b.Fatalf("parse selector for %s: %v", ds.name, err)
		}
		plan, err := NewQueryStreamPlan(selector)
		if err != nil {
			b.Fatalf("new query plan for %s: %v", ds.name, err)
		}

		b.Run(ds.name, func(b *testing.B) {
			b.SetBytes(int64(len(ds.payload)))
			strategies := []struct {
				name    string
				usePlan bool
			}{
				{name: "selector", usePlan: false},
				{name: "plan", usePlan: true},
			}

			for _, strategy := range strategies {
				strategy := strategy
				b.Run("decision_only_"+strategy.name, func(b *testing.B) {
					var src bytes.Reader
					reader := bufio.NewReaderSize(&src, 64*1024)
					var seen, matched int
					runBenchmarkModes(b, func() error {
						seen = 0
						matched = 0
						src.Reset(ds.payload)
						reader.Reset(&src)

						req := QueryStreamRequest{
							Reader: reader,
							Mode:   QueryDecisionOnly,
							OnDecision: func(d QueryStreamDecision) error {
								seen++
								if d.Matched {
									matched++
								}
								return nil
							},
						}
						if strategy.usePlan {
							req.Plan = plan
						} else {
							req.Selector = selector
						}
						if err := QueryStream(req); err != nil {
							return err
						}
						if seen == 0 || matched == 0 {
							return fmt.Errorf("unexpected empty result seen=%d matched=%d", seen, matched)
						}
						return nil
					})
				})

				b.Run("plus_value_"+strategy.name, func(b *testing.B) {
					var src bytes.Reader
					reader := bufio.NewReaderSize(&src, 64*1024)
					var seen, matched int
					runBenchmarkModes(b, func() error {
						seen = 0
						matched = 0
						src.Reset(ds.payload)
						reader.Reset(&src)

						req := QueryStreamRequest{
							Reader: reader,
							Mode:   QueryDecisionPlusValue,
							OnValue: func(v QueryStreamValue) error {
								seen++
								if v.Matched {
									matched++
								}
								if v.JSON == nil && v.OpenJSON == nil {
									return fmt.Errorf("expected payload")
								}
								return nil
							},
						}
						if strategy.usePlan {
							req.Plan = plan
						} else {
							req.Selector = selector
						}
						if err := QueryStream(req); err != nil {
							return err
						}
						if seen == 0 || matched == 0 {
							return fmt.Errorf("unexpected empty result seen=%d matched=%d", seen, matched)
						}
						return nil
					})
				})
			}

			if ds.spoolLikelyHit {
				b.Run("plus_value_selector_spooled", func(b *testing.B) {
					var src bytes.Reader
					reader := bufio.NewReaderSize(&src, 64*1024)
					runBenchmarkModes(b, func() error {
						src.Reset(ds.payload)
						reader.Reset(&src)
						return QueryStream(QueryStreamRequest{
							Reader:           reader,
							Selector:         selector,
							Mode:             QueryDecisionPlusValue,
							SpoolMemoryBytes: 1024,
							OnValue: func(v QueryStreamValue) error {
								if v.OpenJSON == nil {
									return fmt.Errorf("expected OpenJSON")
								}
								rc, err := v.OpenJSON()
								if err != nil {
									return err
								}
								_, copyErr := io.Copy(io.Discard, rc)
								closeErr := rc.Close()
								if copyErr != nil {
									return copyErr
								}
								return closeErr
							},
						})
					})
				})
			}
		})
	}
}

func BenchmarkMutateStreamNumericPathSynthetic(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping numeric-path mutate benchmark in short mode")
	}
	datasets, err := loadNumericPathBenchmarkDatasets()
	if err != nil {
		b.Fatalf("load numeric-path datasets: %v", err)
	}

	for _, ds := range datasets {
		ds := ds
		b.Run(ds.name, func(b *testing.B) {
			b.SetBytes(int64(len(ds.payload)))
			muts, err := ParseMutations(ds.mutateExprs, time.Unix(1_750_000_000, 0))
			if err != nil {
				b.Fatalf("parse mutations for %s: %v", ds.name, err)
			}
			plan, err := NewMutateStreamPlan(muts)
			if err != nil {
				b.Fatalf("new mutate plan for %s: %v", ds.name, err)
			}

			b.Run("reuse_program/writer_stream", func(b *testing.B) {
				var src bytes.Reader
				reader := bufio.NewReaderSize(&src, 64*1024)
				runBenchmarkModes(b, func() error {
					src.Reset(ds.payload)
					reader.Reset(&src)
					return MutateStream(MutateStreamRequest{
						Reader: reader,
						Writer: io.Discard,
						Plan:   plan,
					})
				})
			})

			b.Run("reuse_program/callback_raw_json", func(b *testing.B) {
				var src bytes.Reader
				reader := bufio.NewReaderSize(&src, 64*1024)
				onValue := func(MutateStreamValue) error { return nil }
				runBenchmarkModes(b, func() error {
					src.Reset(ds.payload)
					reader.Reset(&src)
					return MutateStream(MutateStreamRequest{
						Reader:  reader,
						Plan:    plan,
						OnValue: onValue,
					})
				})
			})
		})
	}
}

func loadNumericPathBenchmarkDatasets() ([]numericPathBenchmarkDataset, error) {
	numericPathBenchOnce.Do(func() {
		ndjsonCount := benchmarkCountFromEnv("LQL_BENCH_NUMERIC_NDJSON_COUNT", 12000)
		arrayCount := benchmarkCountFromEnv("LQL_BENCH_NUMERIC_ARRAY_COUNT", 12000)
		singleCount := benchmarkCountFromEnv("LQL_BENCH_NUMERIC_SINGLE_COUNT", 6000)

		ndjson, err := buildNumericNDJSONPayload(ndjsonCount)
		if err != nil {
			numericPathBenchErr = err
			return
		}
		arrayPayload, err := buildNumericArrayPayload(arrayCount)
		if err != nil {
			numericPathBenchErr = err
			return
		}
		singlePayload, err := buildNumericSingleRootPayload(singleCount)
		if err != nil {
			numericPathBenchErr = err
			return
		}

		numericPathBenchData = []numericPathBenchmarkDataset{
			{
				name:        "large_ndjson_numeric",
				payload:     ndjson,
				queryExpr:   `/voucher/lines/10/amount>=3000,/voucher/.../10/code="AUTH-10"`,
				mutateExprs: []string{`/voucher/lines/10/amount=+2`, `/voucher/lines/10/status=patched`, `/voucher/.../10/code=AUTH-10-PATCHED`},
			},
			{
				name:        "large_array_numeric",
				payload:     arrayPayload,
				queryExpr:   `/voucher/lines/10/amount>=3000,/voucher/.../10/code="AUTH-10"`,
				mutateExprs: []string{`/voucher/lines/10/amount=+2`, `/voucher/lines/10/status=patched`, `/voucher/.../10/code=AUTH-10-PATCHED`},
			},
			{
				name:           "large_single_numeric",
				payload:        singlePayload,
				queryExpr:      `/records[]/voucher/lines/10/amount>=3000,/records[]/voucher/.../10/code="AUTH-10"`,
				mutateExprs:    []string{`/records[]/voucher/lines/10/amount=+2`, `/records[]/voucher/lines/10/status=patched`, `/records[]/voucher/.../10/code=AUTH-10-PATCHED`},
				spoolLikelyHit: true,
			},
		}
	})
	if numericPathBenchErr != nil {
		return nil, numericPathBenchErr
	}
	return numericPathBenchData, nil
}

func benchmarkCountFromEnv(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return fallback
	}
	return n
}

func buildNumericNDJSONPayload(count int) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := 0; i < count; i++ {
		if err := enc.Encode(syntheticNumericPathRecord(i)); err != nil {
			return nil, fmt.Errorf("encode numeric ndjson record %d: %w", i, err)
		}
	}
	return buf.Bytes(), nil
}

func buildNumericArrayPayload(count int) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i := 0; i < count; i++ {
		if i > 0 {
			buf.WriteByte(',')
		}
		payload, err := json.Marshal(syntheticNumericPathRecord(i))
		if err != nil {
			return nil, fmt.Errorf("marshal numeric array record %d: %w", i, err)
		}
		if _, err := buf.Write(payload); err != nil {
			return nil, fmt.Errorf("write numeric array record %d: %w", i, err)
		}
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

func buildNumericSingleRootPayload(count int) ([]byte, error) {
	records := make([]map[string]any, 0, count)
	for i := 0; i < count; i++ {
		records = append(records, syntheticNumericPathRecord(i))
	}
	payload, err := json.Marshal(map[string]any{
		"id":      "numeric-root",
		"version": 1,
		"records": records,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal numeric single root: %w", err)
	}
	return payload, nil
}

func syntheticNumericPathRecord(i int) map[string]any {
	makeLinesArray := func(value map[string]any) []any {
		lines := make([]any, 12)
		for idx := range lines {
			lines[idx] = map[string]any{"amount": idx, "status": "noop"}
		}
		lines[10] = value
		return lines
	}
	value := map[string]any{
		"amount": 2400 + ((i % 5) * 400),
		"status": []string{"open", "closed", "queued"}[i%3],
		"code":   "AUTH-10",
		"msg":    "hello numeric path",
	}

	var lines any = map[string]any{"10": value}
	if i%2 == 1 {
		lines = makeLinesArray(value)
	}
	return map[string]any{
		"id":      fmt.Sprintf("numeric-%07d", i),
		"region":  []string{"eu", "us", "apac"}[i%3],
		"service": "lockd",
		"voucher": map[string]any{
			"lines": lines,
			"groups": []any{
				map[string]any{
					"lines": map[string]any{
						"10": map[string]any{
							"code": "AUTH-10",
						},
					},
				},
			},
		},
	}
}
