package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"pkt.systems/lql"
)

type benchmarkQuery struct {
	name   string
	args   []string
	orMode bool
}

type benchmarkDataset struct {
	name    string
	payload []byte
	queries []benchmarkQuery
}

var (
	benchmarkDatasetsOnce sync.Once
	benchmarkDatasets     []benchmarkDataset
	benchmarkDatasetErr   error
)

func BenchmarkLQLSelectionBaseline(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large synthetic benchmark in short mode")
	}

	datasets, err := loadBenchmarkDatasets()
	if err != nil {
		b.Fatalf("load benchmark datasets: %v", err)
	}

	for _, ds := range datasets {
		ds := ds
		b.Run(ds.name, func(b *testing.B) {
			b.SetBytes(int64(len(ds.payload)))
			for _, query := range ds.queries {
				query := query
				b.Run(query.name, func(b *testing.B) {
					b.Run("reuse_selector", func(b *testing.B) {
						sel, err := buildSelector(query.args, query.orMode)
						if err != nil {
							b.Fatalf("build selector: %v", err)
						}
						runBenchmarkModes(b, func() error {
							if err := runSelectionQueryOnly(ds.payload, sel); err != nil {
								return err
							}
							return nil
						})
					})

					b.Run("reparse_selector_each_run", func(b *testing.B) {
						runBenchmarkModes(b, func() error {
							sel, err := buildSelector(query.args, query.orMode)
							if err != nil {
								return err
							}
							if err := runSelectionQueryOnly(ds.payload, sel); err != nil {
								return err
							}
							return nil
						})
					})
				})
			}
		})
	}
}

func runSelectionQueryOnly(payload []byte, selector lql.Selector) error {
	return lql.QueryStream(lql.QueryStreamRequest{
		Reader:      bytes.NewReader(payload),
		Selector:    selector,
		IncludeJSON: false,
		OnValue: func(value lql.QueryStreamValue) error {
			_ = value.Matched
			return nil
		},
	})
}

func loadBenchmarkDatasets() ([]benchmarkDataset, error) {
	benchmarkDatasetsOnce.Do(func() {
		ndjsonCount := benchmarkCountFromEnv("LQL_BENCH_NDJSON_COUNT", 30000)
		arrayCount := benchmarkCountFromEnv("LQL_BENCH_ARRAY_COUNT", 30000)
		singleCount := benchmarkCountFromEnv("LQL_BENCH_SINGLE_COUNT", 20000)

		ndjson, err := buildNDJSONPayload(ndjsonCount)
		if err != nil {
			benchmarkDatasetErr = err
			return
		}
		arrayJSON, err := buildArrayPayload(arrayCount)
		if err != nil {
			benchmarkDatasetErr = err
			return
		}
		singleJSON, err := buildSingleObjectPayload(singleCount)
		if err != nil {
			benchmarkDatasetErr = err
			return
		}

		benchmarkDatasets = []benchmarkDataset{
			{
				name:    "large_ndjson",
				payload: ndjson,
				queries: []benchmarkQuery{
					{name: "eq_status_open", args: []string{`/status="open"`}},
					{name: "and_region_latency", args: []string{`and.eq{field=/region,value=us-west}`, `and.range{field=/metrics/latency_ms,lt=350}`}},
					{name: "contains_service", args: []string{`contains{field=/service,value=auth}`}},
				},
			},
			{
				name:    "large_array",
				payload: arrayJSON,
				queries: []benchmarkQuery{
					{name: "eq_status_open", args: []string{`/status="open"`}},
					{name: "and_region_latency", args: []string{`and.eq{field=/region,value=us-west}`, `and.range{field=/metrics/latency_ms,lt=350}`}},
					{name: "contains_service", args: []string{`contains{field=/service,value=auth}`}},
				},
			},
			{
				name:    "large_single_json",
				payload: singleJSON,
				queries: []benchmarkQuery{
					{name: "nested_status_match", args: []string{`/records[]/status="open"`}},
					{name: "nested_and_region_latency", args: []string{`and.eq{field=/records[]/region,value=us-west}`, `and.range{field=/records[]/metrics/latency_ms,lt=350}`}},
					{name: "nested_contains_service", args: []string{`contains{field=/records[]/service,value=auth}`}},
				},
			},
		}
	})

	if benchmarkDatasetErr != nil {
		return nil, benchmarkDatasetErr
	}
	return benchmarkDatasets, nil
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

func buildNDJSONPayload(count int) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := 0; i < count; i++ {
		if err := enc.Encode(syntheticRecord(i)); err != nil {
			return nil, fmt.Errorf("encode ndjson record %d: %w", i, err)
		}
	}
	return buf.Bytes(), nil
}

func buildArrayPayload(count int) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i := 0; i < count; i++ {
		if i > 0 {
			buf.WriteByte(',')
		}
		item, err := json.Marshal(syntheticRecord(i))
		if err != nil {
			return nil, fmt.Errorf("marshal array record %d: %w", i, err)
		}
		if _, err := buf.Write(item); err != nil {
			return nil, fmt.Errorf("write array record %d: %w", i, err)
		}
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

func buildSingleObjectPayload(count int) ([]byte, error) {
	type singleRoot struct {
		ID      string           `json:"id"`
		Region  string           `json:"region"`
		Version int              `json:"version"`
		Records []map[string]any `json:"records"`
	}

	records := make([]map[string]any, 0, count)
	for i := 0; i < count; i++ {
		records = append(records, syntheticRecord(i))
	}
	root := singleRoot{
		ID:      "synthetic-large-object",
		Region:  "us-west",
		Version: 1,
		Records: records,
	}
	payload, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("encode single object payload: %w", err)
	}
	return payload, nil
}

func syntheticRecord(i int) map[string]any {
	regions := []string{"us-west", "us-east", "eu-west", "ap-south"}
	services := []string{"auth-api", "billing", "search", "gateway"}
	statuses := []string{"open", "queued", "closed", "pending"}
	return map[string]any{
		"id":      fmt.Sprintf("id-%07d", i),
		"region":  regions[i%len(regions)],
		"service": services[i%len(services)],
		"status":  statuses[i%len(statuses)],
		"metrics": map[string]any{
			"latency_ms": (i % 900) + 10,
			"qps":        (i % 1200) + 1,
			"errors":     i % 17,
		},
		"message": fmt.Sprintf("request-%d service=%s region=%s", i, services[i%len(services)], regions[i%len(regions)]),
		"tags":    []string{"prod", "blue", "v2"},
	}
}
