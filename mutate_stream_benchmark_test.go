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

type mutateBenchmarkDataset struct {
	name      string
	payload   []byte
	mutations []string
}

var (
	mutateBenchOnce sync.Once
	mutateBenchData []mutateBenchmarkDataset
	mutateBenchErr  error
)

func BenchmarkMutateStreamSynthetic(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping synthetic mutate benchmark in short mode")
	}

	datasets, err := loadMutateBenchmarkDatasets()
	if err != nil {
		b.Fatalf("load mutate benchmark datasets: %v", err)
	}

	for _, ds := range datasets {
		ds := ds
		b.Run(ds.name, func(b *testing.B) {
			b.SetBytes(int64(len(ds.payload)))
			b.Run("reuse_program", func(b *testing.B) {
				muts, err := ParseMutations(ds.mutations, time.Unix(1_750_000_000, 0))
				if err != nil {
					b.Fatalf("parse mutations: %v", err)
				}
				plan, err := NewMutateStreamPlan(muts)
				if err != nil {
					b.Fatalf("compile mutate plan: %v", err)
				}
				b.Run("writer_stream", func(b *testing.B) {
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
				b.Run("callback_raw_json", func(b *testing.B) {
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

			b.Run("reparse_program_each_run", func(b *testing.B) {
				var src bytes.Reader
				reader := bufio.NewReaderSize(&src, 64*1024)
				runBenchmarkModes(b, func() error {
					muts, err := ParseMutations(ds.mutations, time.Unix(1_750_000_000, 0))
					if err != nil {
						return err
					}
					plan, err := NewMutateStreamPlan(muts)
					if err != nil {
						return err
					}
					src.Reset(ds.payload)
					reader.Reset(&src)
					return MutateStream(MutateStreamRequest{
						Reader: reader,
						Writer: io.Discard,
						Plan:   plan,
					})
				})
			})
		})
	}
}

func loadMutateBenchmarkDatasets() ([]mutateBenchmarkDataset, error) {
	mutateBenchOnce.Do(func() {
		ndjsonCount := mutateBenchmarkCountFromEnv("LQL_BENCH_MUTATE_NDJSON_COUNT", 20000)
		arrayCount := mutateBenchmarkCountFromEnv("LQL_BENCH_MUTATE_ARRAY_COUNT", 20000)
		singleCount := mutateBenchmarkCountFromEnv("LQL_BENCH_MUTATE_SINGLE_COUNT", 12000)

		ndjson, err := buildMutateNDJSONPayload(ndjsonCount)
		if err != nil {
			mutateBenchErr = err
			return
		}
		arrayJSON, err := buildMutateArrayPayload(arrayCount)
		if err != nil {
			mutateBenchErr = err
			return
		}
		singleJSON, err := buildMutateSingleObjectPayload(singleCount)
		if err != nil {
			mutateBenchErr = err
			return
		}

		mutateBenchData = []mutateBenchmarkDataset{
			{
				name:      "large_ndjson",
				payload:   ndjson,
				mutations: []string{`/status=ready`, `/metrics/retries=+2`, `/labels/*=tagged`, `rm:/meta/legacy`},
			},
			{
				name:      "large_array",
				payload:   arrayJSON,
				mutations: []string{`/status=ready`, `/metrics/retries=+2`, `/labels/*=tagged`, `rm:/meta/legacy`},
			},
			{
				name:      "large_single_json",
				payload:   singleJSON,
				mutations: []string{`/records[]/status=ready`, `/records[]/metrics/retries=+2`},
			},
		}
	})

	if mutateBenchErr != nil {
		return nil, mutateBenchErr
	}
	return mutateBenchData, nil
}

func mutateBenchmarkCountFromEnv(name string, fallback int) int {
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

func buildMutateNDJSONPayload(count int) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := 0; i < count; i++ {
		if err := enc.Encode(syntheticMutateRecord(i)); err != nil {
			return nil, fmt.Errorf("encode ndjson record %d: %w", i, err)
		}
	}
	return buf.Bytes(), nil
}

func buildMutateArrayPayload(count int) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i := 0; i < count; i++ {
		if i > 0 {
			buf.WriteByte(',')
		}
		item, err := json.Marshal(syntheticMutateRecord(i))
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

func buildMutateSingleObjectPayload(count int) ([]byte, error) {
	type root struct {
		ID      string           `json:"id"`
		Version int              `json:"version"`
		Records []map[string]any `json:"records"`
	}
	records := make([]map[string]any, 0, count)
	for i := 0; i < count; i++ {
		records = append(records, syntheticMutateRecord(i))
	}
	payload, err := json.Marshal(root{
		ID:      "synthetic-mutate-root",
		Version: 1,
		Records: records,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal single root: %w", err)
	}
	return payload, nil
}

func syntheticMutateRecord(i int) map[string]any {
	statuses := []string{"new", "open", "closed", "queued"}
	owners := []string{"alice", "bob", "carol", "dave"}
	return map[string]any{
		"id":     fmt.Sprintf("id-%07d", i),
		"status": statuses[i%len(statuses)],
		"metrics": map[string]any{
			"retries": i % 9,
			"qps":     (i % 1000) + 1,
		},
		"meta": map[string]any{
			"legacy": fmt.Sprintf("legacy-%d", i),
		},
		"labels": map[string]any{
			"owner": owners[i%len(owners)],
			"env":   "prod",
		},
	}
}
