package lql

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	allocBudgetCandidateLimitBytes = 10 * 1024
	allocBudgetCandidateCount      = 512
	allocBudgetBlobSize            = 9800
)

func TestMutateStreamAllocBudgetPerCandidate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping alloc budget test in short mode")
	}
	payload, maxCandidate := buildAllocBudgetPayload(t, allocBudgetCandidateCount, allocBudgetBlobSize)
	if maxCandidate > allocBudgetCandidateLimitBytes {
		t.Fatalf("test fixture invalid: max candidate %d exceeds budget limit %d", maxCandidate, allocBudgetCandidateLimitBytes)
	}

	muts, err := ParseMutations([]string{`/status=ready`}, time.Unix(1_750_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}

	var src bytes.Reader
	reader := bufio.NewReaderSize(&src, 64*1024)
	result := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			src.Reset(payload)
			reader.Reset(&src)
			if err := MutateStream(MutateStreamRequest{
				Reader:    reader,
				Writer:    io.Discard,
				Mutations: muts,
			}); err != nil {
				b.Fatalf("mutate stream: %v", err)
			}
		}
	})

	assertAllocBudgetPerCandidate(t, "mutate_stream_writer", result, allocBudgetCandidateCount, allocBudgetCandidateLimitBytes)
}

func TestQueryStreamAllocBudgetPerCandidate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping alloc budget test in short mode")
	}
	payload, maxCandidate := buildAllocBudgetPayload(t, allocBudgetCandidateCount, allocBudgetBlobSize)
	if maxCandidate > allocBudgetCandidateLimitBytes {
		t.Fatalf("test fixture invalid: max candidate %d exceeds budget limit %d", maxCandidate, allocBudgetCandidateLimitBytes)
	}

	var src bytes.Reader
	reader := bufio.NewReaderSize(&src, 64*1024)
	onValue := func(value QueryStreamValue) error {
		if value.OpenJSON == nil {
			return fmt.Errorf("expected OpenJSON")
		}
		return nil
	}
	result := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			src.Reset(payload)
			reader.Reset(&src)
			if err := QueryStream(QueryStreamRequest{
				Reader:           reader,
				Selector:         Selector{},
				IncludeJSON:      true,
				SpoolMemoryBytes: allocBudgetCandidateLimitBytes,
				OnValue:          onValue,
			}); err != nil {
				b.Fatalf("query stream: %v", err)
			}
		}
	})

	assertAllocBudgetPerCandidate(t, "query_stream_plus_value", result, allocBudgetCandidateCount, allocBudgetCandidateLimitBytes)
}

func assertAllocBudgetPerCandidate(t *testing.T, name string, result testing.BenchmarkResult, candidateCount, budgetBytes int) {
	t.Helper()
	if candidateCount <= 0 {
		t.Fatalf("invalid candidate count %d", candidateCount)
	}
	bytesPerCandidate := result.AllocedBytesPerOp() / int64(candidateCount)
	if bytesPerCandidate > int64(budgetBytes) {
		t.Fatalf("%s alloc budget exceeded: bytes/candidate=%d budget=%d (N=%d bytes/op=%d allocs/op=%d)",
			name,
			bytesPerCandidate,
			budgetBytes,
			result.N,
			result.AllocedBytesPerOp(),
			result.AllocsPerOp(),
		)
	}
}

func buildAllocBudgetPayload(t *testing.T, candidateCount, blobSize int) ([]byte, int) {
	t.Helper()
	var buf bytes.Buffer
	blob := strings.Repeat("x", blobSize)
	maxCandidate := 0
	for i := 0; i < candidateCount; i++ {
		line := `{"id":` + strconv.Itoa(i) + `,"status":"new","blob":"` + blob + `"}`
		if len(line) > maxCandidate {
			maxCandidate = len(line)
		}
		buf.WriteString(line)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), maxCandidate
}
