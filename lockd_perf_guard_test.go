package lql

import (
	"bytes"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestLockdPerfGuardQueryStreamPlan(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping perf guard in short mode")
	}
	allocPayload := lockdPerfSyntheticQueryData(1, 384)
	throughputPayload := lockdPerfSyntheticQueryData(4096, 384)
	selector, err := ParseSelectorString(`/event="tabs_update"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	plan, err := NewQueryStreamPlan(selector)
	if err != nil {
		t.Fatalf("query plan: %v", err)
	}
	runAlloc := func() error {
		_, runErr := QueryStreamWithResult(QueryStreamRequest{
			Reader: bytes.NewReader(allocPayload),
			Plan:   plan,
			Mode:   QueryDecisionOnly,
			OnDecision: func(QueryStreamDecision) error {
				return nil
			},
		})
		return runErr
	}
	if err := runAlloc(); err != nil {
		t.Fatalf("query warmup: %v", err)
	}

	allocs := testing.AllocsPerRun(200, func() {
		if runErr := runAlloc(); runErr != nil {
			t.Fatalf("query run: %v", runErr)
		}
	})
	maxAllocs := lockdPerfGuardFloatEnv("LQL_PERF_GUARD_QUERY_MAX_ALLOCS", 3)
	if allocs > maxAllocs {
		t.Fatalf("query alloc guard exceeded: allocs/run=%.2f max=%.2f", allocs, maxAllocs)
	}

	runThroughput := func() error {
		_, runErr := QueryStreamWithResult(QueryStreamRequest{
			Reader: bytes.NewReader(throughputPayload),
			Plan:   plan,
			Mode:   QueryDecisionOnly,
			OnDecision: func(QueryStreamDecision) error {
				return nil
			},
		})
		return runErr
	}
	loops := 3
	start := time.Now()
	for i := 0; i < loops; i++ {
		if err := runThroughput(); err != nil {
			t.Fatalf("query throughput run %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	throughput := (float64(len(throughputPayload)*loops) / (1024 * 1024)) / elapsed.Seconds()
	minThroughput := lockdPerfGuardFloatEnv("LQL_PERF_GUARD_QUERY_MIN_MBPS", 15.0)
	if throughput < minThroughput {
		t.Fatalf("query throughput guard failed: %.2f MiB/s < %.2f MiB/s", throughput, minThroughput)
	}
}

func TestLockdPerfGuardMutateStreamPlan(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping perf guard in short mode")
	}
	allocPayload := lockdPerfSyntheticMutateData(1, 256)
	throughputPayload := lockdPerfSyntheticMutateData(4096, 256)
	muts, err := ParseMutations([]string{`/counter=+1`}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}
	plan, err := NewMutateStreamPlan(muts)
	if err != nil {
		t.Fatalf("mutate plan: %v", err)
	}

	runAlloc := func() error {
		_, runErr := MutateStreamWithResult(MutateStreamRequest{
			Reader: bytes.NewReader(allocPayload),
			Writer: io.Discard,
			Plan:   plan,
		})
		return runErr
	}
	if err := runAlloc(); err != nil {
		t.Fatalf("mutate warmup: %v", err)
	}

	allocs := testing.AllocsPerRun(200, func() {
		if runErr := runAlloc(); runErr != nil {
			t.Fatalf("mutate run: %v", runErr)
		}
	})
	maxAllocs := lockdPerfGuardFloatEnv("LQL_PERF_GUARD_MUTATE_MAX_ALLOCS", 5)
	if allocs > maxAllocs {
		t.Fatalf("mutate alloc guard exceeded: allocs/run=%.2f max=%.2f", allocs, maxAllocs)
	}

	runThroughput := func() error {
		_, runErr := MutateStreamWithResult(MutateStreamRequest{
			Reader: bytes.NewReader(throughputPayload),
			Writer: io.Discard,
			Plan:   plan,
		})
		return runErr
	}
	loops := 3
	start := time.Now()
	for i := 0; i < loops; i++ {
		if err := runThroughput(); err != nil {
			t.Fatalf("mutate throughput run %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	throughput := (float64(len(throughputPayload)*loops) / (1024 * 1024)) / elapsed.Seconds()
	minThroughput := lockdPerfGuardFloatEnv("LQL_PERF_GUARD_MUTATE_MIN_MBPS", 12.0)
	if throughput < minThroughput {
		t.Fatalf("mutate throughput guard failed: %.2f MiB/s < %.2f MiB/s", throughput, minThroughput)
	}
}

func lockdPerfSyntheticQueryData(count, blobLen int) []byte {
	var b bytes.Buffer
	events := []string{"tabs_update", "noop", "heartbeat"}
	for i := 0; i < count; i++ {
		event := events[i%len(events)]
		b.WriteString(`{"event":"`)
		b.WriteString(event)
		b.WriteString(`","component":"host","idx":`)
		b.WriteString(strconv.Itoa(i))
		b.WriteString(`,"blob":"`)
		b.WriteString(strings.Repeat("x", blobLen))
		b.WriteString(`"}`)
	}
	return b.Bytes()
}

func lockdPerfSyntheticMutateData(count, blobLen int) []byte {
	var b bytes.Buffer
	for i := 0; i < count; i++ {
		b.WriteString(`{"counter":`)
		b.WriteString(strconv.Itoa(i))
		b.WriteString(`,"blob":"`)
		b.WriteString(strings.Repeat("y", blobLen))
		b.WriteString(`"}`)
	}
	return b.Bytes()
}

func lockdPerfGuardFloatEnv(name string, fallback float64) float64 {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}
