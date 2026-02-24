package lql

import (
	"bufio"
	"bytes"
	"io"
	"runtime"
	"testing"
	"time"
)

type heapSample struct {
	heapAlloc   uint64
	heapObjects uint64
}

type heapPlateauConfig struct {
	warmup             int
	checkpoints        int
	batchRuns          int
	maxAllocSpread     uint64
	maxObjectSpread    uint64
	maxAllocTailDelta  uint64
	maxObjectTailDelta uint64
}

func TestQueryStreamHeapPlateauDecisionOnlyPlan(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heap plateau test in short mode")
	}
	payload, err := buildQueryNDJSONPayload(1024, 96)
	if err != nil {
		t.Fatalf("build query payload: %v", err)
	}
	selector, err := ParseSelectorString(`/status="open"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	plan, err := NewQueryStreamPlan(selector)
	if err != nil {
		t.Fatalf("new query plan: %v", err)
	}
	var src bytes.Reader
	reader := bufio.NewReaderSize(&src, 64*1024)
	onValue := func(QueryStreamValue) error { return nil }

	assertHeapPlateau(t, heapPlateauConfig{
		warmup:             4,
		checkpoints:        8,
		batchRuns:          20,
		maxAllocSpread:     512 * 1024,
		maxObjectSpread:    6000,
		maxAllocTailDelta:  256 * 1024,
		maxObjectTailDelta: 2500,
	}, func() error {
		src.Reset(payload)
		reader.Reset(&src)
		return QueryStream(QueryStreamRequest{
			Reader:  reader,
			Plan:    plan,
			Mode:    QueryDecisionOnly,
			OnValue: onValue,
		})
	})
}

func TestQueryStreamHeapPlateauDecisionOnlyPlanWithStopControls(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heap plateau test in short mode")
	}
	payload, err := buildQueryNDJSONPayload(2048, 96)
	if err != nil {
		t.Fatalf("build query payload: %v", err)
	}
	selector, err := ParseSelectorString(`/status="open"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	plan, err := NewQueryStreamPlan(selector)
	if err != nil {
		t.Fatalf("new query plan: %v", err)
	}
	var src bytes.Reader
	reader := bufio.NewReaderSize(&src, 64*1024)

	assertHeapPlateau(t, heapPlateauConfig{
		warmup:             4,
		checkpoints:        8,
		batchRuns:          20,
		maxAllocSpread:     512 * 1024,
		maxObjectSpread:    6000,
		maxAllocTailDelta:  256 * 1024,
		maxObjectTailDelta: 2500,
	}, func() error {
		src.Reset(payload)
		reader.Reset(&src)
		_, err := QueryStreamWithResult(QueryStreamRequest{
			Reader:        reader,
			Plan:          plan,
			Mode:          QueryDecisionOnly,
			MaxCandidates: 256,
			OnDecision:    func(QueryStreamDecision) error { return nil },
		})
		return err
	})
}

func TestQueryStreamHeapPlateauPlusValueSpool(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heap plateau test in short mode")
	}
	payload, err := buildQuerySingleObjectPayload(1600, 256)
	if err != nil {
		t.Fatalf("build query payload: %v", err)
	}
	const spoolMem = int64(32 * 1024)
	if int64(len(payload)) <= spoolMem {
		t.Fatalf("fixture too small: payload=%d spool=%d", len(payload), spoolMem)
	}
	selector, err := ParseSelectorString(`/records[]/status="open"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	plan, err := NewQueryStreamPlan(selector)
	if err != nil {
		t.Fatalf("new query plan: %v", err)
	}
	var src bytes.Reader
	reader := bufio.NewReaderSize(&src, 64*1024)
	copyBuf := make([]byte, 32*1024)
	onValue := func(v QueryStreamValue) error {
		if !v.Matched {
			return nil
		}
		if v.OpenJSON == nil {
			return io.ErrUnexpectedEOF
		}
		rc, err := v.OpenJSON()
		if err != nil {
			return err
		}
		_, copyErr := io.CopyBuffer(io.Discard, rc, copyBuf)
		closeErr := rc.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	}

	assertHeapPlateau(t, heapPlateauConfig{
		warmup:             4,
		checkpoints:        8,
		batchRuns:          8,
		maxAllocSpread:     1024 * 1024,
		maxObjectSpread:    10000,
		maxAllocTailDelta:  512 * 1024,
		maxObjectTailDelta: 4000,
	}, func() error {
		src.Reset(payload)
		reader.Reset(&src)
		return QueryStream(QueryStreamRequest{
			Reader:           reader,
			Plan:             plan,
			Mode:             QueryDecisionPlusValue,
			SpoolMemoryBytes: spoolMem,
			OnValue:          onValue,
		})
	})
}

func TestQueryStreamHeapPlateauPlusValueBestEffortCapture(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heap plateau test in short mode")
	}
	payload := buildLowMatchCapturePayload(192, 8*1024)
	selector, err := ParseSelectorString(`/id="match"`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	var src bytes.Reader
	reader := bufio.NewReaderSize(&src, 64*1024)

	assertHeapPlateau(t, heapPlateauConfig{
		warmup:             4,
		checkpoints:        8,
		batchRuns:          8,
		maxAllocSpread:     1024 * 1024,
		maxObjectSpread:    10000,
		maxAllocTailDelta:  512 * 1024,
		maxObjectTailDelta: 4000,
	}, func() error {
		src.Reset(payload)
		reader.Reset(&src)
		_, err := QueryStreamWithResult(QueryStreamRequest{
			Reader:        reader,
			Selector:      selector,
			Mode:          QueryDecisionPlusValue,
			MatchedOnly:   true,
			CapturePolicy: QueryCaptureMatchesOnlyBestEffort,
			OnValue:       func(QueryStreamValue) error { return nil },
		})
		return err
	})
}

func TestMutateStreamHeapPlateauWriterPlan(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heap plateau test in short mode")
	}
	payload, err := buildMutateNDJSONPayload(1024)
	if err != nil {
		t.Fatalf("build mutate payload: %v", err)
	}
	muts, err := ParseMutations([]string{`/status=ready`, `/metrics/retries=+1`}, time.Unix(1_750_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}
	plan, err := NewMutateStreamPlan(muts)
	if err != nil {
		t.Fatalf("new mutate plan: %v", err)
	}
	var src bytes.Reader
	reader := bufio.NewReaderSize(&src, 64*1024)

	assertHeapPlateau(t, heapPlateauConfig{
		warmup:             4,
		checkpoints:        8,
		batchRuns:          20,
		maxAllocSpread:     512 * 1024,
		maxObjectSpread:    6000,
		maxAllocTailDelta:  256 * 1024,
		maxObjectTailDelta: 2500,
	}, func() error {
		src.Reset(payload)
		reader.Reset(&src)
		return MutateStream(MutateStreamRequest{
			Reader: reader,
			Writer: io.Discard,
			Plan:   plan,
		})
	})
}

func TestMutateStreamHeapPlateauCallbackSpool(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heap plateau test in short mode")
	}
	payload, err := buildMutateSingleObjectPayload(2600)
	if err != nil {
		t.Fatalf("build mutate payload: %v", err)
	}
	const spoolMem = int64(32 * 1024)
	if int64(len(payload)) <= spoolMem {
		t.Fatalf("fixture too small: payload=%d spool=%d", len(payload), spoolMem)
	}
	muts, err := ParseMutations([]string{`/records[]/status=ready`, `/records[]/metrics/retries=+1`}, time.Unix(1_750_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}
	plan, err := NewMutateStreamPlan(muts)
	if err != nil {
		t.Fatalf("new mutate plan: %v", err)
	}
	var src bytes.Reader
	reader := bufio.NewReaderSize(&src, 64*1024)
	copyBuf := make([]byte, 32*1024)
	onValue := func(v MutateStreamValue) error {
		if v.OpenJSON == nil {
			return io.ErrUnexpectedEOF
		}
		rc, err := v.OpenJSON()
		if err != nil {
			return err
		}
		_, copyErr := io.CopyBuffer(io.Discard, rc, copyBuf)
		closeErr := rc.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	}

	assertHeapPlateau(t, heapPlateauConfig{
		warmup:             4,
		checkpoints:        8,
		batchRuns:          8,
		maxAllocSpread:     1024 * 1024,
		maxObjectSpread:    12000,
		maxAllocTailDelta:  512 * 1024,
		maxObjectTailDelta: 5000,
	}, func() error {
		src.Reset(payload)
		reader.Reset(&src)
		return MutateStream(MutateStreamRequest{
			Reader:           reader,
			Plan:             plan,
			OnValue:          onValue,
			SpoolMemoryBytes: spoolMem,
		})
	})
}

func assertHeapPlateau(t *testing.T, cfg heapPlateauConfig, runBatch func() error) {
	t.Helper()
	if cfg.warmup < 1 {
		cfg.warmup = 1
	}
	if cfg.checkpoints < 2 {
		cfg.checkpoints = 2
	}
	if cfg.batchRuns < 1 {
		cfg.batchRuns = 1
	}
	total := cfg.warmup + cfg.checkpoints
	samples := make([]heapSample, 0, total)
	for i := 0; i < total; i++ {
		for run := 0; run < cfg.batchRuns; run++ {
			if err := runBatch(); err != nil {
				t.Fatalf("batch %d run %d: %v", i, run, err)
			}
		}
		forceGC()
		samples = append(samples, readHeapSample())
	}
	steady := samples[cfg.warmup:]
	minAlloc, maxAlloc := steady[0].heapAlloc, steady[0].heapAlloc
	minObjects, maxObjects := steady[0].heapObjects, steady[0].heapObjects
	for _, sample := range steady[1:] {
		if sample.heapAlloc < minAlloc {
			minAlloc = sample.heapAlloc
		}
		if sample.heapAlloc > maxAlloc {
			maxAlloc = sample.heapAlloc
		}
		if sample.heapObjects < minObjects {
			minObjects = sample.heapObjects
		}
		if sample.heapObjects > maxObjects {
			maxObjects = sample.heapObjects
		}
	}
	allocSpread := maxAlloc - minAlloc
	objectSpread := maxObjects - minObjects
	allocTailDelta := absDiffU64(steady[len(steady)-1].heapAlloc, steady[0].heapAlloc)
	objectTailDelta := absDiffU64(steady[len(steady)-1].heapObjects, steady[0].heapObjects)
	if allocSpread > cfg.maxAllocSpread ||
		objectSpread > cfg.maxObjectSpread ||
		allocTailDelta > cfg.maxAllocTailDelta ||
		objectTailDelta > cfg.maxObjectTailDelta {
		t.Fatalf(
			"heap plateau check failed: allocSpread=%d objectSpread=%d allocTailDelta=%d objectTailDelta=%d steadySamples=%v",
			allocSpread,
			objectSpread,
			allocTailDelta,
			objectTailDelta,
			steady,
		)
	}
}

func forceGC() {
	runtime.GC()
	runtime.GC()
}

func readHeapSample() heapSample {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return heapSample{
		heapAlloc:   stats.HeapAlloc,
		heapObjects: stats.HeapObjects,
	}
}

func absDiffU64(a, b uint64) uint64 {
	if a >= b {
		return a - b
	}
	return b - a
}
