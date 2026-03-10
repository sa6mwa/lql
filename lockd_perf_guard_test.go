package lql

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"
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

func TestLockdPerfGuardMutateStreamFileBackedTextPlan(t *testing.T) {
	lockdPerfGuardMutateStreamFileBackedPlan(t,
		"text",
		`textfile:/payload=blob.txt`,
		filepath.Join("/virtual", "blob.txt"),
		bytes.Repeat([]byte("hello world\n"), 512),
		60.0,
	)
}

func TestLockdPerfGuardMutateStreamFileBackedBase64Plan(t *testing.T) {
	lockdPerfGuardMutateStreamFileBackedPlan(t,
		"base64",
		`base64file:/payload=blob.bin`,
		filepath.Join("/virtual", "blob.bin"),
		bytes.Repeat([]byte{0x00, 0x01, 0x02, 0x03}, 2048),
		45.0,
	)
}

func lockdPerfGuardMutateStreamFileBackedPlan(t *testing.T, name string, expr string, path string, filePayload []byte, minThroughputDefault float64) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping perf guard in short mode")
	}
	resolver := &nonAllocMutateFileResolver{
		payloads: map[string][]byte{
			path: filePayload,
		},
	}
	muts, err := ParseMutationsWithOptions([]string{expr}, time.Unix(1_700_000_000, 0), ParseMutationsOptions{
		EnableFileValues:  true,
		FileValueBaseDir:  "/virtual",
		FileValueResolver: resolver,
	})
	if err != nil {
		t.Fatalf("parse file-backed mutations: %v", err)
	}
	plan, err := NewMutateStreamPlan(muts)
	if err != nil {
		t.Fatalf("mutate plan: %v", err)
	}
	input := []byte(`{"payload":"old"}`)

	var src bytes.Reader
	reader := bufio.NewReaderSize(&src, 64*1024)
	runAlloc := func() error {
		src.Reset(input)
		reader.Reset(&src)
		_, runErr := MutateStreamWithResult(MutateStreamRequest{
			Reader: reader,
			Writer: io.Discard,
			Plan:   plan,
		})
		return runErr
	}
	if err := runAlloc(); err != nil {
		t.Fatalf("file-backed mutate warmup: %v", err)
	}
	allocs := testing.AllocsPerRun(200, func() {
		if runErr := runAlloc(); runErr != nil {
			t.Fatalf("file-backed mutate run: %v", runErr)
		}
	})
	maxAllocs := lockdPerfGuardFloatEnv("LQL_PERF_GUARD_MUTATE_FILE_"+strings.ToUpper(name)+"_MAX_ALLOCS", 1.0)
	if allocs > maxAllocs {
		t.Fatalf("%s file-backed mutate alloc guard failed: allocs/run=%.2f max=%.2f", name, allocs, maxAllocs)
	}

	runThroughput := func() error {
		src.Reset(input)
		reader.Reset(&src)
		_, runErr := MutateStreamWithResult(MutateStreamRequest{
			Reader: reader,
			Writer: io.Discard,
			Plan:   plan,
		})
		return runErr
	}
	loops := 200
	start := time.Now()
	for i := 0; i < loops; i++ {
		if err := runThroughput(); err != nil {
			t.Fatalf("file-backed mutate throughput run %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	bytesPerRun := len(filePayload) + len(input)
	throughput := (float64(bytesPerRun*loops) / (1024 * 1024)) / elapsed.Seconds()
	minThroughput := lockdPerfGuardFloatEnv("LQL_PERF_GUARD_MUTATE_FILE_"+strings.ToUpper(name)+"_MIN_MBPS", minThroughputDefault)
	if throughput < minThroughput {
		t.Fatalf("%s file-backed mutate throughput guard failed: %.2f MiB/s < %.2f MiB/s", name, throughput, minThroughput)
	}
}

func TestLockdPerfGuardContainsAnyBeatsExplicitOr(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping perf guard in short mode")
	}
	payload := lockdPerfSyntheticContainsData(4096, 640)

	anySelector, err := ParseSelectorString(`icontains{field=/msg,any=alpha|beta|gamma}`)
	if err != nil {
		t.Fatalf("parse contains.any selector: %v", err)
	}
	orSelector, err := ParseSelectorString(`or.icontains{field=/msg,value=alpha},or.icontains{field=/msg,value=beta},or.icontains{field=/msg,value=gamma}`)
	if err != nil {
		t.Fatalf("parse explicit-or selector: %v", err)
	}

	anyPlan, err := NewQueryStreamPlan(anySelector)
	if err != nil {
		t.Fatalf("query plan contains.any: %v", err)
	}
	orPlan, err := NewQueryStreamPlan(orSelector)
	if err != nil {
		t.Fatalf("query plan explicit-or: %v", err)
	}
	runner := newLockdPerfQueryPlanRunner()

	if err := lockdPerfRunQueryPlan(runner, payload, anyPlan); err != nil {
		t.Fatalf("contains.any warmup: %v", err)
	}
	if err := lockdPerfRunQueryPlan(runner, payload, orPlan); err != nil {
		t.Fatalf("explicit-or warmup: %v", err)
	}

	allocRuns := 80
	anyAllocs := testing.AllocsPerRun(allocRuns, func() {
		if runErr := lockdPerfRunQueryPlan(runner, payload, anyPlan); runErr != nil {
			t.Fatalf("contains.any alloc run: %v", runErr)
		}
	})
	orAllocs := testing.AllocsPerRun(allocRuns, func() {
		if runErr := lockdPerfRunQueryPlan(runner, payload, orPlan); runErr != nil {
			t.Fatalf("explicit-or alloc run: %v", runErr)
		}
	})
	maxAllocs := lockdPerfGuardFloatEnv("LQL_PERF_GUARD_CONTAINS_ANY_MAX_ALLOCS", 0.10)
	if anyAllocs > maxAllocs {
		t.Fatalf("contains.any alloc guard failed: any=%.2f max=%.2f", anyAllocs, maxAllocs)
	}
	if orAllocs > maxAllocs {
		t.Fatalf("explicit-or alloc guard failed: explicit_or=%.2f max=%.2f", orAllocs, maxAllocs)
	}
	maxAllocDelta := lockdPerfGuardFloatEnv("LQL_PERF_GUARD_CONTAINS_ANY_MAX_ALLOC_DELTA", 0.25)
	if anyAllocs > orAllocs+maxAllocDelta {
		t.Fatalf("contains.any alloc guard failed: any=%.2f explicit_or=%.2f max_delta=%.2f", anyAllocs, orAllocs, maxAllocDelta)
	}

	loops := int(lockdPerfGuardFloatEnv("LQL_PERF_GUARD_CONTAINS_ANY_THROUGHPUT_LOOPS", 5))
	if loops < 1 {
		loops = 1
	}
	anyThroughput, err := lockdPerfMeasureQueryThroughput(runner, payload, anyPlan, loops)
	if err != nil {
		t.Fatalf("contains.any throughput run: %v", err)
	}
	orThroughput, err := lockdPerfMeasureQueryThroughput(runner, payload, orPlan, loops)
	if err != nil {
		t.Fatalf("explicit-or throughput run: %v", err)
	}

	minSpeedup := lockdPerfGuardFloatEnv("LQL_PERF_GUARD_CONTAINS_ANY_MIN_SPEEDUP", 1.10)
	required := orThroughput * minSpeedup
	if anyThroughput < required {
		t.Fatalf("contains.any throughput guard failed: any=%.2f MiB/s explicit_or=%.2f MiB/s min_speedup=%.2fx",
			anyThroughput, orThroughput, minSpeedup)
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

func lockdPerfSyntheticContainsData(count, msgLen int) []byte {
	var b bytes.Buffer
	msg := strings.Repeat("x", msgLen)
	for i := 0; i < count; i++ {
		b.WriteString(`{"msg":"`)
		b.WriteString(msg)
		b.WriteString(`","idx":`)
		b.WriteString(strconv.Itoa(i))
		b.WriteString(`}`)
		b.WriteByte('\n')
	}
	return b.Bytes()
}

type lockdPerfQueryPlanRunner struct {
	src    bytes.Reader
	reader *bufio.Reader
}

func newLockdPerfQueryPlanRunner() *lockdPerfQueryPlanRunner {
	runner := &lockdPerfQueryPlanRunner{}
	runner.reader = bufio.NewReaderSize(&runner.src, 64*1024)
	return runner
}

func (r *lockdPerfQueryPlanRunner) run(payload []byte, plan QueryStreamPlan) error {
	r.src.Reset(payload)
	r.reader.Reset(&r.src)
	_, err := QueryStreamWithResult(QueryStreamRequest{
		Reader: r.reader,
		Plan:   plan,
		Mode:   QueryDecisionOnly,
		OnDecision: func(QueryStreamDecision) error {
			return nil
		},
	})
	return err
}

func lockdPerfRunQueryPlan(runner *lockdPerfQueryPlanRunner, payload []byte, plan QueryStreamPlan) error {
	return runner.run(payload, plan)
}

func lockdPerfMeasureQueryThroughput(runner *lockdPerfQueryPlanRunner, payload []byte, plan QueryStreamPlan, loops int) (float64, error) {
	start := time.Now()
	for i := 0; i < loops; i++ {
		if err := lockdPerfRunQueryPlan(runner, payload, plan); err != nil {
			return 0, err
		}
	}
	elapsed := time.Since(start)
	throughput := (float64(len(payload)*loops) / (1024 * 1024)) / elapsed.Seconds()
	return throughput, nil
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
