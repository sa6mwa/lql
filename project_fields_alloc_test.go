package lql

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

func TestProjectFieldsHotPathAllocsStableWithLargeUnselectedBlob(t *testing.T) {
	paths, err := ParseProjectionPaths([]string{"/id"})
	if err != nil {
		t.Fatalf("ParseProjectionPaths: %v", err)
	}
	plan, err := NewProjectionPlan(paths)
	if err != nil {
		t.Fatalf("NewProjectionPlan: %v", err)
	}

	small := []byte(`{"id":"a","blob":"` + strings.Repeat("x", 32) + `"}`)
	large := []byte(`{"id":"a","blob":"` + strings.Repeat("x", 9800) + `"}`)
	reader := bytes.NewReader(nil)
	var out bytes.Buffer

	run := func(payload []byte) (float64, error) {
		var runErr error
		allocs := testing.AllocsPerRun(200, func() {
			reader.Reset(payload)
			out.Reset()
			_, runErr = ProjectFields(ProjectFieldsRequest{
				Reader: reader,
				Writer: &out,
				Plan:   plan,
			})
		})
		return allocs, runErr
	}

	smallAllocs, err := run(small)
	if err != nil {
		t.Fatalf("project small: %v", err)
	}
	largeAllocs, err := run(large)
	if err != nil {
		t.Fatalf("project large: %v", err)
	}
	if largeAllocs > smallAllocs+1 {
		t.Fatalf("expected near-constant allocs for large unselected values, got small=%.2f large=%.2f", smallAllocs, largeAllocs)
	}
}

func TestProjectFieldsAllocBudgetPerCandidate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping alloc budget test in short mode")
	}
	paths, err := ParseProjectionPaths([]string{"/id"})
	if err != nil {
		t.Fatalf("ParseProjectionPaths: %v", err)
	}
	plan, err := NewProjectionPlan(paths)
	if err != nil {
		t.Fatalf("NewProjectionPlan: %v", err)
	}
	candidateCount := 512
	candidates := make([][]byte, candidateCount)
	maxCandidate := 0
	for i := 0; i < candidateCount; i++ {
		candidate := []byte(`{"id":"` + strconv.Itoa(i) + `","blob":"` + strings.Repeat("x", 9800) + `"}`)
		candidates[i] = candidate
		if len(candidate) > maxCandidate {
			maxCandidate = len(candidate)
		}
	}
	if maxCandidate > 10*1024 {
		t.Fatalf("fixture invalid: max candidate %d > 10KiB", maxCandidate)
	}

	var out bytes.Buffer
	reader := bytes.NewReader(nil)
	result := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, candidate := range candidates {
				reader.Reset(candidate)
				out.Reset()
				_, err := ProjectFields(ProjectFieldsRequest{
					Reader: reader,
					Writer: &out,
					Plan:   plan,
				})
				if err != nil {
					b.Fatalf("ProjectFields: %v", err)
				}
			}
		}
	})
	bytesPerCandidate := result.AllocedBytesPerOp() / int64(candidateCount)
	const budgetBytes = 4 * 1024
	if bytesPerCandidate > budgetBytes {
		t.Fatalf("projection alloc budget exceeded: bytes/candidate=%d budget=%d bytes/op=%d allocs/op=%d",
			bytesPerCandidate,
			budgetBytes,
			result.AllocedBytesPerOp(),
			result.AllocsPerOp(),
		)
	}
}
