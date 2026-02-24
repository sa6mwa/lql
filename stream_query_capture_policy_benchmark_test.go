package lql

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func BenchmarkQueryStreamCapturePolicyLowMatch(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping low-match capture policy benchmark in short mode")
	}
	selector, err := ParseSelectorString(`/id="match"`)
	if err != nil {
		b.Fatalf("parse selector: %v", err)
	}
	payload := buildLowMatchCapturePayload(128, 16*1024)

	policies := []struct {
		name   string
		policy QueryStreamCapturePolicy
	}{
		{name: "capture_all_candidates", policy: QueryCaptureAllCandidates},
		{name: "capture_matches_only_best_effort", policy: QueryCaptureMatchesOnlyBestEffort},
	}

	for _, policy := range policies {
		policy := policy
		b.Run(policy.name, func(b *testing.B) {
			run := func() (QueryStreamResult, error) {
				return QueryStreamWithResult(QueryStreamRequest{
					Reader:        bytes.NewReader(payload),
					Selector:      selector,
					Mode:          QueryDecisionPlusValue,
					MatchedOnly:   true,
					CapturePolicy: policy.policy,
					OnValue: func(v QueryStreamValue) error {
						if !v.Matched {
							return fmt.Errorf("expected matched payload in matched-only mode")
						}
						return nil
					},
				})
			}

			b.Run("warmup_included", func(b *testing.B) {
				b.ReportAllocs()
				var captured int64
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := run()
					if err != nil {
						b.Fatalf("benchmark run: %v", err)
					}
					captured += result.BytesCaptured
				}
				b.StopTimer()
				b.ReportMetric(float64(captured)/float64(b.N), "captured-bytes/op")
			})

			b.Run("steady_state", func(b *testing.B) {
				if _, err := run(); err != nil {
					b.Fatalf("benchmark warmup: %v", err)
				}
				b.ReportAllocs()
				var captured int64
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := run()
					if err != nil {
						b.Fatalf("benchmark run: %v", err)
					}
					captured += result.BytesCaptured
				}
				b.StopTimer()
				b.ReportMetric(float64(captured)/float64(b.N), "captured-bytes/op")
			})
		})
	}
}

func BenchmarkQueryStreamCapturePolicyObjectRootPruning(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping object-root capture policy benchmark in short mode")
	}
	selector, err := ParseSelectorString(`not.eq{field=/status,value=closed}`)
	if err != nil {
		b.Fatalf("parse selector: %v", err)
	}
	payload := buildObjectRootLowMatchCapturePayload(128, 16*1024)

	policies := []struct {
		name   string
		policy QueryStreamCapturePolicy
	}{
		{name: "capture_all_candidates", policy: QueryCaptureAllCandidates},
		{name: "capture_matches_only_best_effort", policy: QueryCaptureMatchesOnlyBestEffort},
	}

	for _, policy := range policies {
		policy := policy
		b.Run(policy.name, func(b *testing.B) {
			run := func() (QueryStreamResult, error) {
				return QueryStreamWithResult(QueryStreamRequest{
					Reader:        bytes.NewReader(payload),
					Selector:      selector,
					Mode:          QueryDecisionPlusValue,
					MatchedOnly:   true,
					CapturePolicy: policy.policy,
					OnValue: func(v QueryStreamValue) error {
						if v.Matched {
							return fmt.Errorf("expected no matches in benchmark payload")
						}
						return nil
					},
				})
			}

			b.Run("warmup_included", func(b *testing.B) {
				b.ReportAllocs()
				var captured int64
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := run()
					if err != nil {
						b.Fatalf("benchmark run: %v", err)
					}
					captured += result.BytesCaptured
				}
				b.StopTimer()
				b.ReportMetric(float64(captured)/float64(b.N), "captured-bytes/op")
			})

			b.Run("steady_state", func(b *testing.B) {
				if _, err := run(); err != nil {
					b.Fatalf("benchmark warmup: %v", err)
				}
				b.ReportAllocs()
				var captured int64
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := run()
					if err != nil {
						b.Fatalf("benchmark run: %v", err)
					}
					captured += result.BytesCaptured
				}
				b.StopTimer()
				b.ReportMetric(float64(captured)/float64(b.N), "captured-bytes/op")
			})
		})
	}
}

func buildLowMatchCapturePayload(candidates int, scalarSize int) []byte {
	var builder strings.Builder
	blob := strings.Repeat("x", scalarSize)
	for i := 0; i < candidates; i++ {
		builder.WriteByte('"')
		builder.WriteString(blob)
		builder.WriteByte('"')
	}
	builder.WriteString(`{"id":"match","kind":"object","blob":"`)
	builder.WriteString(blob)
	builder.WriteString(`"}`)
	return []byte(builder.String())
}

func buildObjectRootLowMatchCapturePayload(candidates int, blobSize int) []byte {
	var builder strings.Builder
	blob := strings.Repeat("x", blobSize)
	for i := 0; i < candidates; i++ {
		builder.WriteString(`{"status":"closed","blob":"`)
		builder.WriteString(blob)
		builder.WriteString(`"}`)
	}
	return []byte(builder.String())
}
