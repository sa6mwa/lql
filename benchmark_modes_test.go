package lql

import "testing"

func runBenchmarkModes(b *testing.B, runOnce func() error) {
	b.Helper()
	b.Run("warmup_included", func(b *testing.B) {
		b.Helper()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := runOnce(); err != nil {
				b.Fatalf("benchmark run: %v", err)
			}
		}
	})
	b.Run("steady_state", func(b *testing.B) {
		b.Helper()
		if err := runOnce(); err != nil {
			b.Fatalf("benchmark warmup: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := runOnce(); err != nil {
				b.Fatalf("benchmark run: %v", err)
			}
		}
	})
}
