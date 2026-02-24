package lql

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkMutateStreamCaptureSpool(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping mutate stream capture benchmark in short mode")
	}
	const candidateCount = 512
	var src bytes.Buffer
	for i := 0; i < candidateCount; i++ {
		src.WriteString(`{"id":"`)
		src.WriteString(strconv.Itoa(i))
		src.WriteString(`","blob":"`)
		src.WriteString(strings.Repeat("x", 8192))
		src.WriteString(`"}`)
	}
	payload := src.Bytes()

	cases := []struct {
		name  string
		spool int64
	}{
		{name: "in_memory", spool: 64 * 1024},
		{name: "forced_spill", spool: 8},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			reader := bytes.NewReader(nil)
			runBenchmarkModes(b, func() error {
				reader.Reset(payload)
				var captured int64
				result, err := MutateStreamWithResult(MutateStreamRequest{
					Reader:           reader,
					SpoolMemoryBytes: tc.spool,
					OnValue: func(v MutateStreamValue) error {
						captured += v.Size
						return nil
					},
				})
				if err != nil {
					return err
				}
				b.ReportMetric(float64(result.BytesCaptured), "captured-bytes/op")
				b.ReportMetric(float64(result.SpillBytes), "spill-bytes/op")
				b.ReportMetric(float64(result.SpillCount), "spills/op")
				b.ReportMetric(float64(captured), "callback-bytes/op")
				return nil
			})
		})
	}
}
