package lql

import (
	"bufio"
	"bytes"
	"io"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkMutateStreamFileBacked(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping file-backed mutate benchmark in short mode")
	}

	input := []byte(`{"payload":"old"}`)
	cases := []struct {
		name    string
		expr    string
		payload []byte
	}{
		{name: "text", expr: `textfile:/payload=blob.txt`, payload: bytes.Repeat([]byte("hello world\n"), 512)},
		{name: "base64", expr: `base64file:/payload=blob.bin`, payload: bytes.Repeat([]byte{0x00, 0x01, 0x02, 0x03}, 2048)},
		{name: "auto_text", expr: `file:/payload=blob.txt`, payload: bytes.Repeat([]byte("hello world\n"), 512)},
		{name: "auto_binary", expr: `file:/payload=blob.bin`, payload: bytes.Repeat([]byte{0x00, 0xff, 0x01, 0x02}, 2048)},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			path := filepath.Join("/virtual", "blob.txt")
			if tc.name == "base64" || tc.name == "auto_binary" {
				path = filepath.Join("/virtual", "blob.bin")
			}
			resolver := &nonAllocMutateFileResolver{
				payloads: map[string][]byte{
					path: tc.payload,
				},
			}
			muts, err := ParseMutationsWithOptions([]string{tc.expr}, time.Unix(1_700_000_000, 0), ParseMutationsOptions{
				EnableFileValues:  true,
				FileValueBaseDir:  "/virtual",
				FileValueResolver: resolver,
			})
			if err != nil {
				b.Fatalf("parse file-backed mutations: %v", err)
			}
			plan, err := NewMutateStreamPlan(muts)
			if err != nil {
				b.Fatalf("new mutate plan: %v", err)
			}

			var src bytes.Reader
			reader := bufio.NewReaderSize(&src, 64*1024)
			b.SetBytes(int64(len(tc.payload)))
			runBenchmarkModes(b, func() error {
				src.Reset(input)
				reader.Reset(&src)
				return MutateStream(MutateStreamRequest{
					Reader: reader,
					Writer: io.Discard,
					Plan:   plan,
				})
			})
		})
	}
}
