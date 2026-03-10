package lql

import (
	"bufio"
	"bytes"
	"io"
	"testing"
	"time"
)

func BenchmarkMutateStreamRealworld(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping realworld mutate benchmark in short mode")
	}
	datasets, err := loadQueryRealworldDatasets()
	if err != nil {
		b.Skipf("skip realworld benchmark: %v", err)
	}

	cases := []struct {
		name      string
		mutations []string
	}{
		{name: "set_top_level_dense", mutations: []string{`/component=lql`}},
		{name: "set_top_level_sparse", mutations: []string{`/event=session_sync`}},
		{name: "set_nested_sparse", mutations: []string{`/query/hash=ff`}},
		{name: "increment_sparse", mutations: []string{`/code=+1`}},
		{name: "remove_sparse", mutations: []string{`rm:/payload`}},
		{name: "create_nested_dense", mutations: []string{`/meta/bench=true`}},
		{
			name:      "multi_mutation",
			mutations: []string{`/component=lql`, `/event=session_sync`, `/code=+1`, `rm:/payload`, `/meta/bench=true`},
		},
	}

	for _, ds := range datasets {
		ds := ds
		b.Run(ds.name, func(b *testing.B) {
			b.SetBytes(int64(len(ds.payload)))
			for _, tc := range cases {
				tc := tc
				b.Run(tc.name, func(b *testing.B) {
					muts, err := ParseMutations(tc.mutations, time.Unix(1_750_000_000, 0))
					if err != nil {
						b.Fatalf("parse mutations %s: %v", tc.name, err)
					}
					plan, err := NewMutateStreamPlan(muts)
					if err != nil {
						b.Fatalf("new mutate plan %s: %v", tc.name, err)
					}

					b.Run("reuse_program", func(b *testing.B) {
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
				})
			}
		})
	}
}
