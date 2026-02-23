package lql

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkProjectFieldsSynthetic(b *testing.B) {
	datasets := []struct {
		name   string
		input  []byte
		fields []string
		spool  int64
	}{
		{
			name:  "small_select_id",
			input: []byte(`{"id":"a","status":"new","blob":"` + strings.Repeat("x", 256) + `"}`),
			fields: []string{
				"/id",
			},
			spool: 5 * 1024 * 1024,
		},
		{
			name:  "large_unselected_blob",
			input: []byte(`{"id":"a","status":"new","blob":"` + strings.Repeat("x", 9800) + `"}`),
			fields: []string{
				"/id",
				"/status",
			},
			spool: 5 * 1024 * 1024,
		},
		{
			name:  "selected_large_payload_spooled",
			input: []byte(`{"id":"a","payload":"` + strings.Repeat("y", 9800) + `"}`),
			fields: []string{
				"/payload",
			},
			spool: 8,
		},
	}

	for _, dataset := range datasets {
		dataset := dataset
		b.Run(dataset.name, func(b *testing.B) {
			paths, err := ParseProjectionPaths(dataset.fields)
			if err != nil {
				b.Fatalf("ParseProjectionPaths: %v", err)
			}
			plan, err := NewProjectionPlan(paths)
			if err != nil {
				b.Fatalf("NewProjectionPlan: %v", err)
			}
			var out bytes.Buffer
			reader := bytes.NewReader(nil)
			runBenchmarkModes(b, func() error {
				reader.Reset(dataset.input)
				out.Reset()
				_, err := ProjectFields(ProjectFieldsRequest{
					Reader:           reader,
					Writer:           &out,
					Plan:             plan,
					SpoolMemoryBytes: dataset.spool,
				})
				if err != nil {
					return err
				}
				return nil
			})
		})
	}
}

func BenchmarkProjectFieldsBatchSynthetic(b *testing.B) {
	paths, err := ParseProjectionPaths([]string{"/id"})
	if err != nil {
		b.Fatalf("ParseProjectionPaths: %v", err)
	}
	plan, err := NewProjectionPlan(paths)
	if err != nil {
		b.Fatalf("NewProjectionPlan: %v", err)
	}
	candidates := make([][]byte, 1024)
	for i := range candidates {
		candidates[i] = []byte(`{"id":"` + strconv.Itoa(i) + `","blob":"` + strings.Repeat("x", 9800) + `"}`)
	}
	var out bytes.Buffer
	reader := bytes.NewReader(nil)

	runBenchmarkModes(b, func() error {
		for _, candidate := range candidates {
			reader.Reset(candidate)
			out.Reset()
			_, err := ProjectFields(ProjectFieldsRequest{
				Reader: reader,
				Writer: &out,
				Plan:   plan,
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}
