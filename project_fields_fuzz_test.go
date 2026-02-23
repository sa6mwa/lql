package lql

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func FuzzProjectFieldsParity(f *testing.F) {
	f.Add([]byte("alpha"), uint8(0))
	f.Add([]byte("beta"), uint8(7))
	f.Add([]byte{0, 1, 2, 3}, uint8(31))

	fieldSets := [][]string{
		{"/id"},
		{"/meta/trace"},
		{"/items/0/sku"},
		{"/items/1/tags/0"},
		{"/metrics/retries", "/status"},
		{"/meta/nested/value", "/items/2/qty"},
		{"/labels/env", "/labels/owner"},
		{"/missing", "/id"},
	}

	f.Fuzz(func(t *testing.T, seed []byte, fieldMask uint8) {
		if len(seed) > 128 {
			seed = seed[:128]
		}
		doc := fuzzProjectionObject(seed)
		input, err := json.Marshal(doc)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}

		exprs := fieldSets[int(fieldMask)%len(fieldSets)]
		paths, err := ParseProjectionPaths(exprs)
		if err != nil {
			t.Fatalf("ParseProjectionPaths: %v", err)
		}

		var got bytes.Buffer
		gotRes, gotErr := ProjectFields(ProjectFieldsRequest{
			Reader: bytes.NewReader(input),
			Writer: &got,
			Paths:  paths,
		})
		wantPayload, wantFound, wantErr := legacyProjectFieldsForParity(input, paths)
		if (gotErr != nil) != (wantErr != nil) {
			t.Fatalf("error mismatch: gotErr=%v wantErr=%v", gotErr, wantErr)
		}
		if gotErr != nil {
			return
		}
		if gotRes.Found != wantFound {
			t.Fatalf("found mismatch: got=%v want=%v", gotRes.Found, wantFound)
		}
		if !wantFound {
			if got.Len() != 0 {
				t.Fatalf("expected empty output when not found")
			}
			return
		}
		gotDecoded, err := decodeJSONAny(got.Bytes())
		if err != nil {
			t.Fatalf("decode got: %v", err)
		}
		wantDecoded, err := decodeJSONAny(wantPayload)
		if err != nil {
			t.Fatalf("decode want: %v", err)
		}
		if !reflect.DeepEqual(gotDecoded, wantDecoded) {
			t.Fatalf("projection mismatch:\n got: %#v\nwant: %#v", gotDecoded, wantDecoded)
		}
	})
}

func FuzzProjectFieldsSyntaxRobustness(f *testing.F) {
	f.Add([]byte(`{"id":"a"}`))
	f.Add([]byte(`{"id":`))
	f.Add([]byte{'"', 'a', 0xff, 'b', '"'})
	f.Add([]byte(`[1,2,3]`))

	paths, err := ParseProjectionPaths([]string{"/id", "/meta/trace"})
	if err != nil {
		f.Fatalf("ParseProjectionPaths: %v", err)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 4*1024 {
			input = input[:4*1024]
		}
		var out bytes.Buffer
		_, _ = ProjectFields(ProjectFieldsRequest{
			Reader: bytes.NewReader(input),
			Writer: &out,
			Paths:  paths,
		})
	})
}

func fuzzProjectionObject(seed []byte) map[string]any {
	id := string(seed)
	if id == "" {
		id = "x"
	}
	items := make([]any, 0, 3)
	for i := 0; i < 3; i++ {
		ch := byte('a')
		if len(seed) > i {
			ch = byte('a' + (seed[i] % 26))
		}
		items = append(items, map[string]any{
			"sku": string([]byte{ch}),
			"qty": i + 1,
			"tags": []any{
				string([]byte{'t', byte('0' + byte(i))}),
				id,
			},
		})
	}
	retries := 0
	if len(seed) > 0 {
		retries = int(seed[0] % 8)
	}
	trace := 1
	if len(seed) > 1 {
		trace = int(seed[1]%128 + 1)
	}
	status := "new"
	if len(seed) > 2 && seed[2]%2 == 1 {
		status = "ready"
	}
	return map[string]any{
		"id":     id,
		"status": status,
		"metrics": map[string]any{
			"retries": retries,
		},
		"meta": map[string]any{
			"trace": trace,
			"nested": map[string]any{
				"value": id + "-v",
			},
		},
		"labels": map[string]any{
			"env":   "prod",
			"owner": "ops",
		},
		"items": items,
	}
}
