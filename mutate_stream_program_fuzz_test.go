package lql

import (
	"reflect"
	"testing"
	"time"
)

func FuzzMutateStreamProgramParity(f *testing.F) {
	f.Add([]byte("alpha"), []byte("beta"), true)
	f.Add([]byte("omega"), []byte("delta"), false)
	f.Add([]byte{0, 1, 2, 3}, []byte{9, 8, 7, 6}, true)

	now := time.Unix(1_750_000_000, 0)
	f.Fuzz(func(t *testing.T, seed []byte, programSeed []byte, topArray bool) {
		if len(seed) > 2048 {
			seed = seed[:2048]
		}
		if len(programSeed) > 512 {
			programSeed = programSeed[:512]
		}

		input := synthesizeParityStream(seed, topArray)
		exprs := synthesizeMutationProgram(programSeed)
		muts, err := ParseMutations(exprs, now)
		if err != nil {
			t.Fatalf("parse mutations (%v): %v", exprs, err)
		}

		got, gotErr := runMutateStreamWriterValues(input, muts)
		want, wantErr := collectLegacyMutationStreamValues(input, muts)
		if (gotErr != nil) != (wantErr != nil) {
			t.Fatalf("error presence mismatch exprs=%v gotErr=%v wantErr=%v", exprs, gotErr, wantErr)
		}
		if gotErr != nil {
			return
		}
		wantNDJSON, err := encodeValuesAsNDJSON(want)
		if err != nil {
			t.Fatalf("encode legacy values: %v", err)
		}
		gotDecoded, err := decodeNDJSONValues(got)
		if err != nil {
			t.Fatalf("decode got output: %v", err)
		}
		wantDecoded, err := decodeNDJSONValues(wantNDJSON)
		if err != nil {
			t.Fatalf("decode expected output: %v", err)
		}
		if !reflect.DeepEqual(gotDecoded, wantDecoded) {
			t.Fatalf("mutate stream program parity mismatch exprs=%v", exprs)
		}
	})
}

func synthesizeMutationProgram(seed []byte) []string {
	ops := []string{
		`/status=ready`,
		`/metrics/retries=+2`,
		`rm:/meta/legacy`,
		`/items[]/price=+3`,
		`/labels/*=tagged`,
		`/groups/.../sku="Z"`,
		`/a=1`,
		`/a=+1`,
		`rm:/a`,
		`/a/b=1`,
		`/a/b=+1`,
		`rm:/a/b`,
		`/a/*=1`,
		`/a/*=+1`,
		`rm:/a/*`,
		`/a[]/b=1`,
		`/a[]/b=+1`,
		`rm:/a[]/b`,
		`/a/**=1`,
		`/a/**=+1`,
		`rm:/a/**`,
		`/a/.../b=1`,
		`/a/.../b=+1`,
		`rm:/a/.../b`,
	}
	if len(seed) == 0 {
		return []string{ops[0]}
	}
	count := int(seed[0]%5) + 1
	out := make([]string, 0, count)
	for i := 0; i < count; i++ {
		b := seed[i%len(seed)]
		out = append(out, ops[int(b)%len(ops)])
	}
	return out
}
