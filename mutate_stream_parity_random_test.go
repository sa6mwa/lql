package lql

import (
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestMutateStreamParityRandomizedPrograms(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_750_000_000, 0)
	programs := []string{
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
		`/status=ready`,
		`/items[]/price=+3`,
		`/labels/*=tagged`,
		`/groups/.../sku="Z"`,
		`/voucher/lines/10/status=patched`,
		`/voucher/lines/10/amount=+2`,
		`/voucher/.../10/code=patched`,
	}

	normalize := func(values []any) []any {
		ndjson, err := encodeValuesAsNDJSON(values)
		if err != nil {
			t.Fatalf("encode values: %v", err)
		}
		decoded, err := decodeNDJSONValues(ndjson)
		if err != nil {
			t.Fatalf("decode values: %v", err)
		}
		return decoded
	}

	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 1000; i++ {
		seed := []byte{
			byte(rng.Intn(255)),
			byte(rng.Intn(255)),
			byte(rng.Intn(255)),
			byte(i),
		}
		input := synthesizeParityStream(seed, i%2 == 0)

		count := 1 + rng.Intn(5)
		exprs := make([]string, 0, count)
		for j := 0; j < count; j++ {
			exprs = append(exprs, programs[rng.Intn(len(programs))])
		}
		muts, err := ParseMutations(exprs, now)
		if err != nil {
			t.Fatalf("parse mutations (%v): %v", exprs, err)
		}

		got, gotErr := collectMutateStreamValues(input, muts)
		want, wantErr := collectLegacyMutationStreamValues(input, muts)
		if (gotErr != nil) != (wantErr != nil) {
			t.Fatalf("error presence mismatch exprs=%v gotErr=%v wantErr=%v", exprs, gotErr, wantErr)
		}
		if gotErr != nil {
			continue
		}
		got = normalize(got)
		want = normalize(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("mutate stream parity mismatch exprs=%v", exprs)
		}
	}
}

func TestMutateStreamDirectSetStillEvaluatesEarlierWildcardErrors(t *testing.T) {
	now := time.Unix(1_750_000_000, 0)
	muts, err := ParseMutations([]string{`/a/*=+1`, `/a=2`}, now)
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}

	input := []byte(`{"a":{"b":"x"}}`)
	_, gotErr := collectMutateStreamValues(input, muts)
	_, wantErr := collectLegacyMutationStreamValues(input, muts)
	if (gotErr != nil) != (wantErr != nil) {
		t.Fatalf("error presence mismatch gotErr=%v wantErr=%v", gotErr, wantErr)
	}
	if gotErr == nil {
		t.Fatalf("expected numeric type error")
	}
	if !strings.Contains(gotErr.Error(), "not numeric") || !strings.Contains(wantErr.Error(), "not numeric") {
		t.Fatalf("unexpected error mismatch got=%v want=%v", gotErr, wantErr)
	}
}
