package lql

import (
	"reflect"
	"testing"
	"time"
)

func TestMutateStreamNumericSegmentParityObjectAndArray(t *testing.T) {
	makeLineArray := func(amount int, status string) []any {
		lines := make([]any, 11)
		for i := range lines {
			lines[i] = map[string]any{"amount": i}
		}
		lines[10] = map[string]any{
			"amount": amount,
			"status": status,
			"code":   "before",
		}
		return lines
	}

	inputDocs := []any{
		map[string]any{
			"voucher": map[string]any{
				"lines": map[string]any{
					"10": map[string]any{
						"amount": 5,
						"status": "open",
						"code":   "before",
					},
				},
			},
		},
		map[string]any{
			"voucher": map[string]any{
				"lines": makeLineArray(7, "closed"),
			},
		},
		map[string]any{
			"voucher": map[string]any{
				"groups": []any{
					map[string]any{
						"lines": map[string]any{
							"10": map[string]any{
								"amount": 9,
								"status": "open",
								"code":   "before",
							},
						},
					},
				},
			},
		},
	}
	input, err := encodeValuesAsNDJSON(inputDocs)
	if err != nil {
		t.Fatalf("encode input: %v", err)
	}

	muts, err := ParseMutations([]string{
		`/voucher/lines/10/amount=+2`,
		`/voucher/lines/10/status=patched`,
		`/voucher/.../10/code=patched`,
	}, time.Unix(1_750_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}

	got, gotErr := collectMutateStreamValues(input, muts)
	want, wantErr := collectLegacyMutationStreamValues(input, muts)
	if (gotErr != nil) != (wantErr != nil) {
		t.Fatalf("error presence mismatch gotErr=%v wantErr=%v", gotErr, wantErr)
	}
	if gotErr != nil {
		t.Fatalf("unexpected mutate errors got=%v want=%v", gotErr, wantErr)
	}

	gotNorm, err := normalizeMutateValuesForCompare(got)
	if err != nil {
		t.Fatalf("normalize got: %v", err)
	}
	wantNorm, err := normalizeMutateValuesForCompare(want)
	if err != nil {
		t.Fatalf("normalize want: %v", err)
	}
	if !reflect.DeepEqual(gotNorm, wantNorm) {
		t.Fatalf("numeric path parity mismatch:\n got=%#v\nwant=%#v", gotNorm, wantNorm)
	}
}

func TestMutateStreamNumericSegmentCreatesObjectKey(t *testing.T) {
	inputDocs := []any{
		map[string]any{
			"voucher": map[string]any{
				"lines": map[string]any{},
			},
		},
	}
	input, err := encodeValuesAsNDJSON(inputDocs)
	if err != nil {
		t.Fatalf("encode input: %v", err)
	}

	muts, err := ParseMutations([]string{
		`/voucher/lines/10/status=created`,
		`/voucher/lines/10/amount=+3`,
	}, time.Unix(1_750_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}

	got, gotErr := collectMutateStreamValues(input, muts)
	want, wantErr := collectLegacyMutationStreamValues(input, muts)
	if (gotErr != nil) != (wantErr != nil) {
		t.Fatalf("error presence mismatch gotErr=%v wantErr=%v", gotErr, wantErr)
	}
	if gotErr != nil {
		t.Fatalf("unexpected mutate errors got=%v want=%v", gotErr, wantErr)
	}

	gotNorm, err := normalizeMutateValuesForCompare(got)
	if err != nil {
		t.Fatalf("normalize got: %v", err)
	}
	wantNorm, err := normalizeMutateValuesForCompare(want)
	if err != nil {
		t.Fatalf("normalize want: %v", err)
	}
	if !reflect.DeepEqual(gotNorm, wantNorm) {
		t.Fatalf("numeric create parity mismatch:\n got=%#v\nwant=%#v", gotNorm, wantNorm)
	}
}

func normalizeMutateValuesForCompare(values []any) ([]any, error) {
	payload, err := encodeValuesAsNDJSON(values)
	if err != nil {
		return nil, err
	}
	return decodeNDJSONValues(payload)
}
