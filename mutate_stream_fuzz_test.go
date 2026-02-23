package lql

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func FuzzMutateStreamWriterParity(f *testing.F) {
	f.Add([]byte("alpha"), uint8(0), false)
	f.Add([]byte("beta"), uint8(1), true)
	f.Add([]byte{0, 1, 2, 3}, uint8(2), false)
	f.Add([]byte("delta"), uint8(3), true)

	programs := mutateParityPrograms()
	now := time.Unix(1_750_000_000, 0)
	f.Fuzz(func(t *testing.T, seed []byte, programIdx uint8, topArray bool) {
		if len(seed) > 2048 {
			seed = seed[:2048]
		}
		input := synthesizeParityStream(seed, topArray)
		exprs := programs[int(programIdx)%len(programs)]
		muts, err := ParseMutations(exprs, now)
		if err != nil {
			t.Fatalf("parse mutations: %v", err)
		}

		got, gotErr := runMutateStreamWriterValues(input, muts)
		want, wantErr := collectLegacyMutationStreamValues(input, muts)
		if (gotErr != nil) != (wantErr != nil) {
			t.Fatalf("error presence mismatch: gotErr=%v wantErr=%v", gotErr, wantErr)
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
			t.Fatalf("mutate stream writer parity mismatch:\n got: %#v\nwant: %#v", gotDecoded, wantDecoded)
		}
	})
}

func mutateParityPrograms() [][]string {
	return [][]string{
		{`/status=ready`},
		{`/metrics/retries=+2`, `rm:/meta/legacy`},
		{`/labels/*=tagged`, `/groups/.../sku="Z"`},
		{`/status=ready`, `/items[]/price=+3`, `/labels/*=tagged`},
		{`/id=override`, `rm:/labels/env`, `/metrics/retries=+1`},
	}
}

func runMutateStreamWriterValues(input []byte, muts []Mutation) ([]byte, error) {
	var out bytes.Buffer
	err := MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader(input),
		Writer:    &out,
		Mutations: muts,
	})
	if err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func FuzzMutateStreamCallbackParity(f *testing.F) {
	f.Add([]byte("alpha"), uint8(0), false)
	f.Add([]byte("beta"), uint8(1), true)
	f.Add([]byte{0, 1, 2, 3}, uint8(2), false)
	f.Add([]byte("delta"), uint8(3), true)

	programs := mutateParityPrograms()
	now := time.Unix(1_750_000_000, 0)
	f.Fuzz(func(t *testing.T, seed []byte, programIdx uint8, topArray bool) {
		if len(seed) > 2048 {
			seed = seed[:2048]
		}
		input := synthesizeParityStream(seed, topArray)
		exprs := programs[int(programIdx)%len(programs)]
		muts, err := ParseMutations(exprs, now)
		if err != nil {
			t.Fatalf("parse mutations: %v", err)
		}

		got, gotErr := runMutateStreamCallbackValues(input, muts)
		want, wantErr := collectLegacyMutationStreamValues(input, muts)
		if (gotErr != nil) != (wantErr != nil) {
			t.Fatalf("error presence mismatch: gotErr=%v wantErr=%v", gotErr, wantErr)
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
			t.Fatalf("decode callback output: %v", err)
		}
		wantDecoded, err := decodeNDJSONValues(wantNDJSON)
		if err != nil {
			t.Fatalf("decode expected output: %v", err)
		}
		if !reflect.DeepEqual(gotDecoded, wantDecoded) {
			t.Fatalf("mutate stream callback parity mismatch:\n got: %#v\nwant: %#v", gotDecoded, wantDecoded)
		}
	})
}

func runMutateStreamCallbackValues(input []byte, muts []Mutation) ([]byte, error) {
	var out bytes.Buffer
	err := MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader(input),
		Mutations: muts,
		OnValue: func(value MutateStreamValue) error {
			payload, err := readMutateStreamValuePayload(value)
			if err != nil {
				return err
			}
			if _, err := out.Write(payload); err != nil {
				return err
			}
			if _, err := out.Write([]byte{'\n'}); err != nil {
				return err
			}
			var decoded any
			dec := json.NewDecoder(bytes.NewReader(payload))
			dec.UseNumber()
			if err := dec.Decode(&decoded); err != nil {
				return err
			}
			if err := consumeOnlyWhitespace(dec); err != nil {
				return err
			}
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func FuzzMutateStreamModeRobustness(f *testing.F) {
	f.Add([]byte(`{"id":"a"}`), uint8(0))
	f.Add([]byte(`[{"id":"a"}]`), uint8(1))
	f.Add([]byte(`{"id":"a"}
{"id":"b"}`), uint8(2))
	f.Add([]byte(`{"id":`), uint8(3))

	f.Fuzz(func(t *testing.T, input []byte, modeIdx uint8) {
		if len(input) > 4*1024 {
			input = input[:4*1024]
		}
		mode := MutateModeAuto
		switch modeIdx % 4 {
		case 1:
			mode = MutateSingleValueOnly
		case 2:
			mode = MutateObjectRootOnly
		case 3:
			mode = MutateSingleObjectOnly
		}
		_ = MutateStream(MutateStreamRequest{
			Reader: bytes.NewReader(input),
			Writer: &bytes.Buffer{},
			Mode:   mode,
		})
	})
}
