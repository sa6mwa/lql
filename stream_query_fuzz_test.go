package lql

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"
)

func FuzzQueryStreamStdlibSyntaxParity(f *testing.F) {
	f.Add([]byte(`{"id":"a","status":"open"}`))
	f.Add([]byte(`[{"id":"a"},[{"id":"b"}],true,null]`))
	f.Add([]byte(`{"s":"\\ud800"}`))
	f.Add([]byte{'"', 'a', 0xff, 'b', '"'})
	f.Add([]byte(`{"id":1`))

	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 4*1024 {
			return
		}
		assertQueryStreamParity(t, input, Selector{}, true)
	})
}

func FuzzQueryStreamSelectorParity(f *testing.F) {
	f.Add([]byte("alpha"), uint8(0), true, true)
	f.Add([]byte("beta"), uint8(3), false, true)
	f.Add([]byte("gamma"), uint8(7), true, false)
	f.Add([]byte{0, 1, 2, 3, 4}, uint8(12), false, false)

	expressions := paritySelectorExpressions()
	f.Fuzz(func(t *testing.T, seed []byte, selectorIdx uint8, topArray bool, includeJSON bool) {
		if len(seed) > 2*1024 {
			seed = seed[:2*1024]
		}
		selector := mustParseSelector(t, expressions[int(selectorIdx)%len(expressions)])
		input := synthesizeParityStream(seed, topArray)
		assertQueryStreamParity(t, input, selector, includeJSON)
	})
}

func FuzzQueryStreamSpoolSelectorParity(f *testing.F) {
	f.Add([]byte("alpha"), uint8(0), uint8(4), true)
	f.Add([]byte("beta"), uint8(3), uint8(1), false)
	f.Add([]byte("gamma"), uint8(7), uint8(16), true)

	expressions := paritySelectorExpressions()
	f.Fuzz(func(t *testing.T, seed []byte, selectorIdx uint8, memoryKiB uint8, topArray bool) {
		if len(seed) > 2*1024 {
			seed = seed[:2*1024]
		}
		selector := mustParseSelector(t, expressions[int(selectorIdx)%len(expressions)])
		input := synthesizeParityStream(seed, topArray)
		spoolBytes := int64(memoryKiB%32+1) * 1024

		got, gotErr := runQueryStreamSpoolParity(input, selector, spoolBytes)
		want, wantErr := runReferenceQueryParity(input, selector, true)

		if len(got) != len(want) {
			t.Fatalf("candidate count mismatch: got %d want %d", len(got), len(want))
		}
		for i := range got {
			if got[i].matched != want[i].matched {
				t.Fatalf("candidate %d matched mismatch: got %v want %v", i, got[i].matched, want[i].matched)
			}
			if !reflect.DeepEqual(got[i].value, want[i].value) {
				t.Fatalf("candidate %d value mismatch:\n got: %#v\nwant: %#v", i, got[i].value, want[i].value)
			}
		}
		if (gotErr != nil) != (wantErr != nil) {
			t.Fatalf("error parity mismatch: gotErr=%v wantErr=%v", gotErr, wantErr)
		}
	})
}

func runQueryStreamSpoolParity(input []byte, selector Selector, spoolBytes int64) ([]streamParityRecord, error) {
	records := make([]streamParityRecord, 0, 16)
	err := QueryStream(QueryStreamRequest{
		Reader:           bytes.NewReader(input),
		Selector:         selector,
		IncludeJSON:      true,
		SpoolMemoryBytes: spoolBytes,
		OnValue: func(value QueryStreamValue) error {
			record := streamParityRecord{matched: value.Matched}
			payload := value.JSON
			if payload == nil {
				if value.OpenJSON == nil {
					return fmt.Errorf("missing OpenJSON for spooled payload")
				}
				rc, err := value.OpenJSON()
				if err != nil {
					return err
				}
				defer rc.Close()
				payload, err = io.ReadAll(rc)
				if err != nil {
					return err
				}
			}
			decoded, err := decodeJSONAny(payload)
			if err != nil {
				return err
			}
			record.value = decoded
			records = append(records, record)
			return nil
		},
	})
	return records, err
}

func FuzzQueryStreamMatchedOnlyParity(f *testing.F) {
	f.Add([]byte("alpha"), uint8(0), true)
	f.Add([]byte("beta"), uint8(3), false)
	f.Add([]byte("gamma"), uint8(7), true)

	expressions := paritySelectorExpressions()
	f.Fuzz(func(t *testing.T, seed []byte, selectorIdx uint8, topArray bool) {
		if len(seed) > 2*1024 {
			seed = seed[:2*1024]
		}
		selector := mustParseSelector(t, expressions[int(selectorIdx)%len(expressions)])
		input := synthesizeParityStream(seed, topArray)

		all, err := runQueryStreamMatchedOnlyRecords(input, selector, false)
		if err != nil {
			return
		}
		matchedOnly, err := runQueryStreamMatchedOnlyRecords(input, selector, true)
		if err != nil {
			return
		}

		expected := make([]streamParityRecord, 0, len(all))
		for _, record := range all {
			if record.matched {
				expected = append(expected, record)
			}
		}
		if !reflect.DeepEqual(matchedOnly, expected) {
			t.Fatalf("matched-only parity mismatch:\n got=%#v\nwant=%#v", matchedOnly, expected)
		}
	})
}

func runQueryStreamMatchedOnlyRecords(input []byte, selector Selector, matchedOnly bool) ([]streamParityRecord, error) {
	records := make([]streamParityRecord, 0, 16)
	err := QueryStream(QueryStreamRequest{
		Reader:      bytes.NewReader(input),
		Selector:    selector,
		MatchedOnly: matchedOnly,
		OnValue: func(value QueryStreamValue) error {
			records = append(records, streamParityRecord{matched: value.Matched})
			return nil
		},
	})
	return records, err
}

func FuzzQueryStreamCallerSinkParity(f *testing.F) {
	f.Add([]byte("alpha"), uint8(0), true)
	f.Add([]byte("beta"), uint8(3), false)
	f.Add([]byte("gamma"), uint8(7), true)

	expressions := paritySelectorExpressions()
	f.Fuzz(func(t *testing.T, seed []byte, selectorIdx uint8, topArray bool) {
		if len(seed) > 2*1024 {
			seed = seed[:2*1024]
		}
		selector := mustParseSelector(t, expressions[int(selectorIdx)%len(expressions)])
		input := synthesizeParityStream(seed, topArray)

		got, gotErr := runQueryStreamCallerSinkParity(input, selector)
		want, wantErr := runReferenceQueryParity(input, selector, true)
		if len(got) != len(want) {
			t.Fatalf("candidate count mismatch: got %d want %d", len(got), len(want))
		}
		for i := range got {
			if got[i].matched != want[i].matched {
				t.Fatalf("candidate %d matched mismatch: got %v want %v", i, got[i].matched, want[i].matched)
			}
			if !reflect.DeepEqual(got[i].value, want[i].value) {
				t.Fatalf("candidate %d value mismatch:\n got: %#v\nwant: %#v", i, got[i].value, want[i].value)
			}
		}
		if (gotErr != nil) != (wantErr != nil) {
			t.Fatalf("error parity mismatch: gotErr=%v wantErr=%v", gotErr, wantErr)
		}
	})
}

func runQueryStreamCallerSinkParity(input []byte, selector Selector) ([]streamParityRecord, error) {
	records := make([]streamParityRecord, 0, 16)
	err := QueryStream(QueryStreamRequest{
		Reader:               bytes.NewReader(input),
		Selector:             selector,
		IncludeJSON:          true,
		DisableInternalSpool: true,
		PayloadSinkFactory: func(QueryStreamPayloadSinkRequest) (QueryStreamPayloadSink, error) {
			return &fuzzQueryMemorySink{}, nil
		},
		OnValue: func(value QueryStreamValue) error {
			record := streamParityRecord{matched: value.Matched}
			payload := value.JSON
			if payload == nil {
				if value.OpenJSON == nil {
					return fmt.Errorf("missing OpenJSON for caller sink payload")
				}
				rc, err := value.OpenJSON()
				if err != nil {
					return err
				}
				defer rc.Close()
				payload, err = io.ReadAll(rc)
				if err != nil {
					return err
				}
			}
			decoded, err := decodeJSONAny(payload)
			if err != nil {
				return err
			}
			record.value = decoded
			records = append(records, record)
			return nil
		},
	})
	return records, err
}

type fuzzQueryMemorySink struct {
	buffer bytes.Buffer
}

func (s *fuzzQueryMemorySink) Write(p []byte) (int, error) {
	return s.buffer.Write(p)
}

func (s *fuzzQueryMemorySink) Finalize() error {
	return nil
}

func (s *fuzzQueryMemorySink) Open() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.buffer.Bytes())), nil
}

func (s *fuzzQueryMemorySink) Bytes() []byte {
	return s.buffer.Bytes()
}

func (s *fuzzQueryMemorySink) SizeHint() int {
	return s.buffer.Len()
}

func (s *fuzzQueryMemorySink) Cleanup() error {
	return nil
}
