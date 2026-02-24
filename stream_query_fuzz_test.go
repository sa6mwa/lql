package lql

import (
	"bytes"
	"errors"
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

func FuzzQueryStreamStopControlsParity(f *testing.F) {
	f.Add([]byte("alpha"), uint8(0), uint8(1), uint8(0), uint8(0), true)
	f.Add([]byte("beta"), uint8(3), uint8(0), uint8(2), uint8(1), false)
	f.Add([]byte("gamma"), uint8(7), uint8(4), uint8(4), uint8(4), true)

	expressions := paritySelectorExpressions()
	f.Fuzz(func(t *testing.T, seed []byte, selectorIdx uint8, matchLimit uint8, candidateLimit uint8, byteLimit uint8, topArray bool) {
		if len(seed) > 2*1024 {
			seed = seed[:2*1024]
		}
		selector := mustParseSelector(t, expressions[int(selectorIdx)%len(expressions)])
		input := synthesizeParityStream(seed, topArray)

		baseRecords, baseResult, baseErr := runQueryStreamDecisionRecords(input, selector, 0, 0, 0)
		if baseErr != nil {
			return
		}

		maxMatches := int64(matchLimit % 6)
		maxCandidates := int64(candidateLimit % 6)
		maxBytes := int64(byteLimit % 6)
		if maxBytes > 0 {
			if int(maxBytes) <= len(baseRecords) {
				maxBytes = baseRecords[maxBytes-1].endOffset
			} else {
				maxBytes = baseResult.BytesRead
			}
		}

		gotRecords, gotResult, gotErr := runQueryStreamDecisionRecords(input, selector, maxMatches, maxCandidates, maxBytes)
		if gotErr != nil {
			t.Fatalf("query stream with limits: %v", gotErr)
		}

		expSeen, expMatched, expReason := expectedQueryStop(baseRecords, maxMatches, maxCandidates, maxBytes)
		if int64(len(gotRecords)) != expSeen {
			t.Fatalf("seen mismatch: got=%d want=%d", len(gotRecords), expSeen)
		}
		if gotResult.CandidatesSeen != expSeen {
			t.Fatalf("summary seen mismatch: got=%d want=%d", gotResult.CandidatesSeen, expSeen)
		}
		if gotResult.CandidatesMatched != expMatched {
			t.Fatalf("summary matched mismatch: got=%d want=%d", gotResult.CandidatesMatched, expMatched)
		}
		if gotResult.StopReason != expReason {
			t.Fatalf("stop reason mismatch: got=%s want=%s", gotResult.StopReason, expReason)
		}
	})
}

func FuzzQueryStreamCallerSinkCallbackStop(f *testing.F) {
	f.Add([]byte("alpha"), uint8(0), uint8(1), true)
	f.Add([]byte("beta"), uint8(3), uint8(2), false)
	f.Add([]byte("gamma"), uint8(7), uint8(5), true)

	expressions := paritySelectorExpressions()
	f.Fuzz(func(t *testing.T, seed []byte, selectorIdx uint8, stopAfter uint8, topArray bool) {
		if len(seed) > 2*1024 {
			seed = seed[:2*1024]
		}
		selector := mustParseSelector(t, expressions[int(selectorIdx)%len(expressions)])
		input := synthesizeParityStream(seed, topArray)

		baseRecords, _, baseErr := runQueryStreamDecisionRecords(input, selector, 0, 0, 0)
		if baseErr != nil {
			return
		}
		limit := int(stopAfter % 6)
		if limit == 0 {
			limit = len(baseRecords) + 1
		}

		callbacks := 0
		result, err := QueryStreamWithResult(QueryStreamRequest{
			Reader:               bytes.NewReader(input),
			Selector:             selector,
			IncludeJSON:          true,
			DisableInternalSpool: true,
			PayloadSinkFactory: func(QueryStreamPayloadSinkRequest) (QueryStreamPayloadSink, error) {
				return &fuzzQueryMemorySink{}, nil
			},
			OnValue: func(QueryStreamValue) error {
				callbacks++
				if callbacks >= limit {
					return errors.Join(errors.New("stop"), ErrStreamStop)
				}
				return nil
			},
		})
		if err != nil {
			t.Fatalf("query stream callback stop: %v", err)
		}
		expected := limit
		if expected > len(baseRecords) {
			expected = len(baseRecords)
		}
		if callbacks != expected {
			t.Fatalf("callback count mismatch: got=%d want=%d", callbacks, expected)
		}
		if expected < len(baseRecords) {
			if !result.StoppedEarly || result.StopReason != QueryStreamStopCallbackStop {
				t.Fatalf("expected callback stop summary, got %+v", result)
			}
		}
	})
}

type queryDecisionRecord struct {
	matched   bool
	endOffset int64
}

func runQueryStreamDecisionRecords(input []byte, selector Selector, maxMatches, maxCandidates, maxBytes int64) ([]queryDecisionRecord, QueryStreamResult, error) {
	records := make([]queryDecisionRecord, 0, 16)
	result, err := QueryStreamWithResult(QueryStreamRequest{
		Reader:        bytes.NewReader(input),
		Selector:      selector,
		MaxMatches:    maxMatches,
		MaxCandidates: maxCandidates,
		MaxBytesRead:  maxBytes,
		OnDecision: func(d QueryStreamDecision) error {
			records = append(records, queryDecisionRecord{
				matched:   d.Matched,
				endOffset: d.Offset + d.Size,
			})
			return nil
		},
	})
	return records, result, err
}

func expectedQueryStop(records []queryDecisionRecord, maxMatches, maxCandidates, maxBytes int64) (seen int64, matched int64, reason QueryStreamStopReason) {
	for i, record := range records {
		seen = int64(i + 1)
		if record.matched {
			matched++
		}
		switch {
		case maxMatches > 0 && matched >= maxMatches:
			return seen, matched, QueryStreamStopMatchLimit
		case maxCandidates > 0 && seen >= maxCandidates:
			return seen, matched, QueryStreamStopCandidateLimit
		case maxBytes > 0 && record.endOffset >= maxBytes:
			return seen, matched, QueryStreamStopByteLimit
		}
	}
	return seen, matched, QueryStreamStopNone
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
