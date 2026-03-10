package lql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestMutateStreamFlattensTopArraysAndMutatesObjects(t *testing.T) {
	muts, err := ParseMutations([]string{`/status=ready`}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}

	input := []byte(`[{"id":"a","status":"new"},[{"id":"b","status":"new"}],7]
{"id":"c","status":"new"}
true`)

	got, err := collectMutateStreamValues(input, muts)
	if err != nil {
		t.Fatalf("mutate stream: %v", err)
	}

	if len(got) != 5 {
		t.Fatalf("expected 5 candidates, got %d", len(got))
	}
	for _, idx := range []int{0, 1, 3} {
		doc, ok := got[idx].(map[string]any)
		if !ok {
			t.Fatalf("candidate %d expected object, got %T", idx, got[idx])
		}
		if doc["status"] != "ready" {
			t.Fatalf("candidate %d expected status ready, got %+v", idx, doc["status"])
		}
	}
	if got[2] != json.Number("7") {
		t.Fatalf("expected scalar 7 unchanged, got %#v", got[2])
	}
	if got[4] != true {
		t.Fatalf("expected trailing boolean unchanged, got %#v", got[4])
	}
}

func TestMutateStreamParityWithLegacyPipeline(t *testing.T) {
	now := time.Unix(1_750_000_000, 0)
	muts, err := ParseMutations([]string{
		`/status=ready`,
		`/metrics/retries=+2`,
		`rm:/meta/legacy`,
		`/items[]/price=+3`,
		`/labels/*=tagged`,
		`/groups/.../sku="Z"`,
	}, now)
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}

	input := []byte(`
{"id":"a","status":"new","metrics":{"retries":1},"meta":{"legacy":"x"},"labels":{"env":"prod"},"items":[{"price":10}],"groups":[{"items":[{"sku":"A"}]}]}
[{"id":"b","status":"new","metrics":{"retries":3},"meta":{"legacy":"y"},"labels":{"owner":"alice"},"items":[{"price":2},{"price":8}],"groups":[{"items":[{"sku":"B"}]}]},[{"id":"c","status":"old","meta":{},"items":[]}],99]
{"id":"d","status":"new","labels":{"env":"stage"},"groups":[{"items":[{"sku":"D"}]}]}
`)

	got, gotErr := collectMutateStreamValues(input, muts)
	want, wantErr := collectLegacyMutationStreamValues(input, muts)

	if len(got) != len(want) {
		t.Fatalf("candidate count mismatch: got %d want %d (gotErr=%v wantErr=%v)", len(got), len(want), gotErr, wantErr)
	}
	if (gotErr != nil) != (wantErr != nil) {
		t.Fatalf("error presence mismatch: gotErr=%v wantErr=%v", gotErr, wantErr)
	}
	gotNDJSON, err := encodeValuesAsNDJSON(got)
	if err != nil {
		t.Fatalf("encode got values: %v", err)
	}
	wantNDJSON, err := encodeValuesAsNDJSON(want)
	if err != nil {
		t.Fatalf("encode want values: %v", err)
	}
	gotDecoded, err := decodeNDJSONValues(gotNDJSON)
	if err != nil {
		t.Fatalf("decode got values: %v", err)
	}
	wantDecoded, err := decodeNDJSONValues(wantNDJSON)
	if err != nil {
		t.Fatalf("decode want values: %v", err)
	}
	if !reflect.DeepEqual(gotDecoded, wantDecoded) {
		t.Fatalf("mutate stream parity mismatch:\n got: %#v\nwant: %#v", gotDecoded, wantDecoded)
	}
}

func TestMutateStreamValidationErrors(t *testing.T) {
	err := MutateStream(MutateStreamRequest{})
	if err == nil {
		t.Fatalf("expected reader validation error")
	}

	err = MutateStream(MutateStreamRequest{Reader: bytes.NewReader(nil)})
	if err == nil {
		t.Fatalf("expected callback validation error")
	}

	muts, parseErr := ParseMutations([]string{`/status=ready`}, time.Unix(1_700_000_000, 0))
	if parseErr != nil {
		t.Fatalf("parse mutations: %v", parseErr)
	}
	plan, planErr := NewMutateStreamPlan(muts)
	if planErr != nil {
		t.Fatalf("new mutate plan: %v", planErr)
	}
	err = MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader([]byte(`{"id":"a"}`)),
		Writer:    io.Discard,
		Mutations: muts,
		Plan:      plan,
	})
	if err == nil {
		t.Fatalf("expected mutations/plan conflict error")
	}
}

func TestMutateStreamInvalidJSON(t *testing.T) {
	muts, err := ParseMutations([]string{`/status=ready`}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}

	_, err = collectMutateStreamValues([]byte(`{"id":1`), muts)
	if err == nil {
		t.Fatalf("expected decode error")
	}
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T", err)
	}
	if streamErr.Code != StreamErrorInvalidBody {
		t.Fatalf("expected invalid_body, got %s", streamErr.Code)
	}
}

func TestMutateStreamWriterOutput(t *testing.T) {
	muts, err := ParseMutations([]string{`/status=ready`}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}
	var out bytes.Buffer
	err = MutateStream(MutateStreamRequest{
		Reader: bytes.NewReader([]byte(`{"id":"a","status":"new"}
{"id":"b","status":"new"}`)),
		Writer:    &out,
		Mutations: muts,
	})
	if err != nil {
		t.Fatalf("mutate stream: %v", err)
	}
	values := decodeJSONValuesForMutateTest(t, out.Bytes())
	if len(values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(values))
	}
	if values[0].(map[string]any)["status"] != "ready" || values[1].(map[string]any)["status"] != "ready" {
		t.Fatalf("expected mutated status in writer output, got %#v", values)
	}
}

func TestMutateStreamFileBackedTextCreatesMissingObjectPath(t *testing.T) {
	resolver := &mutateTestFileResolver{
		payloads: map[string][]byte{
			filepath.Join("/virtual", "blob.txt"): []byte("hello\n\"quoted\""),
		},
	}
	muts, err := ParseMutationsWithOptions([]string{`textfile:/meta/blob=blob.txt`}, time.Unix(1_700_000_000, 0), ParseMutationsOptions{
		EnableFileValues:  true,
		FileValueBaseDir:  "/virtual",
		FileValueResolver: resolver,
	})
	if err != nil {
		t.Fatalf("parse file-backed mutations: %v", err)
	}

	var out bytes.Buffer
	err = MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader([]byte(`{"id":"a"}`)),
		Writer:    &out,
		Mutations: muts,
	})
	if err != nil {
		t.Fatalf("mutate stream: %v", err)
	}
	values := decodeJSONValuesForMutateTest(t, out.Bytes())
	doc := values[0].(map[string]any)
	meta := doc["meta"].(map[string]any)
	if meta["blob"] != "hello\n\"quoted\"" {
		t.Fatalf("unexpected text payload: %#v", meta["blob"])
	}
	if got := resolver.opens[filepath.Join("/virtual", "blob.txt")]; got != 1 {
		t.Fatalf("expected one open for explicit text mode, got %d", got)
	}
}

func TestMutateStreamFileBackedBase64WritesStringValue(t *testing.T) {
	resolver := &mutateTestFileResolver{
		payloads: map[string][]byte{
			filepath.Join("/virtual", "blob.bin"): {0x00, 0x01, 0x02, 'a'},
		},
	}
	muts, err := ParseMutationsWithOptions([]string{`base64file:/payload=blob.bin`}, time.Unix(1_700_000_000, 0), ParseMutationsOptions{
		EnableFileValues:  true,
		FileValueBaseDir:  "/virtual",
		FileValueResolver: resolver,
	})
	if err != nil {
		t.Fatalf("parse file-backed mutations: %v", err)
	}

	var out bytes.Buffer
	err = MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader([]byte(`{"id":"a","payload":"old"}`)),
		Writer:    &out,
		Mutations: muts,
	})
	if err != nil {
		t.Fatalf("mutate stream: %v", err)
	}
	values := decodeJSONValuesForMutateTest(t, out.Bytes())
	doc := values[0].(map[string]any)
	if doc["payload"] != "AAECYQ==" {
		t.Fatalf("unexpected base64 payload: %#v", doc["payload"])
	}
	if got := resolver.opens[filepath.Join("/virtual", "blob.bin")]; got != 1 {
		t.Fatalf("expected one open for explicit base64 mode, got %d", got)
	}
}

func TestMutateStreamFileBackedAutoModeSweepsThenRestreams(t *testing.T) {
	resolver := &mutateTestFileResolver{
		payloads: map[string][]byte{
			filepath.Join("/virtual", "text.txt"): []byte("hello world"),
			filepath.Join("/virtual", "blob.bin"): {0x00, 0xff, 0x01},
		},
	}
	textMuts, err := ParseMutationsWithOptions([]string{`file:/payload=text.txt`}, time.Unix(1_700_000_000, 0), ParseMutationsOptions{
		EnableFileValues:  true,
		FileValueBaseDir:  "/virtual",
		FileValueResolver: resolver,
	})
	if err != nil {
		t.Fatalf("parse text file-backed mutations: %v", err)
	}
	binMuts, err := ParseMutationsWithOptions([]string{`file:/payload=blob.bin`}, time.Unix(1_700_000_000, 0), ParseMutationsOptions{
		EnableFileValues:  true,
		FileValueBaseDir:  "/virtual",
		FileValueResolver: resolver,
	})
	if err != nil {
		t.Fatalf("parse binary file-backed mutations: %v", err)
	}

	var textOut bytes.Buffer
	if err := MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader([]byte(`{"payload":"old"}`)),
		Writer:    &textOut,
		Mutations: textMuts,
	}); err != nil {
		t.Fatalf("mutate stream auto text: %v", err)
	}
	var binOut bytes.Buffer
	if err := MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader([]byte(`{"payload":"old"}`)),
		Writer:    &binOut,
		Mutations: binMuts,
	}); err != nil {
		t.Fatalf("mutate stream auto binary: %v", err)
	}

	textDoc := decodeJSONValuesForMutateTest(t, textOut.Bytes())[0].(map[string]any)
	if textDoc["payload"] != "hello world" {
		t.Fatalf("unexpected auto text payload: %#v", textDoc["payload"])
	}
	binDoc := decodeJSONValuesForMutateTest(t, binOut.Bytes())[0].(map[string]any)
	if binDoc["payload"] != "AP8B" {
		t.Fatalf("unexpected auto binary payload: %#v", binDoc["payload"])
	}
	if got := resolver.opens[filepath.Join("/virtual", "text.txt")]; got != 2 {
		t.Fatalf("expected two opens for auto text mode, got %d", got)
	}
	if got := resolver.opens[filepath.Join("/virtual", "blob.bin")]; got != 2 {
		t.Fatalf("expected two opens for auto binary mode, got %d", got)
	}
}

func TestMutateStreamFileBackedPlanMatchesMutations(t *testing.T) {
	resolver := &mutateTestFileResolver{
		payloads: map[string][]byte{
			filepath.Join("/virtual", "blob.txt"): []byte("plan payload"),
		},
	}
	muts, err := ParseMutationsWithOptions([]string{`textfile:/payload=blob.txt`}, time.Unix(1_700_000_000, 0), ParseMutationsOptions{
		EnableFileValues:  true,
		FileValueBaseDir:  "/virtual",
		FileValueResolver: resolver,
	})
	if err != nil {
		t.Fatalf("parse file-backed mutations: %v", err)
	}
	plan, err := NewMutateStreamPlan(muts)
	if err != nil {
		t.Fatalf("new mutate plan: %v", err)
	}

	input := []byte(`{"id":"a","payload":"old"}`)
	var outMutations bytes.Buffer
	if err := MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader(input),
		Writer:    &outMutations,
		Mutations: muts,
	}); err != nil {
		t.Fatalf("mutate stream with mutations: %v", err)
	}
	var outPlan bytes.Buffer
	if err := MutateStream(MutateStreamRequest{
		Reader: bytes.NewReader(input),
		Writer: &outPlan,
		Plan:   plan,
	}); err != nil {
		t.Fatalf("mutate stream with plan: %v", err)
	}

	gotMutations := decodeJSONValuesForMutateTest(t, outMutations.Bytes())
	gotPlan := decodeJSONValuesForMutateTest(t, outPlan.Bytes())
	if !reflect.DeepEqual(gotMutations, gotPlan) {
		t.Fatalf("file-backed mutate plan parity mismatch:\nmutations=%#v\nplan=%#v", gotMutations, gotPlan)
	}
}

func TestMutateStreamFileBackedResolverOpenErrorPropagates(t *testing.T) {
	sentinel := errors.New("open failed")
	muts, err := ParseMutationsWithOptions([]string{`textfile:/payload=blob.txt`}, time.Unix(1_700_000_000, 0), ParseMutationsOptions{
		EnableFileValues:  true,
		FileValueBaseDir:  "/virtual",
		FileValueResolver: failingMutateFileResolver{err: sentinel},
	})
	if err != nil {
		t.Fatalf("parse file-backed mutations: %v", err)
	}
	err = MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader([]byte(`{"payload":"old"}`)),
		Writer:    io.Discard,
		Mutations: muts,
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected open sentinel, got %v", err)
	}
}

func TestMutateStreamFileBackedReadErrorPropagates(t *testing.T) {
	sentinel := errors.New("read failed")
	resolver := &failingReadMutateFileResolver{
		payload: []byte("hello"),
		err:     sentinel,
	}
	muts, err := ParseMutationsWithOptions([]string{`textfile:/payload=blob.txt`}, time.Unix(1_700_000_000, 0), ParseMutationsOptions{
		EnableFileValues:  true,
		FileValueBaseDir:  "/virtual",
		FileValueResolver: resolver,
	})
	if err != nil {
		t.Fatalf("parse file-backed mutations: %v", err)
	}
	err = MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader([]byte(`{"payload":"old"}`)),
		Writer:    io.Discard,
		Mutations: muts,
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected read sentinel, got %v", err)
	}
}

func TestMutateStreamTextFileRejectsInvalidUTF8(t *testing.T) {
	resolver := &nonAllocMutateFileResolver{
		payloads: map[string][]byte{
			filepath.Join("/virtual", "blob.txt"): {0xff, 0xfe},
		},
	}
	muts, err := ParseMutationsWithOptions([]string{`textfile:/payload=blob.txt`}, time.Unix(1_700_000_000, 0), ParseMutationsOptions{
		EnableFileValues:  true,
		FileValueBaseDir:  "/virtual",
		FileValueResolver: resolver,
	})
	if err != nil {
		t.Fatalf("parse file-backed mutations: %v", err)
	}
	err = MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader([]byte(`{"payload":"old"}`)),
		Writer:    io.Discard,
		Mutations: muts,
	})
	if err == nil || !strings.Contains(err.Error(), "invalid UTF-8") {
		t.Fatalf("expected invalid UTF-8 error, got %v", err)
	}
}

func TestMutateStreamTextFileRejectsNUL(t *testing.T) {
	resolver := &nonAllocMutateFileResolver{
		payloads: map[string][]byte{
			filepath.Join("/virtual", "blob.txt"): {'a', 0x00, 'b'},
		},
	}
	muts, err := ParseMutationsWithOptions([]string{`textfile:/payload=blob.txt`}, time.Unix(1_700_000_000, 0), ParseMutationsOptions{
		EnableFileValues:  true,
		FileValueBaseDir:  "/virtual",
		FileValueResolver: resolver,
	})
	if err != nil {
		t.Fatalf("parse file-backed mutations: %v", err)
	}
	err = MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader([]byte(`{"payload":"old"}`)),
		Writer:    io.Discard,
		Mutations: muts,
	})
	if err == nil || !strings.Contains(err.Error(), "NUL byte") {
		t.Fatalf("expected NUL byte error, got %v", err)
	}
}

func TestMutateStreamFileBackedThenChildMutationCreatesObject(t *testing.T) {
	resolver := &nonAllocMutateFileResolver{
		payloads: map[string][]byte{
			filepath.Join("/virtual", "blob.txt"): []byte("hello"),
		},
	}
	muts, err := ParseMutationsWithOptions([]string{
		`textfile:/payload=blob.txt`,
		`/payload/version=1`,
	}, time.Unix(1_700_000_000, 0), ParseMutationsOptions{
		EnableFileValues:  true,
		FileValueBaseDir:  "/virtual",
		FileValueResolver: resolver,
	})
	if err != nil {
		t.Fatalf("parse file-backed mutations: %v", err)
	}

	var out bytes.Buffer
	err = MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader([]byte(`{"payload":"old"}`)),
		Writer:    &out,
		Mutations: muts,
	})
	if err != nil {
		t.Fatalf("mutate stream: %v", err)
	}
	doc := decodeJSONValuesForMutateTest(t, out.Bytes())[0].(map[string]any)
	payload := doc["payload"].(map[string]any)
	if payload["version"] != json.Number("1") {
		t.Fatalf("expected child mutation to replace file-backed leaf with object, got %#v", doc["payload"])
	}
}

func TestMutateStreamWriterParityWithLegacyPipeline(t *testing.T) {
	now := time.Unix(1_750_000_000, 0)
	muts, err := ParseMutations([]string{
		`/status=ready`,
		`/metrics/retries=+2`,
		`rm:/meta/legacy`,
		`/items[]/price=+3`,
		`/labels/*=tagged`,
		`/groups/.../sku="Z"`,
	}, now)
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}
	input := []byte(`
{"id":"a","status":"new","metrics":{"retries":1},"meta":{"legacy":"x"},"labels":{"env":"prod"},"items":[{"price":10}],"groups":[{"items":[{"sku":"A"}]}]}
[{"id":"b","status":"new","metrics":{"retries":3},"meta":{"legacy":"y"},"labels":{"owner":"alice"},"items":[{"price":2},{"price":8}],"groups":[{"items":[{"sku":"B"}]}]},[{"id":"c","status":"old","meta":{},"items":[]}],99]
{"id":"d","status":"new","labels":{"env":"stage"},"groups":[{"items":[{"sku":"D"}]}]}
`)

	var out bytes.Buffer
	err = MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader(input),
		Writer:    &out,
		Mutations: muts,
	})
	if err != nil {
		t.Fatalf("mutate stream writer: %v", err)
	}
	want, wantErr := collectLegacyMutationStreamValues(input, muts)
	if wantErr != nil {
		t.Fatalf("legacy parity pipeline error: %v", wantErr)
	}
	wantNDJSON, err := encodeValuesAsNDJSON(want)
	if err != nil {
		t.Fatalf("encode legacy values: %v", err)
	}
	gotDecoded, err := decodeNDJSONValues(out.Bytes())
	if err != nil {
		t.Fatalf("decode stream output: %v", err)
	}
	wantDecoded, err := decodeNDJSONValues(wantNDJSON)
	if err != nil {
		t.Fatalf("decode expected output: %v", err)
	}
	if !reflect.DeepEqual(gotDecoded, wantDecoded) {
		t.Fatalf("mutate stream writer parity mismatch:\n got: %#v\nwant: %#v", gotDecoded, wantDecoded)
	}
}

func TestMutateStreamPlanMatchesMutationsParity(t *testing.T) {
	now := time.Unix(1_750_000_000, 0)
	muts, err := ParseMutations([]string{
		`/status=ready`,
		`/metrics/retries=+2`,
		`rm:/meta/legacy`,
		`/items[]/price=+3`,
		`/labels/*=tagged`,
		`/groups/.../sku="Z"`,
	}, now)
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}
	plan, err := NewMutateStreamPlan(muts)
	if err != nil {
		t.Fatalf("new mutate plan: %v", err)
	}

	input := []byte(`
{"id":"a","status":"new","metrics":{"retries":1},"meta":{"legacy":"x"},"labels":{"env":"prod"},"items":[{"price":10}],"groups":[{"items":[{"sku":"A"}]}]}
{"id":"b","status":"new","metrics":{"retries":3},"meta":{"legacy":"y"},"labels":{"owner":"alice"},"items":[{"price":2},{"price":8}],"groups":[{"items":[{"sku":"B"}]}]}
`)

	var outMutations bytes.Buffer
	err = MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader(input),
		Writer:    &outMutations,
		Mutations: muts,
	})
	if err != nil {
		t.Fatalf("mutate stream with mutations: %v", err)
	}

	var outPlan bytes.Buffer
	err = MutateStream(MutateStreamRequest{
		Reader: bytes.NewReader(input),
		Writer: &outPlan,
		Plan:   plan,
	})
	if err != nil {
		t.Fatalf("mutate stream with plan: %v", err)
	}

	gotMutations, err := decodeNDJSONValues(outMutations.Bytes())
	if err != nil {
		t.Fatalf("decode mutations output: %v", err)
	}
	gotPlan, err := decodeNDJSONValues(outPlan.Bytes())
	if err != nil {
		t.Fatalf("decode plan output: %v", err)
	}
	if !reflect.DeepEqual(gotMutations, gotPlan) {
		t.Fatalf("mutate plan parity mismatch:\nmutations=%#v\nplan=%#v", gotMutations, gotPlan)
	}
}

func TestMutateStreamWriterPathNoCallback(t *testing.T) {
	input := []byte(`{"id":"a","status":"new"}`)
	muts, err := ParseMutations([]string{`/status=ready`}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}
	calls := 0
	var out bytes.Buffer
	err = MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader(input),
		Writer:    &out,
		Mutations: muts,
		OnValue: func(MutateStreamValue) error {
			calls++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("mutate stream: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected callback invocation, got %d", calls)
	}
}

func TestMutateStreamContextCanceledReturnsTypedError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := MutateStream(MutateStreamRequest{
		Ctx:    ctx,
		Reader: bytes.NewReader([]byte(`{"id":"a"}`)),
		Writer: io.Discard,
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorContextCanceled {
		t.Fatalf("expected context_canceled, got %s", streamErr.Code)
	}
}

func TestMutateStreamMaxCandidateBytesReturnsTypedError(t *testing.T) {
	muts, err := ParseMutations([]string{`/status=ready`}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}

	err = MutateStream(MutateStreamRequest{
		Reader:            bytes.NewReader([]byte(`{"id":"a","status":"new","payload":"abcdefghijklmnopqrstuvwxyz"}`)),
		Writer:            io.Discard,
		Mutations:         muts,
		MaxCandidateBytes: 24,
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorDocumentTooLarge {
		t.Fatalf("expected document_too_large, got %s", streamErr.Code)
	}
}

func TestMutateStreamWriterShortWriteReturnsTypedError(t *testing.T) {
	muts, err := ParseMutations([]string{`/status=ready`}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}

	err = MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader([]byte(`{"id":"a","status":"new"}`)),
		Writer:    shortWriteMutateTestWriter{},
		Mutations: muts,
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorInvalidBody {
		t.Fatalf("expected invalid_body, got %s", streamErr.Code)
	}
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("expected io.ErrShortWrite, got %v", err)
	}
}

func TestMutateStreamOnValueErrorPropagates(t *testing.T) {
	sentinel := errors.New("stop")
	err := MutateStream(MutateStreamRequest{
		Reader: bytes.NewReader([]byte(`{"id":"a","status":"new"}`)),
		OnValue: func(MutateStreamValue) error {
			return sentinel
		},
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected callback error, got %v", err)
	}
}

func TestMutateStreamCallbackSpoolUsesOpenJSONAndCleansUp(t *testing.T) {
	tempDir := t.TempDir()
	payload := []byte(`{"id":"a","status":"new","blob":"` + strings.Repeat("x", 16*1024) + `"}`)
	seen := 0
	err := MutateStream(MutateStreamRequest{
		Reader:           bytes.NewReader(payload),
		SpoolMemoryBytes: 8,
		SpoolTempDir:     tempDir,
		SpoolFilePattern: "mutate-spool-*.json",
		OnValue: func(value MutateStreamValue) error {
			seen++
			if value.JSON != nil {
				return fmt.Errorf("expected nil JSON for spooled payload")
			}
			if value.Value != nil {
				return fmt.Errorf("expected nil Value for spooled payload")
			}
			if value.OpenJSON == nil {
				return fmt.Errorf("expected OpenJSON")
			}
			rc, err := value.OpenJSON()
			if err != nil {
				return err
			}
			defer rc.Close()
			if f, ok := rc.(*os.File); ok {
				info, err := f.Stat()
				if err != nil {
					return err
				}
				if info.Mode().Perm() != 0o600 {
					return fmt.Errorf("expected 0600 spool file mode, got %o", info.Mode().Perm())
				}
			} else {
				return fmt.Errorf("expected file-backed payload reader")
			}
			got, err := io.ReadAll(rc)
			if err != nil {
				return err
			}
			decoded, err := decodeJSONAnyForMutateTest(got)
			if err != nil {
				return err
			}
			doc, ok := decoded.(map[string]any)
			if !ok {
				return fmt.Errorf("expected object payload")
			}
			if doc["id"] != "a" || doc["status"] != "new" {
				return fmt.Errorf("unexpected payload: %#v", doc)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("mutate stream: %v", err)
	}
	if seen != 1 {
		t.Fatalf("expected one callback, got %d", seen)
	}
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("read temp dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected spool cleanup, found %d files", len(entries))
	}
}

func TestMutateStreamInMemoryCallbackStillSupportsOpenJSON(t *testing.T) {
	input := []byte(`{"id":"a","status":"new"}`)
	err := MutateStream(MutateStreamRequest{
		Reader:           bytes.NewReader(input),
		SpoolMemoryBytes: 1024,
		OnValue: func(value MutateStreamValue) error {
			if value.JSON == nil {
				return fmt.Errorf("expected in-memory JSON payload")
			}
			if value.OpenJSON == nil {
				return fmt.Errorf("expected OpenJSON")
			}
			rc, err := value.OpenJSON()
			if err != nil {
				return err
			}
			defer rc.Close()
			fromReader, err := io.ReadAll(rc)
			if err != nil {
				return err
			}
			if !bytes.Equal(fromReader, value.JSON) {
				return fmt.Errorf("OpenJSON payload mismatch")
			}
			if !bytes.Equal(fromReader, value.Value) {
				return fmt.Errorf("Value payload mismatch")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("mutate stream: %v", err)
	}
}

func TestMutateStreamSingleValueOnlyDoesNotFlattenTopArray(t *testing.T) {
	var out bytes.Buffer
	err := MutateStream(MutateStreamRequest{
		Reader: bytes.NewReader([]byte(`[{"id":"a"},{"id":"b"}]`)),
		Writer: &out,
		Mode:   MutateSingleValueOnly,
	})
	if err != nil {
		t.Fatalf("mutate stream: %v", err)
	}
	values := decodeJSONValuesForMutateTest(t, out.Bytes())
	if len(values) != 1 {
		t.Fatalf("expected one top-level value, got %d", len(values))
	}
	if _, ok := values[0].([]any); !ok {
		t.Fatalf("expected top-level array output, got %T", values[0])
	}
}

func TestMutateStreamSingleValueOnlyRejectsMultipleTopLevelValues(t *testing.T) {
	err := MutateStream(MutateStreamRequest{
		Reader: bytes.NewReader([]byte(`{"id":"a"}
{"id":"b"}`)),
		Writer: io.Discard,
		Mode:   MutateSingleValueOnly,
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorInvalidBody {
		t.Fatalf("expected invalid_body, got %s", streamErr.Code)
	}
}

func TestMutateStreamObjectRootOnlyRejectsNonObjects(t *testing.T) {
	for _, input := range []string{`7`, `[{"id":"a"}]`, `null`, `"x"`} {
		err := MutateStream(MutateStreamRequest{
			Reader: bytes.NewReader([]byte(input)),
			Writer: io.Discard,
			Mode:   MutateObjectRootOnly,
		})
		var streamErr *StreamError
		if !errors.As(err, &streamErr) {
			t.Fatalf("input %q expected StreamError, got %T (%v)", input, err, err)
		}
		if streamErr.Code != StreamErrorInvalidBody {
			t.Fatalf("input %q expected invalid_body, got %s", input, streamErr.Code)
		}
	}
}

func TestMutateStreamSingleObjectOnly(t *testing.T) {
	muts, err := ParseMutations([]string{`/status=ready`}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("parse mutations: %v", err)
	}
	var out bytes.Buffer
	err = MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader([]byte(`{"id":"a","status":"new"}`)),
		Writer:    &out,
		Mutations: muts,
		Mode:      MutateSingleObjectOnly,
	})
	if err != nil {
		t.Fatalf("mutate stream: %v", err)
	}
	values := decodeJSONValuesForMutateTest(t, out.Bytes())
	if len(values) != 1 {
		t.Fatalf("expected one output value, got %d", len(values))
	}
	doc := values[0].(map[string]any)
	if doc["status"] != "ready" {
		t.Fatalf("expected mutation applied, got %+v", doc)
	}

	err = MutateStream(MutateStreamRequest{
		Reader: bytes.NewReader([]byte(`{"id":"a"}
{"id":"b"}`)),
		Writer: io.Discard,
		Mode:   MutateSingleObjectOnly,
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorInvalidBody {
		t.Fatalf("expected invalid_body, got %s", streamErr.Code)
	}
}

func TestMutateStreamUnknownMode(t *testing.T) {
	err := MutateStream(MutateStreamRequest{
		Reader: bytes.NewReader([]byte(`{"id":"a"}`)),
		Writer: io.Discard,
		Mode:   MutateStreamMode(99),
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorInvalidBody {
		t.Fatalf("expected invalid_body, got %s", streamErr.Code)
	}
}

func TestMutateStreamSingleValueOnlyRejectsEmptyInput(t *testing.T) {
	err := MutateStream(MutateStreamRequest{
		Reader: bytes.NewReader(nil),
		Writer: io.Discard,
		Mode:   MutateSingleValueOnly,
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorInvalidBody {
		t.Fatalf("expected invalid_body, got %s", streamErr.Code)
	}
}

func TestMutateStreamModeCrossProductStrictWithMaxBytesAndShortWrite(t *testing.T) {
	cases := []struct {
		name      string
		mode      MutateStreamMode
		input     string
		maxBytes  int64
		writer    io.Writer
		wantCode  StreamErrorCode
		wantCause error
	}{
		{
			name:      "single-object-too-large",
			mode:      MutateSingleObjectOnly,
			input:     `{"id":"a","payload":"abcdefghijklmnopqrstuvwxyz"}`,
			maxBytes:  16,
			writer:    io.Discard,
			wantCode:  StreamErrorDocumentTooLarge,
			wantCause: nil,
		},
		{
			name:      "single-value-array-too-large-no-flatten",
			mode:      MutateSingleValueOnly,
			input:     `[{"id":"a"},{"id":"b"}]`,
			maxBytes:  8,
			writer:    io.Discard,
			wantCode:  StreamErrorDocumentTooLarge,
			wantCause: nil,
		},
		{
			name:      "object-root-short-write",
			mode:      MutateObjectRootOnly,
			input:     `{"id":"a"}`,
			maxBytes:  0,
			writer:    shortWriteMutateTestWriter{},
			wantCode:  StreamErrorInvalidBody,
			wantCause: io.ErrShortWrite,
		},
		{
			name:      "single-object-short-write",
			mode:      MutateSingleObjectOnly,
			input:     `{"id":"a"}`,
			maxBytes:  0,
			writer:    shortWriteMutateTestWriter{},
			wantCode:  StreamErrorInvalidBody,
			wantCause: io.ErrShortWrite,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := MutateStream(MutateStreamRequest{
				Reader:            bytes.NewReader([]byte(tc.input)),
				Writer:            tc.writer,
				Mode:              tc.mode,
				MaxCandidateBytes: tc.maxBytes,
			})
			var streamErr *StreamError
			if !errors.As(err, &streamErr) {
				t.Fatalf("expected StreamError, got %T (%v)", err, err)
			}
			if streamErr.Code != tc.wantCode {
				t.Fatalf("expected code %s, got %s", tc.wantCode, streamErr.Code)
			}
			if tc.wantCause != nil && !errors.Is(err, tc.wantCause) {
				t.Fatalf("expected cause %v, got %v", tc.wantCause, err)
			}
		})
	}
}

type shortWriteMutateTestWriter struct{}

func (shortWriteMutateTestWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

func collectMutateStreamValues(input []byte, muts []Mutation) ([]any, error) {
	values := make([]any, 0, 8)
	err := MutateStream(MutateStreamRequest{
		Reader:    bytes.NewReader(input),
		Mutations: muts,
		OnValue: func(value MutateStreamValue) error {
			payload, err := readMutateStreamValuePayload(value)
			if err != nil {
				return err
			}
			decoded, err := decodeJSONAnyForMutateTest(payload)
			if err != nil {
				return err
			}
			values = append(values, decoded)
			return nil
		},
	})
	return values, err
}

func readMutateStreamValuePayload(value MutateStreamValue) ([]byte, error) {
	if value.JSON != nil {
		return value.JSON, nil
	}
	if value.OpenJSON == nil {
		return nil, fmt.Errorf("mutate stream payload unavailable")
	}
	rc, err := value.OpenJSON()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func collectLegacyMutationStreamValues(input []byte, muts []Mutation) ([]any, error) {
	values := make([]any, 0, 8)
	err := QueryStream(QueryStreamRequest{
		Reader:      bytes.NewReader(input),
		Selector:    Selector{},
		IncludeJSON: true,
		OnValue: func(value QueryStreamValue) error {
			decoded, err := decodeJSONAnyForMutateTest(value.JSON)
			if err != nil {
				return err
			}
			if doc, ok := decoded.(map[string]any); ok {
				if err := ApplyMutations(doc, muts); err != nil {
					return err
				}
			}
			values = append(values, decoded)
			return nil
		},
	})
	return values, err
}

func decodeJSONAnyForMutateTest(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	err := dec.Decode(&trailing)
	if err == io.EOF {
		return value, nil
	}
	if err != nil {
		return nil, err
	}
	return nil, io.ErrUnexpectedEOF
}

func decodeJSONValuesForMutateTest(t *testing.T, payload []byte) []any {
	t.Helper()
	values, err := decodeNDJSONValues(payload)
	if err != nil {
		t.Fatalf("decode output: %v", err)
	}
	return values
}

func encodeValuesAsNDJSON(values []any) ([]byte, error) {
	var out bytes.Buffer
	for i, value := range values {
		payload, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("marshal value %d: %w", i, err)
		}
		if _, err := out.Write(payload); err != nil {
			return nil, err
		}
		if err := out.WriteByte('\n'); err != nil {
			return nil, err
		}
	}
	return out.Bytes(), nil
}

func decodeNDJSONValues(payload []byte) ([]any, error) {
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	values := make([]any, 0, 8)
	for {
		var value any
		if err := dec.Decode(&value); err != nil {
			if err == io.EOF {
				return values, nil
			}
			return nil, err
		}
		values = append(values, value)
	}
}

type mutateTestFileResolver struct {
	payloads map[string][]byte
	opens    map[string]int
}

func (r *mutateTestFileResolver) Open(path string) (io.ReadCloser, error) {
	if r.opens == nil {
		r.opens = make(map[string]int)
	}
	payload, ok := r.payloads[path]
	if !ok {
		return nil, fmt.Errorf("unknown test payload %q", path)
	}
	r.opens[path]++
	return io.NopCloser(bytes.NewReader(payload)), nil
}
