package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"pkt.systems/lql"
)

func TestBuildSelectorAND(t *testing.T) {
	sel, err := buildSelector([]string{`/status="open"`, `/progress>=50`}, false)
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}
	if len(sel.Or) != 0 {
		t.Fatalf("expected no OR clauses, got %d", len(sel.Or))
	}
	if !lql.Matches(sel, map[string]any{"status": "open", "progress": 72}) {
		t.Fatalf("expected selector to match")
	}
	if lql.Matches(sel, map[string]any{"status": "open", "progress": 10}) {
		t.Fatalf("expected selector to reject low progress")
	}
}

func TestWriteQueryStreamValueFromOpenJSON(t *testing.T) {
	var out bytes.Buffer
	enc := newOutputEncoder(&out, config{compact: true})
	err := writeQueryStreamValue(enc, lql.QueryStreamValue{
		OpenJSON: func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte(`{"id":"a","status":"ok"}`))), nil
		},
	})
	if err != nil {
		t.Fatalf("writeQueryStreamValue: %v", err)
	}
	values := decodeJSONValues(t, out.Bytes())
	if len(values) != 1 {
		t.Fatalf("expected single JSON value, got %d", len(values))
	}
	doc := values[0].(map[string]any)
	if doc["id"] != "a" || doc["status"] != "ok" {
		t.Fatalf("unexpected payload: %#v", doc)
	}
}

func TestMutateAndWriteQueryStreamValueFromOpenJSON(t *testing.T) {
	muts, err := lql.ParseMutations([]string{`/status=ready`}, time.Unix(1_750_000_000, 0))
	if err != nil {
		t.Fatalf("ParseMutations: %v", err)
	}

	var out bytes.Buffer
	enc := newOutputEncoder(&out, config{compact: true})
	err = mutateAndWriteQueryStreamValue(enc, lql.QueryStreamValue{
		OpenJSON: func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte(`{"id":"a","status":"new"}`))), nil
		},
	}, muts)
	if err != nil {
		t.Fatalf("mutateAndWriteQueryStreamValue: %v", err)
	}

	values := decodeJSONValues(t, out.Bytes())
	if len(values) != 1 {
		t.Fatalf("expected one mutated value, got %d", len(values))
	}
	doc := values[0].(map[string]any)
	if doc["status"] != "ready" {
		t.Fatalf("expected mutated status, got %#v", doc["status"])
	}
}

func TestMutateAndWriteQueryStreamValueFromOpenJSONArrayCandidate(t *testing.T) {
	var out bytes.Buffer
	enc := newOutputEncoder(&out, config{compact: true})
	err := mutateAndWriteQueryStreamValue(enc, lql.QueryStreamValue{
		OpenJSON: func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte(`[{"id":"a","status":"new"},{"id":"b","status":"new"}]`))), nil
		},
	}, nil)
	if err != nil {
		t.Fatalf("mutateAndWriteQueryStreamValue: %v", err)
	}

	values := decodeJSONValues(t, out.Bytes())
	if len(values) != 1 {
		t.Fatalf("expected one mutated value, got %d", len(values))
	}
	items, ok := values[0].([]any)
	if !ok {
		t.Fatalf("expected top-level array output, got %T", values[0])
	}
	if len(items) != 2 {
		t.Fatalf("expected two array items, got %d", len(items))
	}
	first := items[0].(map[string]any)
	second := items[1].(map[string]any)
	if first["status"] != "new" {
		t.Fatalf("expected first status unchanged, got %#v", first["status"])
	}
	if second["status"] != "new" {
		t.Fatalf("expected second status unchanged, got %#v", second["status"])
	}
}

func TestProjectQueryStreamValueFromOpenJSON(t *testing.T) {
	fields, err := parseFieldPaths([]string{"/id"})
	if err != nil {
		t.Fatalf("parseFieldPaths: %v", err)
	}

	projected, found, err := projectQueryStreamValue(lql.QueryStreamValue{
		OpenJSON: func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte(`{"id":"a","status":"new","blob":"` + strings.Repeat("x", 2048) + `"}`))), nil
		},
	}, fields, nil)
	if err != nil {
		t.Fatalf("projectQueryStreamValue: %v", err)
	}
	if !found {
		t.Fatalf("expected projection to find /id")
	}
	values := decodeJSONValues(t, projected)
	if len(values) != 1 {
		t.Fatalf("expected one value, got %d", len(values))
	}
	doc := values[0].(map[string]any)
	if doc["id"] != "a" {
		t.Fatalf("expected id=a, got %#v", doc["id"])
	}
	if _, ok := doc["status"]; ok {
		t.Fatalf("expected projected value to exclude status, got %#v", doc)
	}
}

func TestBuildSelectorOR(t *testing.T) {
	sel, err := buildSelector([]string{`/status="open"`, `/status="queued"`}, true)
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}
	if len(sel.Or) != 2 {
		t.Fatalf("expected 2 OR clauses, got %d", len(sel.Or))
	}
	if len(sel.And) != 0 {
		t.Fatalf("expected no AND clauses, got %d", len(sel.And))
	}
	if !lql.Matches(sel, map[string]any{"status": "queued"}) {
		t.Fatalf("expected selector to match queued")
	}
	if lql.Matches(sel, map[string]any{"status": "closed"}) {
		t.Fatalf("expected selector to reject closed")
	}
}

func TestBuildSelectorAndOrMix(t *testing.T) {
	args := []string{
		`/status=404`,
		`/lvl="warn"`,
		`or.eq{field=/domain,value=pkt.systems},or.eq{field=/domain,value=qzj.se}`,
	}
	sel, err := buildSelector(args, false)
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}
	matchPkt := map[string]any{"status": 404, "lvl": "warn", "domain": "pkt.systems"}
	if !lql.Matches(sel, matchPkt) {
		t.Fatalf("expected selector to match pkt.systems")
	}
	matchQzj := map[string]any{"status": 404, "lvl": "warn", "domain": "qzj.se"}
	if !lql.Matches(sel, matchQzj) {
		t.Fatalf("expected selector to match qzj.se")
	}
	noMatchDomain := map[string]any{"status": 404, "lvl": "warn", "domain": "alltsomkod.se"}
	if lql.Matches(sel, noMatchDomain) {
		t.Fatalf("expected selector to reject other domains")
	}
	noMatchStatus := map[string]any{"status": 500, "lvl": "warn", "domain": "pkt.systems"}
	if lql.Matches(sel, noMatchStatus) {
		t.Fatalf("expected selector to reject wrong status")
	}
}

func TestBuildSelectorContainsVariants(t *testing.T) {
	sel, err := buildSelector([]string{
		`icontains{field=/msg,value=timeout}`,
		`iprefix{field=/service,value=auth}`,
	}, false)
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}
	if !lql.Matches(sel, map[string]any{
		"msg":     "Error: TIMEOUT",
		"service": "Auth-API",
	}) {
		t.Fatalf("expected selector to match case-insensitive terms")
	}
	if lql.Matches(sel, map[string]any{
		"msg":     "Error: TIMEOUT",
		"service": "billing-api",
	}) {
		t.Fatalf("expected selector to reject non-matching prefix")
	}
}

func TestEmitSelectionStreamWildcards(t *testing.T) {
	selector, err := buildSelector([]string{`/items[]/sku="B"`}, false)
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}

	payload := `[{"id":"a","items":[{"sku":"A"}]},{"id":"b","items":[{"sku":"B"}]}]`
	tmp, err := os.CreateTemp("", "lql-cli-select-*.json")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(payload); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp: %v", err)
	}

	origStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = writePipe
	err = runSelections(config{compact: true}, selector, nil, tmp.Name())
	writePipe.Close()
	os.Stdout = origStdout
	if err != nil {
		t.Fatalf("runSelections: %v", err)
	}
	output, err := io.ReadAll(readPipe)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	values := decodeJSONValues(t, output)
	if len(values) != 1 {
		t.Fatalf("expected 1 match, got %d", len(values))
	}
	matchedDoc := values[0].(map[string]any)
	if matchedDoc["id"] != "b" {
		t.Fatalf("expected id b, got %+v", matchedDoc["id"])
	}
}

func TestRunMutationsWildcards(t *testing.T) {
	doc := `{"items":[{"sku":"A","status":"new"},{"sku":"B","status":"new"}],"labels":{"env":"prod"}}`
	tmp, err := os.CreateTemp("", "lql-cli-*.json")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(doc); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp: %v", err)
	}

	cfg := config{
		mutations: stringList{
			`/items[]/status=ready`,
			`/labels/*="tagged"`,
		},
		compact: true,
	}
	selector, err := buildSelector([]string{`/items[]/sku="B"`}, false)
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}

	origStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = writePipe
	err = runMutations(cfg, selector, nil, []string{tmp.Name()})
	writePipe.Close()
	os.Stdout = origStdout
	if err != nil {
		t.Fatalf("runMutations: %v", err)
	}

	output, err := io.ReadAll(readPipe)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	values := decodeJSONValues(t, output)
	if len(values) != 1 {
		t.Fatalf("expected 1 output doc, got %d", len(values))
	}
	updated := values[0].(map[string]any)
	items := updated["items"].([]any)
	for _, item := range items {
		entry := item.(map[string]any)
		if entry["status"] != "ready" {
			t.Fatalf("expected status ready, got %+v", entry["status"])
		}
	}
	labels := updated["labels"].(map[string]any)
	if labels["env"] != "tagged" {
		t.Fatalf("expected labels tagged, got %+v", labels)
	}
}

func TestRunMutationsStreamMultipleObjects(t *testing.T) {
	doc := `{"id":"a","status":"new"}
{"id":"b","status":"new"}`
	tmp, err := os.CreateTemp("", "lql-cli-*.json")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(doc); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp: %v", err)
	}

	cfg := config{
		mutations: stringList{
			`/status=ready`,
		},
		compact: true,
	}
	selector, err := buildSelector([]string{`/id="b"`}, false)
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}

	origStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = writePipe
	err = runMutations(cfg, selector, nil, []string{tmp.Name()})
	writePipe.Close()
	os.Stdout = origStdout
	if err != nil {
		t.Fatalf("runMutations: %v", err)
	}

	output, err := io.ReadAll(readPipe)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	values := decodeJSONValues(t, output)
	if len(values) != 2 {
		t.Fatalf("expected 2 output docs, got %d", len(values))
	}
	docA := values[0].(map[string]any)
	docB := values[1].(map[string]any)
	if docA["id"] != "a" || docA["status"] != "new" {
		t.Fatalf("expected first doc unchanged, got %+v", docA)
	}
	if docB["id"] != "b" || docB["status"] != "ready" {
		t.Fatalf("expected second doc mutated, got %+v", docB)
	}
}

func TestRunMutationsFileBackedMutationsRequireEnableFlag(t *testing.T) {
	cfg := config{
		mutations: stringList{`file:/payload=blob.txt`},
		compact:   true,
	}
	err := runMutations(cfg, lql.Selector{}, nil, []string{"-"})
	if err == nil || !strings.Contains(err.Error(), "file-backed mutations are disabled") {
		t.Fatalf("expected file-backed mutations disabled error, got %v", err)
	}
}

func TestRunMutationsFileBackedMutationsEnabled(t *testing.T) {
	tempDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tempDir, "blob.txt"), []byte("hello from file"), 0o600); err != nil {
		t.Fatalf("write blob: %v", err)
	}
	inputPath := filepath.Join(tempDir, "input.json")
	if err := os.WriteFile(inputPath, []byte(`{"id":"a"}`), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	origWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	defer os.Chdir(origWD)

	cfg := config{
		mutations:           stringList{`textfile:/payload=blob.txt`},
		compact:             true,
		enableFileMutations: true,
	}

	origStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = writePipe
	err = runMutations(cfg, lql.Selector{}, nil, []string{inputPath})
	writePipe.Close()
	os.Stdout = origStdout
	if err != nil {
		t.Fatalf("runMutations: %v", err)
	}

	output, err := io.ReadAll(readPipe)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	values := decodeJSONValues(t, output)
	doc := values[0].(map[string]any)
	if doc["payload"] != "hello from file" {
		t.Fatalf("unexpected file-backed payload: %#v", doc["payload"])
	}
}

func TestRunMutationsFileBackedMutationsExpandHomePath(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	if err := os.WriteFile(filepath.Join(homeDir, "blob.txt"), []byte("hello from home"), 0o600); err != nil {
		t.Fatalf("write blob: %v", err)
	}
	inputPath := filepath.Join(t.TempDir(), "input.json")
	if err := os.WriteFile(inputPath, []byte(`{"id":"a"}`), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	cfg := config{
		mutations:           stringList{`textfile:/payload=~/blob.txt`},
		compact:             true,
		enableFileMutations: true,
	}

	origStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = writePipe
	err = runMutations(cfg, lql.Selector{}, nil, []string{inputPath})
	writePipe.Close()
	os.Stdout = origStdout
	if err != nil {
		t.Fatalf("runMutations: %v", err)
	}

	output, err := io.ReadAll(readPipe)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	values := decodeJSONValues(t, output)
	doc := values[0].(map[string]any)
	if doc["payload"] != "hello from home" {
		t.Fatalf("unexpected home-expanded file-backed payload: %#v", doc["payload"])
	}
}

func TestPrintUsageIncludesFileMutationShortFlag(t *testing.T) {
	var out bytes.Buffer
	printUsage(&out)

	usage := out.String()
	if !strings.Contains(usage, "-F, --enable-file-mutations") {
		t.Fatalf("expected usage to include -F shorthand, got %q", usage)
	}
	if !strings.Contains(usage, "printf '{}\\n' | lql -F \\") {
		t.Fatalf("expected usage example to include -F shorthand, got %q", usage)
	}
}

func TestRunMutationsFieldsEmptyOutput(t *testing.T) {
	doc := `{"id":"a"}
{"id":"b"}`
	tmp, err := os.CreateTemp("", "lql-cli-*.json")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(doc); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp: %v", err)
	}

	cfg := config{
		mutations: stringList{
			`/noop=1`,
		},
		compact: true,
	}
	selector, err := buildSelector(nil, false)
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}
	fields, err := parseFieldPaths([]string{"/missing"})
	if err != nil {
		t.Fatalf("parseFieldPaths: %v", err)
	}

	origStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = writePipe
	err = runMutations(cfg, selector, fields, []string{tmp.Name()})
	writePipe.Close()
	os.Stdout = origStdout
	if err != nil {
		t.Fatalf("runMutations: %v", err)
	}

	output, err := io.ReadAll(readPipe)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	values := decodeJSONValues(t, output)
	if len(values) != 0 {
		t.Fatalf("expected no output docs, got %d", len(values))
	}
}

func TestRunMutationsFieldsDropMissing(t *testing.T) {
	doc := `{"status":404}
{"status":200,"uri":"/ok"}`
	tmp, err := os.CreateTemp("", "lql-cli-*.json")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(doc); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp: %v", err)
	}

	cfg := config{
		mutations: stringList{
			`/hello=world`,
		},
		compact: true,
	}
	selector, err := buildSelector([]string{`/status=404`}, false)
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}
	fields, err := parseFieldPaths([]string{"/uri"})
	if err != nil {
		t.Fatalf("parseFieldPaths: %v", err)
	}

	origStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = writePipe
	err = runMutations(cfg, selector, fields, []string{tmp.Name()})
	writePipe.Close()
	os.Stdout = origStdout
	if err != nil {
		t.Fatalf("runMutations: %v", err)
	}

	output, err := io.ReadAll(readPipe)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	values := decodeJSONValues(t, output)
	if len(values) != 1 {
		t.Fatalf("expected 1 output doc, got %d", len(values))
	}
	docOut := values[0].(map[string]any)
	if docOut["uri"] != "/ok" {
		t.Fatalf("expected surviving doc with uri, got %+v", docOut)
	}
	if _, ok := docOut["hello"]; ok {
		t.Fatalf("expected no hello for non-matching doc, got %+v", docOut)
	}
}

func TestRunMutationsFieldsSelectorGatesMutation(t *testing.T) {
	doc := `{"uri":"/a","status":404}
{"uri":"/b","status":200}`
	tmp, err := os.CreateTemp("", "lql-cli-*.json")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(doc); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp: %v", err)
	}

	cfg := config{
		mutations: stringList{
			`/hello=world`,
		},
		compact: true,
	}
	selector, err := buildSelector([]string{`/status=404`}, false)
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}
	fields, err := parseFieldPaths([]string{"/uri"})
	if err != nil {
		t.Fatalf("parseFieldPaths: %v", err)
	}

	origStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = writePipe
	err = runMutations(cfg, selector, fields, []string{tmp.Name()})
	writePipe.Close()
	os.Stdout = origStdout
	if err != nil {
		t.Fatalf("runMutations: %v", err)
	}

	output, err := io.ReadAll(readPipe)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	values := decodeJSONValues(t, output)
	if len(values) != 2 {
		t.Fatalf("expected 2 output docs, got %d", len(values))
	}
	docA := values[0].(map[string]any)
	docB := values[1].(map[string]any)
	if docA["uri"] != "/a" || docA["hello"] != "world" {
		t.Fatalf("expected first doc mutated, got %+v", docA)
	}
	if docB["uri"] != "/b" {
		t.Fatalf("expected second doc unchanged, got %+v", docB)
	}
	if _, ok := docB["hello"]; ok {
		t.Fatalf("expected no hello in second doc, got %+v", docB)
	}
}

func TestRunSelectionsFieldsDropAllOutputsNothing(t *testing.T) {
	doc := `{"id":"a"}
{"id":"b"}`
	tmp, err := os.CreateTemp("", "lql-cli-*.json")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(doc); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp: %v", err)
	}

	cfg := config{compact: true}
	selector, err := buildSelector(nil, false)
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}
	fields, err := parseFieldPaths([]string{"/missing"})
	if err != nil {
		t.Fatalf("parseFieldPaths: %v", err)
	}

	origStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = writePipe
	err = runSelections(cfg, selector, fields, tmp.Name())
	writePipe.Close()
	os.Stdout = origStdout
	if err != nil {
		t.Fatalf("runSelections: %v", err)
	}

	output, err := io.ReadAll(readPipe)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	values := decodeJSONValues(t, output)
	if len(values) != 0 {
		t.Fatalf("expected no output docs, got %d", len(values))
	}
}

func TestRunMutationsInlineFieldsDropAllOutputsNothing(t *testing.T) {
	doc := `{"id":"a"}
{"id":"b"}`
	tmp, err := os.CreateTemp("", "lql-cli-inline-*.json")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(doc); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp: %v", err)
	}

	cfg := config{
		mutations: stringList{`/noop=1`},
		inline:    true,
		compact:   true,
	}
	selector, err := buildSelector(nil, false)
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}
	fields, err := parseFieldPaths([]string{"/missing"})
	if err != nil {
		t.Fatalf("parseFieldPaths: %v", err)
	}

	if err := runMutations(cfg, selector, fields, []string{tmp.Name()}); err != nil {
		t.Fatalf("runMutations: %v", err)
	}

	updated, err := os.ReadFile(tmp.Name())
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	values := decodeJSONValues(t, updated)
	if len(values) != 0 {
		t.Fatalf("expected no output docs, got %d", len(values))
	}
}

func TestRunMutationsMatchesOnly(t *testing.T) {
	doc := `{"uri":"/a","status":404}
{"uri":"/b","status":200}`
	tmp, err := os.CreateTemp("", "lql-cli-*.json")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(doc); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp: %v", err)
	}

	cfg := config{
		mutations:   stringList{`/hello=world`},
		matchesOnly: true,
		compact:     true,
	}
	selector, err := buildSelector([]string{`/status=404`}, false)
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}
	fields, err := parseFieldPaths([]string{"/uri"})
	if err != nil {
		t.Fatalf("parseFieldPaths: %v", err)
	}

	origStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = writePipe
	err = runMutations(cfg, selector, fields, []string{tmp.Name()})
	writePipe.Close()
	os.Stdout = origStdout
	if err != nil {
		t.Fatalf("runMutations: %v", err)
	}

	output, err := io.ReadAll(readPipe)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	values := decodeJSONValues(t, output)
	if len(values) != 1 {
		t.Fatalf("expected 1 output doc, got %d", len(values))
	}
	docOut := values[0].(map[string]any)
	if docOut["uri"] != "/a" || docOut["hello"] != "world" {
		t.Fatalf("expected mutated output, got %+v", docOut)
	}
}

func TestSelectionsFlagOrderInterspersed(t *testing.T) {
	doc := `{"id":"a","status":"open"}
{"id":"b","status":"closed"}`
	tmp, err := os.CreateTemp("", "lql-cli-*.json")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(doc); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp: %v", err)
	}

	run := func(args []string) []any {
		t.Helper()
		origArgs := os.Args
		origStdout := os.Stdout
		readPipe, writePipe, err := os.Pipe()
		if err != nil {
			t.Fatalf("os.Pipe: %v", err)
		}
		os.Args = append([]string{"lql"}, args...)
		os.Stdout = writePipe
		main()
		writePipe.Close()
		os.Stdout = origStdout
		os.Args = origArgs

		output, readErr := io.ReadAll(readPipe)
		if readErr != nil {
			t.Fatalf("read output: %v", readErr)
		}
		return decodeJSONValues(t, output)
	}

	argsBefore := []string{`/status="open"`, "-f", "/id", tmp.Name()}
	argsAfter := []string{"-f", "/id", `/status="open"`, tmp.Name()}
	valuesBefore := run(argsBefore)
	valuesAfter := run(argsAfter)
	if len(valuesBefore) != 1 {
		t.Fatalf("expected 1 output doc, got %d", len(valuesBefore))
	}
	if len(valuesAfter) != 1 {
		t.Fatalf("expected 1 output doc, got %d", len(valuesAfter))
	}
	docBefore := valuesBefore[0].(map[string]any)
	docAfter := valuesAfter[0].(map[string]any)
	if docBefore["id"] != "a" || docAfter["id"] != "a" {
		t.Fatalf("expected id a, got %+v %+v", docBefore, docAfter)
	}
}

func TestRunMutationsSelectorEmptyFastPathHandlesMixedStream(t *testing.T) {
	doc := `1
{"id":"a","status":"new"}
[{"id":"b","status":"new"},3]`
	tmp, err := os.CreateTemp("", "lql-cli-*.json")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(doc); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close temp: %v", err)
	}

	cfg := config{
		mutations: stringList{`/status=ready`},
		compact:   true,
	}
	selector, err := buildSelector(nil, false)
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}

	origStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = writePipe
	err = runMutations(cfg, selector, nil, []string{tmp.Name()})
	writePipe.Close()
	os.Stdout = origStdout
	if err != nil {
		t.Fatalf("runMutations: %v", err)
	}

	output, err := io.ReadAll(readPipe)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	values := decodeJSONValues(t, output)
	if len(values) != 4 {
		t.Fatalf("expected 4 output values, got %d", len(values))
	}
	if values[0] != json.Number("1") {
		t.Fatalf("expected first scalar unchanged, got %#v", values[0])
	}
	docA := values[1].(map[string]any)
	docB := values[2].(map[string]any)
	if docA["status"] != "ready" || docB["status"] != "ready" {
		t.Fatalf("expected object statuses mutated, got %+v %+v", docA, docB)
	}
	if values[3] != json.Number("3") {
		t.Fatalf("expected trailing scalar unchanged, got %#v", values[3])
	}
}

func TestSplitArgsVariants(t *testing.T) {
	fileA, err := os.CreateTemp("", "lql-cli-split-a-*.json")
	if err != nil {
		t.Fatalf("CreateTemp fileA: %v", err)
	}
	fileB, err := os.CreateTemp("", "lql-cli-split-b-*.json")
	if err != nil {
		t.Fatalf("CreateTemp fileB: %v", err)
	}
	defer os.Remove(fileA.Name())
	defer os.Remove(fileB.Name())
	_ = fileA.Close()
	_ = fileB.Close()

	dir := t.TempDir()
	cases := []struct {
		name          string
		args          []string
		mutating      bool
		wantSelectors []string
		wantInput     string
		wantErrSubstr string
	}{
		{
			name:          "empty args",
			args:          nil,
			mutating:      false,
			wantSelectors: nil,
			wantInput:     "",
		},
		{
			name:          "stdin marker as input",
			args:          []string{`/status="open"`, "-"},
			mutating:      false,
			wantSelectors: []string{`/status="open"`},
			wantInput:     "-",
		},
		{
			name:          "file as trailing input",
			args:          []string{`/status="open"`, fileA.Name()},
			mutating:      false,
			wantSelectors: []string{`/status="open"`},
			wantInput:     fileA.Name(),
		},
		{
			name:          "directory is not treated as file input",
			args:          []string{`/status="open"`, dir},
			mutating:      false,
			wantSelectors: []string{`/status="open"`, dir},
			wantInput:     "",
		},
		{
			name:          "mutating rejects multiple files",
			args:          []string{fileA.Name(), fileB.Name()},
			mutating:      true,
			wantSelectors: nil,
			wantInput:     "",
			wantErrSubstr: "mutation input accepts a single JSON file",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			gotSelectors, gotInput, err := splitArgs(tc.args, tc.mutating)
			if tc.wantErrSubstr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q", tc.wantErrSubstr)
				}
				if !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("expected error containing %q, got %q", tc.wantErrSubstr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("splitArgs returned error: %v", err)
			}
			if !reflect.DeepEqual(gotSelectors, tc.wantSelectors) {
				t.Fatalf("selectors mismatch: got=%#v want=%#v", gotSelectors, tc.wantSelectors)
			}
			if gotInput != tc.wantInput {
				t.Fatalf("input mismatch: got=%q want=%q", gotInput, tc.wantInput)
			}
		})
	}
}

func TestSplitMutationArgsVariants(t *testing.T) {
	fileA, err := os.CreateTemp("", "lql-cli-splitm-a-*.json")
	if err != nil {
		t.Fatalf("CreateTemp fileA: %v", err)
	}
	fileB, err := os.CreateTemp("", "lql-cli-splitm-b-*.json")
	if err != nil {
		t.Fatalf("CreateTemp fileB: %v", err)
	}
	defer os.Remove(fileA.Name())
	defer os.Remove(fileB.Name())
	_ = fileA.Close()
	_ = fileB.Close()

	cases := []struct {
		name          string
		args          []string
		inline        bool
		wantSelectors []string
		wantInputs    []string
		wantErrSubstr string
	}{
		{
			name:          "selectors and file",
			args:          []string{`/status=404`, fileA.Name()},
			inline:        false,
			wantSelectors: []string{`/status=404`},
			wantInputs:    []string{fileA.Name()},
		},
		{
			name:          "stdin input",
			args:          []string{`/status=404`, "-"},
			inline:        false,
			wantSelectors: []string{`/status=404`},
			wantInputs:    []string{"-"},
		},
		{
			name:          "inline requires file path",
			args:          []string{`/status=404`},
			inline:        true,
			wantErrSubstr: "inline mode requires a file path",
		},
		{
			name:          "inline rejects stdin",
			args:          []string{"-"},
			inline:        true,
			wantErrSubstr: "inline mode requires a single JSON file",
		},
		{
			name:          "inline rejects multiple files",
			args:          []string{fileA.Name(), fileB.Name()},
			inline:        true,
			wantErrSubstr: "inline mode requires a single JSON file",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			gotSelectors, gotInputs, err := splitMutationArgs(tc.args, tc.inline)
			if tc.wantErrSubstr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q", tc.wantErrSubstr)
				}
				if !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("expected error containing %q, got %q", tc.wantErrSubstr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("splitMutationArgs returned error: %v", err)
			}
			if !reflect.DeepEqual(gotSelectors, tc.wantSelectors) {
				t.Fatalf("selectors mismatch: got=%#v want=%#v", gotSelectors, tc.wantSelectors)
			}
			if !reflect.DeepEqual(gotInputs, tc.wantInputs) {
				t.Fatalf("inputs mismatch: got=%#v want=%#v", gotInputs, tc.wantInputs)
			}
		})
	}
}

func TestValidateThemeInvalid(t *testing.T) {
	err := validateTheme("definitely-not-a-theme")
	if err == nil {
		t.Fatalf("expected validateTheme to reject unknown theme")
	}
	if !strings.Contains(err.Error(), "unknown theme") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWriteInlineMutationsCreateTempFailure(t *testing.T) {
	muts, err := lql.ParseMutations([]string{`/status=ready`}, time.Unix(1_750_000_000, 0))
	if err != nil {
		t.Fatalf("ParseMutations: %v", err)
	}
	path := filepath.Join(t.TempDir(), "missing-dir", "doc.json")
	err = writeInlineMutations(path, config{compact: true}, lql.Selector{}, nil, muts)
	if err == nil {
		t.Fatalf("expected create temp failure")
	}
}

func TestWriteInlineMutationsNoJSONInput(t *testing.T) {
	file, err := os.CreateTemp("", "lql-cli-inline-empty-*.json")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(file.Name())
	if err := file.Close(); err != nil {
		t.Fatalf("close temp: %v", err)
	}

	muts, err := lql.ParseMutations([]string{`/status=ready`}, time.Unix(1_750_000_000, 0))
	if err != nil {
		t.Fatalf("ParseMutations: %v", err)
	}
	err = writeInlineMutations(file.Name(), config{compact: true}, lql.Selector{}, nil, muts)
	if err == nil {
		t.Fatalf("expected no JSON input error")
	}
	if !strings.Contains(err.Error(), "no JSON input") {
		t.Fatalf("expected no JSON input error, got %v", err)
	}
}

func TestWriteInlineMutationsInvalidJSON(t *testing.T) {
	file, err := os.CreateTemp("", "lql-cli-inline-invalid-*.json")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(file.Name())
	if _, err := file.WriteString(`{"id":`); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close temp: %v", err)
	}

	muts, err := lql.ParseMutations([]string{`/status=ready`}, time.Unix(1_750_000_000, 0))
	if err != nil {
		t.Fatalf("ParseMutations: %v", err)
	}
	err = writeInlineMutations(file.Name(), config{compact: true}, lql.Selector{}, nil, muts)
	if err == nil {
		t.Fatalf("expected invalid JSON error")
	}
}

func decodeJSONValues(t *testing.T, payload []byte) []any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	var values []any
	for {
		var val any
		if err := dec.Decode(&val); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("decode output: %v", err)
		}
		values = append(values, val)
	}
	return values
}
