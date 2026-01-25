package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"testing"

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

func TestEmitSelectionStreamWildcards(t *testing.T) {
	selector, err := buildSelector([]string{`/items[]/sku="B"`}, false)
	if err != nil {
		t.Fatalf("buildSelector: %v", err)
	}

	input := []any{
		map[string]any{
			"id": "a",
			"items": []any{
				map[string]any{"sku": "A"},
			},
		},
		map[string]any{
			"id": "b",
			"items": []any{
				map[string]any{"sku": "B"},
			},
		},
	}

	var buf bytes.Buffer
	enc := newOutputEncoder(&buf, config{compact: true})
	if err := emitSelectionStream(enc, selector, nil, input); err != nil {
		t.Fatalf("emitSelectionStream: %v", err)
	}

	values := decodeJSONValues(t, buf.Bytes())
	if len(values) != 1 {
		t.Fatalf("expected 1 match, got %d", len(values))
	}
	doc := values[0].(map[string]any)
	if doc["id"] != "b" {
		t.Fatalf("expected id b, got %+v", doc["id"])
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
		mutations: stringList{`/hello=world`},
		matchesOnly: true,
		compact:   true,
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
