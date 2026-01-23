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
	err = runMutations(cfg, selector, nil, tmp.Name())
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
