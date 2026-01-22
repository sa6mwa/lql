package main

import (
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
