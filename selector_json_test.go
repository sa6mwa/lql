package lql

import (
	"encoding/json"
	"testing"
)

func TestTermMarshalJSONOmittedValueDoesNotEmitValueField(t *testing.T) {
	cases := []struct {
		name    string
		expr    string
		termKey string
	}{
		{name: "contains", expr: `contains{f=/hello/world}`, termKey: "contains"},
		{name: "icontains", expr: `icontains{f=/hello/world}`, termKey: "icontains"},
		{name: "prefix", expr: `prefix{f=/hello/world}`, termKey: "prefix"},
		{name: "iprefix", expr: `iprefix{f=/hello/world}`, termKey: "iprefix"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sel := mustParseSelector(t, tc.expr)
			payload := mustMarshalJSON(t, sel)
			termObj := selectorJSONTermObject(t, payload, tc.termKey)
			if _, exists := termObj["value"]; exists {
				t.Fatalf("expected omitted value for %q, payload=%s", tc.expr, payload)
			}
		})
	}
}

func TestTermMarshalJSONExplicitEmptyEmitsValueField(t *testing.T) {
	cases := []struct {
		name    string
		expr    string
		termKey string
	}{
		{name: "contains", expr: `contains{f=/hello/world,v=""}`, termKey: "contains"},
		{name: "icontains", expr: `icontains{f=/hello/world,v=""}`, termKey: "icontains"},
		{name: "prefix", expr: `prefix{f=/hello/world,v=""}`, termKey: "prefix"},
		{name: "iprefix", expr: `iprefix{f=/hello/world,v=""}`, termKey: "iprefix"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sel := mustParseSelector(t, tc.expr)
			payload := mustMarshalJSON(t, sel)
			termObj := selectorJSONTermObject(t, payload, tc.termKey)
			value, exists := termObj["value"]
			if !exists {
				t.Fatalf("expected explicit empty value for %q, payload=%s", tc.expr, payload)
			}
			if valueStr, ok := value.(string); !ok || valueStr != "" {
				t.Fatalf("expected explicit empty string for %q, got %#v", tc.expr, value)
			}
		})
	}
}

func TestTermMarshalJSONIncludesNonEmptyValueWithoutValueSet(t *testing.T) {
	sel := Selector{
		Contains: &Term{
			Field: "/hello/world",
			Value: "needle",
		},
	}
	payload := mustMarshalJSON(t, sel)
	termObj := selectorJSONTermObject(t, payload, "contains")
	value, exists := termObj["value"]
	if !exists {
		t.Fatalf("expected non-empty value field, payload=%s", payload)
	}
	if valueStr, ok := value.(string); !ok || valueStr != "needle" {
		t.Fatalf("expected value=needle, got %#v", value)
	}
}

func TestSelectorJSONRoundTripStringTermSemantics(t *testing.T) {
	docs := []map[string]any{
		{
			"hello": map[string]any{
				"world": map[string]any{"nested": true},
			},
		},
		{
			"hello": map[string]any{
				"world": []any{1, 2, 3},
			},
		},
		{
			"hello": map[string]any{
				"world": nil,
			},
		},
		{
			"hello": map[string]any{
				"world": "prefix-timeout-ready",
			},
		},
		{
			"hello": map[string]any{
				"other": "x",
			},
		},
	}

	cases := []struct {
		name string
		expr string
	}{
		{name: "contains_omitted", expr: `contains{f=/hello/world}`},
		{name: "icontains_omitted", expr: `icontains{f=/hello/world}`},
		{name: "prefix_omitted", expr: `prefix{f=/hello/world}`},
		{name: "iprefix_omitted", expr: `iprefix{f=/hello/world}`},
		{name: "contains_empty", expr: `contains{f=/hello/world,v=""}`},
		{name: "icontains_empty", expr: `icontains{f=/hello/world,v=""}`},
		{name: "prefix_empty", expr: `prefix{f=/hello/world,v=""}`},
		{name: "iprefix_empty", expr: `iprefix{f=/hello/world,v=""}`},
		{name: "contains_non_empty", expr: `contains{f=/hello/world,v=timeout}`},
		{name: "icontains_any", expr: `icontains{f=/hello/world,a=TIMEOUT|ERROR}`},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sel := mustParseSelector(t, tc.expr)
			roundTrip := roundTripSelectorJSON(t, sel)
			for i, doc := range docs {
				before := Matches(sel, doc)
				after := Matches(roundTrip, doc)
				if before != after {
					t.Fatalf("doc[%d] mismatch for %q: before=%v after=%v", i, tc.expr, before, after)
				}
			}
		})
	}
}

func roundTripSelectorJSON(t *testing.T, sel Selector) Selector {
	t.Helper()
	payload := mustMarshalJSON(t, sel)
	var out Selector
	if err := json.Unmarshal(payload, &out); err != nil {
		t.Fatalf("unmarshal selector: %v", err)
	}
	return out
}

func mustMarshalJSON(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return payload
}

func selectorJSONTermObject(t *testing.T, payload []byte, key string) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode selector payload: %v", err)
	}
	rawTerm, ok := decoded[key]
	if !ok {
		t.Fatalf("selector payload missing %q term: %s", key, payload)
	}
	termObj, ok := rawTerm.(map[string]any)
	if !ok {
		t.Fatalf("selector payload %q term has type %T", key, rawTerm)
	}
	return termObj
}
