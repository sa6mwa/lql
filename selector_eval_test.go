package lql

import "testing"

func TestMatchesWildcardSelectors(t *testing.T) {
	doc := map[string]any{
		"labels": map[string]any{
			"env":   "prod",
			"owner": "alice",
		},
		"items": []any{
			map[string]any{"sku": "A"},
			map[string]any{"sku": "B"},
		},
		"groups": []any{
			map[string]any{
				"items": []any{
					map[string]any{"sku": "A"},
					map[string]any{"sku": "B"},
				},
			},
		},
		"scalar":   "x",
		"arrEmpty": []any{},
	}

	cases := []struct {
		name     string
		expr     string
		expected bool
	}{
		{
			name:     "object-wildcard-on-object",
			expr:     `/labels/*="alice"`,
			expected: true,
		},
		{
			name:     "object-wildcard-on-array-no-match",
			expr:     `/items/*/sku="B"`,
			expected: false,
		},
		{
			name:     "any-child-on-array",
			expr:     `/items/**/sku="B"`,
			expected: true,
		},
		{
			name:     "array-wildcard-sugar",
			expr:     `/items[]/sku="B"`,
			expected: true,
		},
		{
			name:     "nested-wildcards",
			expr:     `/groups[]/items/**/sku="B"`,
			expected: true,
		},
		{
			name:     "recursive-descent",
			expr:     `/groups/.../sku="B"`,
			expected: true,
		},
		{
			name:     "array-wildcard-no-match",
			expr:     `/items[]/sku="C"`,
			expected: false,
		},
		{
			name:     "any-child-on-scalar-no-match",
			expr:     `/scalar/*="x"`,
			expected: false,
		},
		{
			name:     "array-wildcard-empty-array",
			expr:     `/arrEmpty[]/sku="A"`,
			expected: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sel := mustParseSelector(t, tc.expr)
			got := Matches(sel, doc)
			if got != tc.expected {
				t.Fatalf("selector %q expected %v got %v", tc.expr, tc.expected, got)
			}
		})
	}
}

func TestMatchesArrayWildcardOnObjectNoMatch(t *testing.T) {
	doc := map[string]any{
		"items": map[string]any{"sku": "B"},
	}
	sel := mustParseSelector(t, `/items[]/sku="B"`)
	if Matches(sel, doc) {
		t.Fatal("expected no match when [] targets a non-array")
	}
}

func TestMatchesWildcardClauses(t *testing.T) {
	doc := map[string]any{
		"labels": map[string]any{
			"env":   "prod",
			"owner": "alice",
		},
		"items": []any{
			map[string]any{"sku": "A", "price": 10},
			map[string]any{"sku": "B", "price": 25},
		},
		"metrics": []any{
			map[string]any{"battery_mv": 4100},
			map[string]any{"battery_mv": 3300},
		},
	}

	cases := []struct {
		name     string
		expr     string
		expected bool
	}{
		{
			name:     "range-with-array-wildcard",
			expr:     `/items[]/price>=20`,
			expected: true,
		},
		{
			name:     "range-with-any-child",
			expr:     `/metrics/**/battery_mv<3600`,
			expected: true,
		},
		{
			name:     "no-match-on-object-wildcard-array",
			expr:     `/items/*/price>=20`,
			expected: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sel := mustParseSelector(t, tc.expr)
			got := Matches(sel, doc)
			if got != tc.expected {
				t.Fatalf("selector %q expected %v got %v", tc.expr, tc.expected, got)
			}
		})
	}
}

func TestMatchInWildcard(t *testing.T) {
	doc := map[string]any{
		"labels": map[string]any{
			"env":   "prod",
			"owner": "alice",
		},
	}
	sel := Selector{
		In: &InTerm{
			Field: "/labels/*",
			Any:   []string{"prod", "stage"},
		},
	}
	if !Matches(sel, doc) {
		t.Fatal("expected in clause to match object wildcard")
	}
}

func TestMatchExistsWildcard(t *testing.T) {
	doc := map[string]any{
		"items": []any{
			map[string]any{"sku": "A"},
		},
	}
	sel := Selector{Exists: "/items/.../sku"}
	if !Matches(sel, doc) {
		t.Fatal("expected exists clause to match recursive wildcard")
	}
}

func TestMatchesSelectorStringsAndSlice(t *testing.T) {
	sel, err := ParseSelectorStrings([]string{`/status="ok"`, `/msg="done"`})
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if Matches(sel, map[string]any{"status": "ok", "msg": "done"}) == false {
		t.Fatal("expected selector to match")
	}
	if Matches(sel, map[string]any{"status": "ok", "msg": "nope"}) {
		t.Fatal("expected selector to reject mismatched msg")
	}
	if Matches(sel, map[string]any{"status": "nope", "msg": "done"}) {
		t.Fatal("expected selector to reject mismatched status")
	}
}

func TestMatchesSelectorStringsOrSlice(t *testing.T) {
	sel, err := ParseSelectorStringsOr([]string{`/status="ok"`, `/msg="done"`})
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if !Matches(sel, map[string]any{"status": "ok", "msg": "nope"}) {
		t.Fatal("expected selector to match status")
	}
	if !Matches(sel, map[string]any{"status": "nope", "msg": "done"}) {
		t.Fatal("expected selector to match msg")
	}
	if Matches(sel, map[string]any{"status": "nope", "msg": "nope"}) {
		t.Fatal("expected selector to reject with no matches")
	}
}

func mustParseSelector(t *testing.T, expr string) Selector {
	t.Helper()
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse selector %q: %v", expr, err)
	}
	return sel
}
