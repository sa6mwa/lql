package lql

import (
	"testing"
	"time"
)

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

func TestMatchesContainsAndCaseModes(t *testing.T) {
	doc := map[string]any{
		"msg":     "Error: Timeout while reading",
		"service": "Auth-Service",
		"labels": map[string]any{
			"owner": "ALICE-Team",
			"env":   "prod",
		},
	}

	cases := []struct {
		name     string
		expr     string
		expected bool
	}{
		{
			name:     "contains-case-sensitive-match",
			expr:     `contains{field=/msg,value=Timeout}`,
			expected: true,
		},
		{
			name:     "contains-case-sensitive-miss",
			expr:     `contains{field=/msg,value=timeout}`,
			expected: false,
		},
		{
			name:     "contains-ic-alias-true",
			expr:     `contains{field=/msg,value=timeout,ic=t}`,
			expected: true,
		},
		{
			name:     "contains-ignorecase-false-shorthand",
			expr:     `contains{field=/msg,value=timeout,ignoreCase=f}`,
			expected: false,
		},
		{
			name:     "icontains-default",
			expr:     `icontains{field=/msg,value=timeout}`,
			expected: true,
		},
		{
			name:     "icontains-overrides-ignorecase-false",
			expr:     `icontains{field=/msg,value=timeout,ignoreCase=f}`,
			expected: true,
		},
		{
			name:     "prefix-case-sensitive-miss",
			expr:     `prefix{field=/service,value=auth}`,
			expected: false,
		},
		{
			name:     "prefix-ignorecase-true",
			expr:     `prefix{field=/service,value=auth,ignoreCase=true}`,
			expected: true,
		},
		{
			name:     "iprefix-default",
			expr:     `iprefix{field=/service,value=auth}`,
			expected: true,
		},
		{
			name:     "iprefix-overrides-ignorecase-false",
			expr:     `iprefix{field=/service,value=auth,ignoreCase=f}`,
			expected: true,
		},
		{
			name:     "icontains-wildcard",
			expr:     `icontains{field=/labels/*,value=alice}`,
			expected: true,
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

func TestMatchesContainsAnyModes(t *testing.T) {
	doc := map[string]any{
		"msg": "Error: Timeout while reading",
	}
	cases := []struct {
		name     string
		expr     string
		expected bool
	}{
		{name: "contains any match", expr: `contains{f=/msg,a=warn|Timeout}`, expected: true},
		{name: "contains any no match", expr: `contains{f=/msg,a=warn|fatal}`, expected: false},
		{name: "icontains any match", expr: `icontains{f=/msg,a=warn|timeout}`, expected: true},
		{name: "icontains any no match", expr: `icontains{f=/msg,a=warn|fatal}`, expected: false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sel := mustParseSelector(t, tc.expr)
			got := Matches(sel, doc)
			if got != tc.expected {
				t.Fatalf("selector %q expected %v got %v", tc.expr, tc.expected, got)
			}
		})
	}
}

func TestMatchesContainsAnyEquivalentToExplicitOr(t *testing.T) {
	anySel := mustParseSelector(t, `contains{f=/msg,a=warn|timeout}`)
	orSel := mustParseSelector(t, `or.contains{f=/msg,v=warn},or.contains{f=/msg,v=timeout}`)
	docs := []map[string]any{
		{"msg": "warn: cache miss"},
		{"msg": "timeout waiting for reply"},
		{"msg": "all good"},
	}
	for i, doc := range docs {
		gotAny := Matches(anySel, doc)
		gotOr := Matches(orSel, doc)
		if gotAny != gotOr {
			t.Fatalf("doc[%d] any=%v or=%v", i, gotAny, gotOr)
		}
	}
}

func TestMatchesStringTermEmptyValueRemapsToMatchAll(t *testing.T) {
	doc := map[string]any{"status": "open"}
	cases := []struct {
		name string
		expr string
	}{
		{name: "contains", expr: `contains{f=/,v=""}`},
		{name: "icontains", expr: `icontains{f=/,v=""}`},
		{name: "prefix", expr: `prefix{f=/,v=""}`},
		{name: "iprefix", expr: `iprefix{f=/,v=""}`},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sel := mustParseSelector(t, tc.expr)
			if !Matches(sel, doc) {
				t.Fatalf("expected %q to match all documents", tc.expr)
			}
		})
	}
}

func TestMatchesStringTermOmittedValueActsAsPathAssertionForAllStringSelectors(t *testing.T) {
	docWithObject := map[string]any{
		"hello": map[string]any{
			"world": map[string]any{"nested": true},
		},
	}
	docWithArray := map[string]any{
		"hello": map[string]any{
			"world": []any{1, 2, 3},
		},
	}
	docWithNull := map[string]any{
		"hello": map[string]any{
			"world": nil,
		},
	}
	docMissingPath := map[string]any{
		"hello": map[string]any{
			"other": "x",
		},
	}

	cases := []string{
		`contains{f=/hello/world}`,
		`icontains{f=/hello/world}`,
		`prefix{f=/hello/world}`,
		`iprefix{f=/hello/world}`,
	}
	for _, expr := range cases {
		expr := expr
		t.Run(expr, func(t *testing.T) {
			sel := mustParseSelector(t, expr)
			if !Matches(sel, docWithObject) {
				t.Fatalf("expected %q to match when path exists regardless of value type", expr)
			}
			if !Matches(sel, docWithArray) {
				t.Fatalf("expected %q to match array value", expr)
			}
			if !Matches(sel, docWithNull) {
				t.Fatalf("expected %q to match null value", expr)
			}
			if Matches(sel, docMissingPath) {
				t.Fatalf("expected %q to reject when asserted path is missing", expr)
			}
		})
	}
}

func TestMatchesStringTermOmittedValuePathVariants(t *testing.T) {
	doc := map[string]any{
		"hello": map[string]any{
			"world": map[string]any{"nested": true},
			"names": []any{"alice", "bob"},
		},
		"arrays": []any{
			map[string]any{"id": 1},
			map[string]any{"id": 2},
		},
	}

	cases := []struct {
		name     string
		expr     string
		expected bool
	}{
		{name: "object wildcard", expr: `contains{f=/hello/*}`, expected: true},
		{name: "recursive", expr: `contains{f=/hello/...}`, expected: true},
		{name: "array wildcard", expr: `contains{f=/arrays/[]}`, expected: true},
		{name: "array wildcard child", expr: `contains{f=/arrays/[]/id}`, expected: true},
		{name: "missing literal", expr: `contains{f=/hello/missing}`, expected: false},
		{name: "missing wildcard parent", expr: `contains{f=/missing/*}`, expected: false},
		{name: "missing recursive parent", expr: `contains{f=/missing/...}`, expected: false},
		{name: "missing array parent", expr: `contains{f=/missing/[]}`, expected: false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sel := mustParseSelector(t, tc.expr)
			got := Matches(sel, doc)
			if got != tc.expected {
				t.Fatalf("selector %q expected %v got %v", tc.expr, tc.expected, got)
			}
		})
	}
}

func TestMatchesNotOfStringTermEmptyValueIsAlwaysFalse(t *testing.T) {
	sel := mustParseSelector(t, `not.icontains{f=/,v=""}`)
	if Matches(sel, map[string]any{"status": "open"}) {
		t.Fatal("expected selector to be always false")
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

func TestMatchesShorthandAndClauses(t *testing.T) {
	expr := `/field="value",/status="ok"`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if !Matches(sel, map[string]any{"field": "value", "status": "ok"}) {
		t.Fatal("expected selector to match")
	}
	if Matches(sel, map[string]any{"field": "value", "status": "nope"}) {
		t.Fatal("expected selector to reject mismatched status")
	}
	if Matches(sel, map[string]any{"field": "nope", "status": "ok"}) {
		t.Fatal("expected selector to reject mismatched field")
	}
}

func TestMatchesShorthandOrClauses(t *testing.T) {
	expr := `/field="value",/status="ok"`
	sel, err := ParseSelectorStringOr(expr)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if !Matches(sel, map[string]any{"field": "value", "status": "nope"}) {
		t.Fatal("expected selector to match field")
	}
	if !Matches(sel, map[string]any{"field": "nope", "status": "ok"}) {
		t.Fatal("expected selector to match status")
	}
	if Matches(sel, map[string]any{"field": "nope", "status": "nope"}) {
		t.Fatal("expected selector to reject with no matches")
	}
}

func TestMatchesShorthandOrClausesCommaElement(t *testing.T) {
	sel, err := ParseSelectorStringsOr([]string{`/status="ok",/status="done"`})
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if !Matches(sel, map[string]any{"status": "ok"}) {
		t.Fatal("expected selector to match ok")
	}
	if !Matches(sel, map[string]any{"status": "done"}) {
		t.Fatal("expected selector to match done")
	}
	if Matches(sel, map[string]any{"status": "nope"}) {
		t.Fatal("expected selector to reject with no matches")
	}
}

func TestMatchesExplicitAndInStringOr(t *testing.T) {
	expr := `and.eq{field=/status,value=ok},/msg="done"`
	sel, err := ParseSelectorStringOr(expr)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if !Matches(sel, map[string]any{"status": "ok", "msg": "done"}) {
		t.Fatal("expected selector to match both clauses")
	}
	if Matches(sel, map[string]any{"status": "ok", "msg": "nope"}) {
		t.Fatal("expected selector to reject missing msg")
	}
	if Matches(sel, map[string]any{"status": "nope", "msg": "done"}) {
		t.Fatal("expected selector to reject missing status")
	}
}

func TestMatchesExplicitOrIndexMerges(t *testing.T) {
	expr := `or.0.eq{field=/status,value=ok},or.0.range{field=/progress,gte=10}`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if !Matches(sel, map[string]any{"status": "ok", "progress": 10}) {
		t.Fatal("expected selector to match")
	}
	if Matches(sel, map[string]any{"status": "ok", "progress": 5}) {
		t.Fatal("expected selector to reject low progress")
	}
	if Matches(sel, map[string]any{"status": "nope", "progress": 10}) {
		t.Fatal("expected selector to reject wrong status")
	}
}

func TestMatchesExplicitAndIndexMerges(t *testing.T) {
	expr := `and.0.eq{field=/status,value=ok},and.0.range{field=/progress,gte=10}`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if !Matches(sel, map[string]any{"status": "ok", "progress": 10}) {
		t.Fatal("expected selector to match")
	}
	if Matches(sel, map[string]any{"status": "ok", "progress": 5}) {
		t.Fatal("expected selector to reject low progress")
	}
	if Matches(sel, map[string]any{"status": "nope", "progress": 10}) {
		t.Fatal("expected selector to reject wrong status")
	}
}

func TestMatchesNotWithAndGroup(t *testing.T) {
	expr := `not.eq{field=/status,value=closed},/region="us"`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if !Matches(sel, map[string]any{"status": "open", "region": "us"}) {
		t.Fatal("expected selector to match")
	}
	if Matches(sel, map[string]any{"status": "closed", "region": "us"}) {
		t.Fatal("expected selector to reject closed")
	}
	if Matches(sel, map[string]any{"status": "open", "region": "eu"}) {
		t.Fatal("expected selector to reject wrong region")
	}
}

func TestMatchesExistsInOrString(t *testing.T) {
	expr := `exists{/meta/etag},/status="ok"`
	sel, err := ParseSelectorStringOr(expr)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if !Matches(sel, map[string]any{"meta": map[string]any{"etag": "abc"}}) {
		t.Fatal("expected selector to match exists")
	}
	if !Matches(sel, map[string]any{"status": "ok"}) {
		t.Fatal("expected selector to match status")
	}
	if Matches(sel, map[string]any{"status": "nope"}) {
		t.Fatal("expected selector to reject")
	}
}

func TestMatchesInOrString(t *testing.T) {
	expr := `in{field=/env,any=prod|stage},/status="ok"`
	sel, err := ParseSelectorStringOr(expr)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if !Matches(sel, map[string]any{"env": "prod"}) {
		t.Fatal("expected selector to match env")
	}
	if !Matches(sel, map[string]any{"status": "ok"}) {
		t.Fatal("expected selector to match status")
	}
	if Matches(sel, map[string]any{"env": "dev"}) {
		t.Fatal("expected selector to reject")
	}
}

func TestMatchesInlineAliases(t *testing.T) {
	expr := `eq{f=/status,v=ok},in{f=/env,a=prod|stage}`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if !Matches(sel, map[string]any{"status": "ok", "env": "prod"}) {
		t.Fatal("expected selector to match")
	}
	if Matches(sel, map[string]any{"status": "ok", "env": "dev"}) {
		t.Fatal("expected selector to reject env")
	}
}

func TestMatchesEqDateOnlyIntersectsTimestamp(t *testing.T) {
	selector := mustParseSelector(t, `/something="2025-01-01"`)
	docMatch := map[string]any{"something": "2025-01-01T15:00:00Z"}
	docMiss := map[string]any{"something": "2025-01-02T00:00:00Z"}
	if !Matches(selector, docMatch) {
		t.Fatalf("expected date-only equality to match timestamp on same date")
	}
	if Matches(selector, docMiss) {
		t.Fatalf("expected date-only equality to reject different date")
	}
}

func TestMatchesRangeDatetimeAcrossTimezone(t *testing.T) {
	selector := mustParseSelector(t, `/something>=2026-03-05T10:28:21Z`)
	doc := map[string]any{"something": "2026-03-05T11:28:21+01:00"}
	if !Matches(selector, doc) {
		t.Fatalf("expected timezone-normalized datetime range match")
	}
}

func TestMatchesRangeNaiveDatetimeUsesUTC(t *testing.T) {
	selector := mustParseSelector(t, `/something>=2026-03-05T10:28:21`)
	docMatch := map[string]any{"something": "2026-03-05T11:28:21+01:00"}
	docMiss := map[string]any{"something": "2026-03-05T10:28:20Z"}
	if !Matches(selector, docMatch) {
		t.Fatalf("expected naive datetime range to use UTC")
	}
	if Matches(selector, docMiss) {
		t.Fatalf("expected naive datetime range miss below UTC threshold")
	}
}

func TestMatchesDateSelectorNaiveDatetimeFractionUsesUTC(t *testing.T) {
	selector := mustParseSelector(t, `date{f=/something,after=2026-03-05T10:28:21.123,before=2026-03-05T10:28:21.123456790}`)
	docMatch := map[string]any{"something": "2026-03-05T10:28:21.123456789Z"}
	docMiss := map[string]any{"something": "2026-03-05T10:28:21.123+01:00"}
	if !Matches(selector, docMatch) {
		t.Fatalf("expected naive fractional datetime bounds to match UTC candidate")
	}
	if Matches(selector, docMiss) {
		t.Fatalf("expected naive fractional datetime bounds to miss non-UTC-equivalent candidate")
	}
}

func TestMatchesDateSelectorBounds(t *testing.T) {
	selector := mustParseSelector(t, `date{f=/something,after=2025-01-01,before=2025-01-03}`)
	docMatch := map[string]any{"something": "2025-01-02T06:00:00Z"}
	docMiss := map[string]any{"something": "2025-01-03T00:00:00Z"}
	if !Matches(selector, docMatch) {
		t.Fatalf("expected date selector bound match")
	}
	if Matches(selector, docMiss) {
		t.Fatalf("expected date selector upper-bound miss")
	}
}

func TestMatchesDateSelectorSinceNowMacro(t *testing.T) {
	now := time.Now()
	selector := mustParseSelector(t, `date{f=/something,since=now}`)
	docFuture := map[string]any{"something": now.Add(2 * time.Minute).Format(time.RFC3339Nano)}
	docPast := map[string]any{"something": now.Add(-2 * time.Minute).Format(time.RFC3339Nano)}
	if !Matches(selector, docFuture) {
		t.Fatalf("expected future timestamp to match since=now")
	}
	if Matches(selector, docPast) {
		t.Fatalf("expected past timestamp to miss since=now")
	}
}

func TestMatchesInAny(t *testing.T) {
	sel, err := ParseSelectorString(`in{field=/env,any=prod|stage|dev}`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if !Matches(sel, map[string]any{"env": "stage"}) {
		t.Fatal("expected selector to match stage")
	}
	if Matches(sel, map[string]any{"env": "devops"}) {
		t.Fatal("expected selector to reject non-member")
	}
}

func TestMatchesInAnyWithOrString(t *testing.T) {
	expr := `in{field=/env,any=prod|stage},/status="ok"`
	sel, err := ParseSelectorStringOr(expr)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if !Matches(sel, map[string]any{"env": "prod"}) {
		t.Fatal("expected selector to match env")
	}
	if !Matches(sel, map[string]any{"status": "ok"}) {
		t.Fatal("expected selector to match status")
	}
	if Matches(sel, map[string]any{"env": "dev"}) {
		t.Fatal("expected selector to reject")
	}
}

func TestMatchesShorthandAndOrClauses(t *testing.T) {
	expr := `/field="value",/status="ok",or.eq{field=/msg,value=done},or.eq{field=/msg,value=complete}`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if !Matches(sel, map[string]any{"field": "value", "status": "ok", "msg": "done"}) {
		t.Fatal("expected selector to match done msg")
	}
	if !Matches(sel, map[string]any{"field": "value", "status": "ok", "msg": "complete"}) {
		t.Fatal("expected selector to match complete msg")
	}
	if Matches(sel, map[string]any{"field": "value", "status": "ok", "msg": "nope"}) {
		t.Fatal("expected selector to reject non-matching msg")
	}
	if Matches(sel, map[string]any{"field": "value", "status": "nope", "msg": "done"}) {
		t.Fatal("expected selector to reject mismatched status")
	}
	if Matches(sel, map[string]any{"field": "nope", "status": "ok", "msg": "done"}) {
		t.Fatal("expected selector to reject mismatched field")
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
