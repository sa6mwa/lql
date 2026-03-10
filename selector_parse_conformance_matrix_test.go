package lql

import (
	"net/url"
	"strings"
	"testing"
)

func TestSelectorParseConformanceBraceAssignmentPermutations(t *testing.T) {
	cases := []struct {
		name        string
		term        string
		assignments []string
		want        Selector
		wrappers    []string
	}{
		{
			name:        "eq",
			term:        "eq",
			assignments: []string{"field=/status", "value=open"},
			want:        Selector{Eq: stringTerm("/status", "open", nil, false, true)},
			wrappers:    []string{"root", "and", "or", "not", "and_or_indexed", "or_and_indexed"},
		},
		{
			name:        "contains_ignore_case",
			term:        "contains",
			assignments: []string{"field=/msg", "value=timeout", "ignoreCase=t"},
			want:        Selector{Contains: stringTerm("/msg", "timeout", nil, true, true)},
			wrappers:    []string{"root", "and", "or", "not", "and_or_indexed", "or_and_indexed"},
		},
		{
			name:        "contains_any",
			term:        "contains",
			assignments: []string{"field=/msg", "any=timeout|error"},
			want:        Selector{Contains: stringTerm("/msg", "", []string{"timeout", "error"}, false, false)},
			wrappers:    []string{"root", "and", "or", "not", "and_or_indexed", "or_and_indexed"},
		},
		{
			name:        "range_datetime_window",
			term:        "range",
			assignments: []string{"field=/timestamp", "gte=2026-03-05T10:28:21Z", "lt=2026-03-05T10:30:00Z"},
			want:        Selector{Range: &RangeTerm{Field: "/timestamp", GTE: NewDatetimeRangeBound("2026-03-05T10:28:21Z"), LT: NewDatetimeRangeBound("2026-03-05T10:30:00Z")}},
			wrappers:    []string{"root", "and", "or", "not", "and_or_indexed", "or_and_indexed"},
		},
		{
			name:        "date_after_before",
			term:        "date",
			assignments: []string{"field=/timestamp", "after=2025-01-01", "before=2025-01-03"},
			want:        Selector{Date: &DateTerm{Field: "/timestamp", After: "2025-01-01", Before: "2025-01-03"}},
			wrappers:    []string{"root", "and", "or", "not", "and_or_indexed", "or_and_indexed"},
		},
		{
			name:        "in_any_phrases",
			term:        "in",
			assignments: []string{"field=/greeting", `any="hello world|goodbye jupiter"`},
			want:        Selector{In: &InTerm{Field: "/greeting", Any: []string{"hello world", "goodbye jupiter"}}},
			wrappers:    []string{"root", "and", "or", "not", "and_or_indexed", "or_and_indexed"},
		},
	}

	for _, tc := range cases {
		tc := tc
		for _, wrapper := range selectorParseWrappers(tc.wrappers) {
			wrapper := wrapper
			for _, assignments := range permuteStrings(tc.assignments) {
				assignments := assignments
				for _, expr := range braceExpressionVariants(tc.term, assignments) {
					expr := expr
					name := strings.Join([]string{tc.name, wrapper.name, sanitizeTestName(expr)}, "/")
					t.Run(name, func(t *testing.T) {
						want := wrapper.wrap(tc.want)
						for _, ep := range selectorStringSingleClauseEntryPoints() {
							ep := ep
							t.Run(ep.name, func(t *testing.T) {
								got, err := ep.parse(t, wrapper.expr(expr))
								if err != nil {
									t.Fatalf("parse failed: %v", err)
								}
								assertSelectorJSONEqual(t, got, want)
							})
						}
					})
				}
			}
		}
	}
}

func TestSelectorParseConformanceParseSelectorValuesMixedForms(t *testing.T) {
	cases := []struct {
		name   string
		values url.Values
		want   Selector
	}{
		{
			name: "root_brace_plus_dotted",
			values: url.Values{
				`eq{field=/status,value=open}`: []string{""},
				"in.field":                     []string{"/env"},
				"in.any":                       []string{"prod|stage"},
			},
			want: Selector{
				Eq: stringTerm("/status", "open", nil, false, true),
				In: &InTerm{Field: "/env", Any: []string{"prod", "stage"}},
			},
		},
		{
			name: "nested_and_or_mixed",
			values: url.Values{
				"and.0.eq.field":                     []string{"/status"},
				"and.0.eq.value":                     []string{"open"},
				`and.1.or.0.in{f=/env,a=prod|stage}`: []string{""},
				"and.1.or.1.exists":                  []string{"/meta/etag"},
			},
			want: Selector{
				And: []Selector{
					{Eq: stringTerm("/status", "open", nil, false, true)},
					{
						Or: []Selector{
							{In: &InTerm{Field: "/env", Any: []string{"prod", "stage"}}},
							{Exists: "/meta/etag"},
						},
					},
				},
			},
		},
		{
			name: "root_brace_plus_dotted_same_clause_idempotent",
			values: url.Values{
				`eq{field=/status,value=open}`: []string{""},
				"eq.field":                     []string{"/status"},
				"eq.value":                     []string{"open"},
			},
			want: Selector{
				Eq: stringTerm("/status", "open", nil, false, true),
			},
		},
		{
			name: "or_nested_and_mixed",
			values: url.Values{
				`or.0.eq{field=/status,value=open}`: []string{""},
				"or.1.and.0.range.field":            []string{"/progress"},
				"or.1.and.0.range.gte":              []string{"10"},
				"or.1.and.0.exists":                 []string{"/meta/etag"},
			},
			want: Selector{
				Or: []Selector{
					{Eq: stringTerm("/status", "open", nil, false, true)},
					{
						And: []Selector{
							{
								Range:  &RangeTerm{Field: "/progress", GTE: NewNumericRangeBound(10)},
								Exists: "/meta/etag",
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseSelectorValues(tc.values)
			if err != nil {
				t.Fatalf("parse values failed: %v", err)
			}
			assertSelectorJSONEqual(t, got, tc.want)
		})
	}
}

func TestSelectorParseConformanceNestedWrapperMatrix(t *testing.T) {
	type nestedWrapper struct {
		name string
		expr func(expr string) string
		wrap func(sel Selector) Selector
	}

	baseCases := []struct {
		name string
		expr string
		want Selector
	}{
		{name: "eq", expr: `eq{field=/status,value=open}`, want: Selector{Eq: stringTerm("/status", "open", nil, false, true)}},
		{name: "in", expr: `in{field=/env,any=prod|stage}`, want: Selector{In: &InTerm{Field: "/env", Any: []string{"prod", "stage"}}}},
		{name: "exists", expr: `exists{/meta/etag}`, want: Selector{Exists: "/meta/etag"}},
	}

	wrappers := []nestedWrapper{
		{
			name: "and_or_not",
			expr: func(expr string) string { return "and.0.or.0.not." + expr },
			wrap: func(sel Selector) Selector {
				return Selector{And: []Selector{{Or: []Selector{{Not: &sel}}}}}
			},
		},
		{
			name: "or_and_not",
			expr: func(expr string) string { return "or.0.and.0.not." + expr },
			wrap: func(sel Selector) Selector {
				return Selector{Or: []Selector{{And: []Selector{{Not: &sel}}}}}
			},
		},
		{
			name: "and_or_and",
			expr: func(expr string) string { return "and.0.or.0.and.0." + expr },
			wrap: func(sel Selector) Selector {
				return Selector{And: []Selector{{Or: []Selector{{And: []Selector{sel}}}}}}
			},
		},
		{
			name: "or_and_or",
			expr: func(expr string) string { return "or.0.and.0.or.0." + expr },
			wrap: func(sel Selector) Selector {
				return Selector{Or: []Selector{{And: []Selector{{Or: []Selector{sel}}}}}}
			},
		},
	}

	for _, base := range baseCases {
		base := base
		for _, wrapper := range wrappers {
			wrapper := wrapper
			t.Run(base.name+"/"+wrapper.name, func(t *testing.T) {
				for _, ep := range selectorStringSingleClauseEntryPoints() {
					ep := ep
					t.Run(ep.name, func(t *testing.T) {
						got, err := ep.parse(t, wrapper.expr(base.expr))
						if err != nil {
							t.Fatalf("parse failed: %v", err)
						}
						assertSelectorJSONEqual(t, got, wrapper.wrap(base.want))
					})
				}
			})
		}
	}
}

func TestSelectorParseConformanceWhitespaceVariants(t *testing.T) {
	cases := []struct {
		name     string
		variants []string
		want     Selector
	}{
		{
			name: "eq_shorthand_spacing",
			variants: []string{
				`/status="open"`,
				` /status = "open" `,
				"\n/status = \"open\"\n",
			},
			want: Selector{Eq: stringTerm("/status", "open", nil, false, true)},
		},
		{
			name: "range_shorthand_spacing",
			variants: []string{
				`/progress>=42`,
				` /progress >= 42 `,
				"\n/progress   >=   42\n",
			},
			want: Selector{Range: &RangeTerm{Field: "/progress", GTE: NewNumericRangeBound(42)}},
		},
		{
			name: "multiline_composition",
			variants: []string{
				"eq{field=/status,value=open},in{field=/env,any=prod|stage}",
				"eq{field=/status,value=open}\nin{field=/env,any=prod|stage}",
				" eq{field=/status,value=open},\n in{field=/env,any=prod|stage} ",
			},
			want: Selector{
				And: []Selector{
					{Eq: stringTerm("/status", "open", nil, false, true)},
					{In: &InTerm{Field: "/env", Any: []string{"prod", "stage"}}},
				},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		for i, expr := range tc.variants {
			expr := expr
			t.Run(tc.name+"/"+string(rune('a'+i)), func(t *testing.T) {
				got, err := ParseSelectorString(expr)
				if err != nil {
					t.Fatalf("parse failed: %v", err)
				}
				assertSelectorJSONEqual(t, got, tc.want)
			})
		}
	}
}

func TestSelectorParseConformanceInvalidValuesMixedForms(t *testing.T) {
	cases := []struct {
		name   string
		values url.Values
	}{
		{
			name: "brace_plus_dotted_conflict",
			values: url.Values{
				`eq{field=/status,value=open}`: []string{""},
				"eq.field":                     []string{"/other"},
				"eq.value":                     []string{"closed"},
			},
		},
		{
			name: "alias_conflict_same_term",
			values: url.Values{
				"contains.field": []string{"/msg"},
				"contains.f":     []string{"/other"},
				"contains.value": []string{"timeout"},
			},
		},
		{
			name: "nested_exists_conflict",
			values: url.Values{
				"and.0.exists":     []string{"/meta/etag"},
				"and.0.exists.foo": []string{"bar"},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseSelectorValues(tc.values); err == nil {
				t.Fatalf("expected parse values error for %#v", tc.values)
			}
		})
	}
}

func braceExpressionVariants(term string, assignments []string) []string {
	return []string{
		term + "{" + strings.Join(assignments, ",") + "}",
		term + "{ " + strings.Join(assignments, ", ") + " }",
		term + "{\n" + strings.Join(assignments, "\n") + "\n}",
		term + "{\n  " + strings.Join(assignments, ",\n  ") + "\n}",
	}
}

func permuteStrings(input []string) [][]string {
	if len(input) == 0 {
		return [][]string{{}}
	}
	working := append([]string(nil), input...)
	var out [][]string
	var walk func(int)
	walk = func(idx int) {
		if idx == len(working) {
			out = append(out, append([]string(nil), working...))
			return
		}
		for i := idx; i < len(working); i++ {
			working[idx], working[i] = working[i], working[idx]
			walk(idx + 1)
			working[idx], working[i] = working[i], working[idx]
		}
	}
	walk(0)
	return out
}

func sanitizeTestName(raw string) string {
	replacer := strings.NewReplacer(" ", "_", "\n", "_", "\t", "_", "{", "", "}", "", ",", "_", "\"", "")
	return replacer.Replace(raw)
}
