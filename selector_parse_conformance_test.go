package lql

import (
	"encoding/json"
	"net/url"
	"slices"
	"strings"
	"testing"
)

type selectorParseEntryPoint struct {
	name  string
	parse func(t *testing.T, expr string) (Selector, error)
}

type selectorParseWrapper struct {
	name string
	expr func(expr string) string
	wrap func(sel Selector) Selector
}

type selectorExpressionCase struct {
	name     string
	expr     string
	want     Selector
	wrappers []string
}

type selectorValuesCase struct {
	name     string
	values   url.Values
	want     Selector
	wrappers []string
}

type selectorInvalidExpressionCase struct {
	name       string
	expr       string
	wrappers   []string
	entrypoint []string
}

type selectorInvalidValuesCase struct {
	name   string
	values url.Values
}

func TestSelectorParseConformanceSingleClauseBraceForms(t *testing.T) {
	for _, tc := range selectorPositiveExpressionCases() {
		tc := tc
		for _, wrapper := range selectorParseWrappers(tc.wrappers) {
			wrapper := wrapper
			want := wrapper.wrap(tc.want)
			for _, ep := range selectorStringSingleClauseEntryPoints() {
				ep := ep
				name := strings.Join([]string{tc.name, wrapper.name, ep.name}, "/")
				t.Run(name, func(t *testing.T) {
					got, err := ep.parse(t, wrapper.expr(tc.expr))
					if err != nil {
						t.Fatalf("parse failed: %v", err)
					}
					assertSelectorJSONEqual(t, got, want)
				})
			}
		}
	}
}

func TestSelectorParseConformanceStringSliceEntryPoints(t *testing.T) {
	for _, tc := range selectorPositiveExpressionCases() {
		tc := tc
		for _, wrapper := range selectorParseWrappers(tc.wrappers) {
			wrapper := wrapper
			t.Run(tc.name+"/"+wrapper.name+"/ParseSelectorStrings", func(t *testing.T) {
				got, err := ParseSelectorStrings([]string{wrapper.expr(tc.expr)})
				if err != nil {
					t.Fatalf("parse strings failed: %v", err)
				}
				want := Selector{And: []Selector{wrapper.wrap(tc.want)}}
				assertSelectorJSONEqual(t, got, want)
			})
			t.Run(tc.name+"/"+wrapper.name+"/ParseSelectorStringsOr", func(t *testing.T) {
				got, err := ParseSelectorStringsOr([]string{wrapper.expr(tc.expr)})
				if err != nil {
					t.Fatalf("parse strings or failed: %v", err)
				}
				want := Selector{Or: []Selector{wrapper.wrap(tc.want)}}
				assertSelectorJSONEqual(t, got, want)
			})
		}
	}
}

func TestSelectorParseConformanceSingleClauseDottedValues(t *testing.T) {
	for _, tc := range selectorPositiveValuesCases() {
		tc := tc
		for _, wrapper := range selectorParseWrappers(tc.wrappers) {
			wrapper := wrapper
			want := wrapper.wrap(tc.want)
			name := strings.Join([]string{tc.name, wrapper.name}, "/")
			t.Run(name, func(t *testing.T) {
				values := wrapSelectorValues(tc.values, wrapper)
				got, err := ParseSelectorValues(values)
				if err != nil {
					t.Fatalf("parse values failed: %v", err)
				}
				assertSelectorJSONEqual(t, got, want)
			})
		}
	}
}

func TestSelectorParseConformanceShorthandForms(t *testing.T) {
	cases := []selectorExpressionCase{
		{
			name: "eq_string",
			expr: `/status="open"`,
			want: Selector{Eq: stringTerm("/status", "open", nil, false, true)},
		},
		{
			name: "eq_bool",
			expr: `/enabled=true`,
			want: Selector{Eq: stringTerm("/enabled", "true", nil, false, true)},
		},
		{
			name: "range_numeric",
			expr: `/progress>=42`,
			want: Selector{Range: &RangeTerm{Field: "/progress", GTE: NewNumericRangeBound(42)}},
		},
		{
			name: "range_datetime",
			expr: `/timestamp<"2026-03-05T11:29:41.265+01:00"`,
			want: Selector{Range: &RangeTerm{Field: "/timestamp", LT: NewDatetimeRangeBound("2026-03-05T11:29:41.265+01:00")}},
		},
	}
	for _, tc := range cases {
		tc := tc
		for _, ep := range selectorStringOnlyEntryPoints() {
			ep := ep
			t.Run(tc.name+"/"+ep.name, func(t *testing.T) {
				got, err := ep.parse(t, tc.expr)
				if err != nil {
					t.Fatalf("parse shorthand failed: %v", err)
				}
				assertSelectorJSONEqual(t, got, tc.want)
			})
		}
	}
}

func TestSelectorParseConformanceCompositionForms(t *testing.T) {
	cases := []struct {
		name string
		expr string
		want Selector
	}{
		{
			name: "implicit_and",
			expr: `eq{field=/status,value=open},range{field=/progress,gte=10}`,
			want: Selector{
				And: []Selector{
					{Eq: stringTerm("/status", "open", nil, false, true)},
					{Range: &RangeTerm{Field: "/progress", GTE: NewNumericRangeBound(10)}},
				},
			},
		},
		{
			name: "explicit_or",
			expr: `or.eq{field=/region,value=us},or.eq{field=/region,value=eu}`,
			want: Selector{
				Or: []Selector{
					{Eq: stringTerm("/region", "us", nil, false, true)},
					{Eq: stringTerm("/region", "eu", nil, false, true)},
				},
			},
		},
		{
			name: "indexed_and_merge",
			expr: `and.0.eq{field=/status,value=open},and.0.range{field=/progress,gte=10}`,
			want: Selector{
				And: []Selector{
					{
						Eq:    stringTerm("/status", "open", nil, false, true),
						Range: &RangeTerm{Field: "/progress", GTE: NewNumericRangeBound(10)},
					},
				},
			},
		},
		{
			name: "indexed_or_merge",
			expr: `or.0.eq{field=/status,value=open},or.0.in{field=/env,any=prod|stage}`,
			want: Selector{
				Or: []Selector{
					{
						Eq: stringTerm("/status", "open", nil, false, true),
						In: &InTerm{Field: "/env", Any: []string{"prod", "stage"}},
					},
				},
			},
		},
		{
			name: "and_with_nested_or_group",
			expr: `and.0.eq{field=/status,value=open},and.1.or.0.in{field=/env,any=prod|stage},and.1.or.1.exists{/meta/etag}`,
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
			name: "or_with_nested_and_group",
			expr: `or.0.eq{field=/status,value=open},or.1.and.0.range{field=/progress,gte=10},or.1.and.0.exists{/meta/etag}`,
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
		for _, ep := range selectorStringCompositionEntryPoints() {
			ep := ep
			t.Run(tc.name+"/"+ep.name, func(t *testing.T) {
				got, err := ep.parse(t, tc.expr)
				if err != nil {
					t.Fatalf("parse composition failed: %v", err)
				}
				assertSelectorJSONEqual(t, got, tc.want)
			})
		}
	}
}

func TestSelectorParseConformanceEquivalentForms(t *testing.T) {
	cases := []struct {
		name  string
		exprs []string
	}{
		{
			name: "eq_aliases",
			exprs: []string{
				`eq{field=/status,value=open}`,
				`eq{f=/status,v=open}`,
				`/status="open"`,
			},
		},
		{
			name: "contains_any_aliases",
			exprs: []string{
				`contains{field=/msg,any=timeout|error}`,
				`contains{f=/msg,a=timeout|error}`,
				"contains{ field=/msg,\nany=timeout|error }",
			},
		},
		{
			name: "in_any_quoted_equivalence",
			exprs: []string{
				`in{field=/env,any=prod|stage}`,
				`in{f=/env,a=prod|stage}`,
				`in{field=/env,any="prod|stage"}`,
			},
		},
		{
			name: "exists_spacing_equivalence",
			exprs: []string{
				`exists{/meta/etag}`,
				" exists{/meta/etag} ",
			},
		},
		{
			name: "range_datetime_aliases",
			exprs: []string{
				`range{field=/timestamp,gte=2026-03-05T10:28:21Z}`,
				`range{ field=/timestamp, gte=2026-03-05T10:28:21Z }`,
			},
		},
		{
			name: "date_aliases",
			exprs: []string{
				`date{field=/timestamp,after=2025-01-01,before=2025-01-03}`,
				`date{f=/timestamp,a=2025-01-01,b=2025-01-03}`,
			},
		},
		{
			name: "contains_ignore_case_aliases",
			exprs: []string{
				`contains{field=/msg,value=timeout,ignoreCase=true}`,
				`contains{f=/msg,v=timeout,ic=t}`,
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		for _, ep := range selectorStringOnlyEntryPoints() {
			ep := ep
			t.Run(tc.name+"/"+ep.name, func(t *testing.T) {
				var first Selector
				for i, expr := range tc.exprs {
					got, err := ep.parse(t, expr)
					if err != nil {
						t.Fatalf("parse equivalence case %q failed: %v", expr, err)
					}
					if i == 0 {
						first = got
						continue
					}
					assertSelectorJSONEqual(t, got, first)
				}
			})
		}
	}
}

func TestSelectorParseConformanceSingleQuotedValues(t *testing.T) {
	cases := []struct {
		name string
		expr string
		want Selector
	}{
		{
			name: "eq_value_with_comma",
			expr: `eq{field=/status,value='open,closed'}`,
			want: Selector{Eq: stringTerm("/status", "open,closed", nil, false, true)},
		},
		{
			name: "contains_value_with_space",
			expr: `contains{field=/msg,value='hello world'}`,
			want: Selector{Contains: stringTerm("/msg", "hello world", nil, false, true)},
		},
		{
			name: "exists_pointer_with_comma",
			expr: `exists{'/meta,etag'}`,
			want: Selector{Exists: "/meta,etag"},
		},
	}

	for _, tc := range cases {
		tc := tc
		for _, ep := range selectorStringSingleClauseEntryPoints() {
			ep := ep
			t.Run(tc.name+"/"+ep.name, func(t *testing.T) {
				got, err := ep.parse(t, tc.expr)
				if err != nil {
					t.Fatalf("parse failed: %v", err)
				}
				assertSelectorJSONEqual(t, got, tc.want)
			})
		}
	}
}

func TestSelectorParseConformanceInvalidBraceForms(t *testing.T) {
	for _, tc := range selectorInvalidExpressionCases() {
		tc := tc
		for _, wrapper := range selectorParseWrappers(tc.wrappers) {
			wrapper := wrapper
			for _, ep := range selectorFilterEntryPoints(selectorStringSingleClauseEntryPoints(), tc.entrypoint) {
				ep := ep
				name := strings.Join([]string{tc.name, wrapper.name, ep.name}, "/")
				t.Run(name, func(t *testing.T) {
					if _, err := ep.parse(t, wrapper.expr(tc.expr)); err == nil {
						t.Fatalf("expected parse error for %q", wrapper.expr(tc.expr))
					}
				})
			}
		}
	}
}

func TestSelectorParseConformanceInvalidDottedValues(t *testing.T) {
	for _, tc := range selectorInvalidValuesCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseSelectorValues(tc.values); err == nil {
				t.Fatalf("expected parse values error for %#v", tc.values)
			}
		})
	}
}

func selectorStringSingleClauseEntryPoints() []selectorParseEntryPoint {
	return []selectorParseEntryPoint{
		{
			name: "ParseSelectorString",
			parse: func(_ *testing.T, expr string) (Selector, error) {
				return ParseSelectorString(expr)
			},
		},
		{
			name: "ParseSelectorStringOr",
			parse: func(_ *testing.T, expr string) (Selector, error) {
				return ParseSelectorStringOr(expr)
			},
		},
		{
			name: "ParseSelectorValuesBrace",
			parse: func(_ *testing.T, expr string) (Selector, error) {
				values := url.Values{}
				values.Set(expr, "")
				return ParseSelectorValues(values)
			},
		},
	}
}

func selectorFilterEntryPoints(entrypoints []selectorParseEntryPoint, names []string) []selectorParseEntryPoint {
	if len(names) == 0 {
		return entrypoints
	}
	allowed := make(map[string]struct{}, len(names))
	for _, name := range names {
		allowed[name] = struct{}{}
	}
	out := make([]selectorParseEntryPoint, 0, len(names))
	for _, ep := range entrypoints {
		if _, ok := allowed[ep.name]; ok {
			out = append(out, ep)
		}
	}
	return out
}

func selectorStringOnlyEntryPoints() []selectorParseEntryPoint {
	return []selectorParseEntryPoint{
		{
			name: "ParseSelectorString",
			parse: func(_ *testing.T, expr string) (Selector, error) {
				return ParseSelectorString(expr)
			},
		},
		{
			name: "ParseSelectorStringOr",
			parse: func(_ *testing.T, expr string) (Selector, error) {
				return ParseSelectorStringOr(expr)
			},
		},
	}
}

func selectorStringCompositionEntryPoints() []selectorParseEntryPoint {
	return []selectorParseEntryPoint{
		{
			name: "ParseSelectorString",
			parse: func(_ *testing.T, expr string) (Selector, error) {
				return ParseSelectorString(expr)
			},
		},
	}
}

func selectorParseWrappers(filter []string) []selectorParseWrapper {
	all := []selectorParseWrapper{
		{
			name: "root",
			expr: func(expr string) string { return expr },
			wrap: func(sel Selector) Selector { return sel },
		},
		{
			name: "and",
			expr: func(expr string) string { return "and." + expr },
			wrap: func(sel Selector) Selector { return Selector{And: []Selector{sel}} },
		},
		{
			name: "and_indexed",
			expr: func(expr string) string { return "and.0." + expr },
			wrap: func(sel Selector) Selector { return Selector{And: []Selector{sel}} },
		},
		{
			name: "or",
			expr: func(expr string) string { return "or." + expr },
			wrap: func(sel Selector) Selector { return Selector{Or: []Selector{sel}} },
		},
		{
			name: "or_indexed",
			expr: func(expr string) string { return "or.0." + expr },
			wrap: func(sel Selector) Selector { return Selector{Or: []Selector{sel}} },
		},
		{
			name: "not",
			expr: func(expr string) string { return "not." + expr },
			wrap: func(sel Selector) Selector { return Selector{Not: &sel} },
		},
		{
			name: "and_not",
			expr: func(expr string) string { return "and.not." + expr },
			wrap: func(sel Selector) Selector {
				return Selector{And: []Selector{{Not: &sel}}}
			},
		},
		{
			name: "or_not",
			expr: func(expr string) string { return "or.not." + expr },
			wrap: func(sel Selector) Selector {
				return Selector{Or: []Selector{{Not: &sel}}}
			},
		},
		{
			name: "and_or_indexed",
			expr: func(expr string) string { return "and.or.0." + expr },
			wrap: func(sel Selector) Selector {
				return Selector{And: []Selector{{Or: []Selector{sel}}}}
			},
		},
		{
			name: "or_and_indexed",
			expr: func(expr string) string { return "or.and.0." + expr },
			wrap: func(sel Selector) Selector {
				return Selector{Or: []Selector{{And: []Selector{sel}}}}
			},
		},
	}
	if len(filter) == 0 {
		return all
	}
	allowed := make(map[string]struct{}, len(filter))
	for _, name := range filter {
		allowed[name] = struct{}{}
	}
	out := make([]selectorParseWrapper, 0, len(filter))
	for _, wrapper := range all {
		if _, ok := allowed[wrapper.name]; ok {
			out = append(out, wrapper)
		}
	}
	return out
}

func selectorPositiveExpressionCases() []selectorExpressionCase {
	allWrappers := []string{
		"root",
		"and",
		"and_indexed",
		"or",
		"or_indexed",
		"not",
		"and_not",
		"or_not",
		"and_or_indexed",
		"or_and_indexed",
	}
	return []selectorExpressionCase{
		{
			name:     "eq",
			expr:     `eq{field=/status,value=open}`,
			want:     Selector{Eq: stringTerm("/status", "open", nil, false, true)},
			wrappers: allWrappers,
		},
		{
			name:     "eq_quoted_phrase",
			expr:     `eq{field=/status,value="hello world"}`,
			want:     Selector{Eq: stringTerm("/status", "hello world", nil, false, true)},
			wrappers: allWrappers,
		},
		{
			name:     "eq_numeric_literal",
			expr:     `eq{field=/count,value=42}`,
			want:     Selector{Eq: stringTerm("/count", "42", nil, false, true)},
			wrappers: allWrappers,
		},
		{
			name:     "eq_boolean_literal",
			expr:     `eq{field=/enabled,value=false}`,
			want:     Selector{Eq: stringTerm("/enabled", "false", nil, false, true)},
			wrappers: allWrappers,
		},
		{
			name:     "contains_value",
			expr:     `contains{field=/msg,value=timeout}`,
			want:     Selector{Contains: stringTerm("/msg", "timeout", nil, false, true)},
			wrappers: allWrappers,
		},
		{
			name:     "contains_any",
			expr:     `contains{field=/msg,any=timeout|error}`,
			want:     Selector{Contains: stringTerm("/msg", "", []string{"timeout", "error"}, false, false)},
			wrappers: allWrappers,
		},
		{
			name:     "contains_omitted_value",
			expr:     `contains{field=/msg}`,
			want:     Selector{Contains: stringTerm("/msg", "", nil, false, false)},
			wrappers: allWrappers,
		},
		{
			name:     "contains_explicit_empty_value",
			expr:     `contains{field=/msg,value=""}`,
			want:     Selector{Contains: stringTerm("/msg", "", nil, false, true)},
			wrappers: allWrappers,
		},
		{
			name:     "contains_ignore_case_true",
			expr:     `contains{field=/msg,value=timeout,ignoreCase=true}`,
			want:     Selector{Contains: stringTerm("/msg", "timeout", nil, true, true)},
			wrappers: allWrappers,
		},
		{
			name:     "contains_ignore_case_alias",
			expr:     `contains{f=/msg,v=timeout,ic=t}`,
			want:     Selector{Contains: stringTerm("/msg", "timeout", nil, true, true)},
			wrappers: allWrappers,
		},
		{
			name:     "icontains_value",
			expr:     `icontains{field=/msg,value=timeout}`,
			want:     Selector{IContains: stringTerm("/msg", "timeout", nil, false, true)},
			wrappers: allWrappers,
		},
		{
			name:     "icontains_any",
			expr:     `icontains{field=/msg,any=warn|timeout}`,
			want:     Selector{IContains: stringTerm("/msg", "", []string{"warn", "timeout"}, false, false)},
			wrappers: allWrappers,
		},
		{
			name:     "icontains_alias_any",
			expr:     `icontains{f=/msg,a=warn|timeout}`,
			want:     Selector{IContains: stringTerm("/msg", "", []string{"warn", "timeout"}, false, false)},
			wrappers: allWrappers,
		},
		{
			name:     "prefix",
			expr:     `prefix{field=/service,value=auth}`,
			want:     Selector{Prefix: stringTerm("/service", "auth", nil, false, true)},
			wrappers: allWrappers,
		},
		{
			name:     "prefix_omitted_value",
			expr:     `prefix{field=/service}`,
			want:     Selector{Prefix: stringTerm("/service", "", nil, false, false)},
			wrappers: allWrappers,
		},
		{
			name:     "iprefix",
			expr:     `iprefix{field=/service,value=auth}`,
			want:     Selector{IPrefix: stringTerm("/service", "auth", nil, false, true)},
			wrappers: allWrappers,
		},
		{
			name:     "iprefix_explicit_empty",
			expr:     `iprefix{field=/service,value=""}`,
			want:     Selector{IPrefix: stringTerm("/service", "", nil, false, true)},
			wrappers: allWrappers,
		},
		{
			name:     "range_numeric",
			expr:     `range{field=/progress,gte=10}`,
			want:     Selector{Range: &RangeTerm{Field: "/progress", GTE: NewNumericRangeBound(10)}},
			wrappers: allWrappers,
		},
		{
			name:     "range_gt_lte_numeric",
			expr:     `range{field=/progress,gt=10,lte=20}`,
			want:     Selector{Range: &RangeTerm{Field: "/progress", GT: NewNumericRangeBound(10), LTE: NewNumericRangeBound(20)}},
			wrappers: allWrappers,
		},
		{
			name:     "range_datetime",
			expr:     `range{field=/timestamp,gte=2026-03-05T10:28:21Z}`,
			want:     Selector{Range: &RangeTerm{Field: "/timestamp", GTE: NewDatetimeRangeBound("2026-03-05T10:28:21Z")}},
			wrappers: allWrappers,
		},
		{
			name:     "range_datetime_window",
			expr:     `range{field=/timestamp,gte=2026-03-05T10:28:21Z,lt=2026-03-05T10:30:00Z}`,
			want:     Selector{Range: &RangeTerm{Field: "/timestamp", GTE: NewDatetimeRangeBound("2026-03-05T10:28:21Z"), LT: NewDatetimeRangeBound("2026-03-05T10:30:00Z")}},
			wrappers: allWrappers,
		},
		{
			name:     "date_after_before",
			expr:     `date{field=/timestamp,after=2025-01-01,before=2025-01-03}`,
			want:     Selector{Date: &DateTerm{Field: "/timestamp", After: "2025-01-01", Before: "2025-01-03"}},
			wrappers: allWrappers,
		},
		{
			name:     "date_alias_after_before",
			expr:     `date{f=/timestamp,a=2025-01-01,b=2025-01-03}`,
			want:     Selector{Date: &DateTerm{Field: "/timestamp", After: "2025-01-01", Before: "2025-01-03"}},
			wrappers: allWrappers,
		},
		{
			name:     "date_value",
			expr:     `date{field=/timestamp,value=2025-01-01}`,
			want:     Selector{Date: &DateTerm{Field: "/timestamp", Value: "2025-01-01"}},
			wrappers: allWrappers,
		},
		{
			name:     "date_since",
			expr:     `date{field=/timestamp,since=yesterday}`,
			want:     Selector{Date: &DateTerm{Field: "/timestamp", Since: "yesterday"}},
			wrappers: allWrappers,
		},
		{
			name:     "date_gte_lt",
			expr:     `date{field=/timestamp,gte=2025-01-01,lt=2025-01-03}`,
			want:     Selector{Date: &DateTerm{Field: "/timestamp", GTE: "2025-01-01", LT: "2025-01-03"}},
			wrappers: allWrappers,
		},
		{
			name:     "in_any",
			expr:     `in{field=/env,any=prod|stage}`,
			want:     Selector{In: &InTerm{Field: "/env", Any: []string{"prod", "stage"}}},
			wrappers: allWrappers,
		},
		{
			name:     "in_any_quoted",
			expr:     `in{field=/env,any="prod|stage"}`,
			want:     Selector{In: &InTerm{Field: "/env", Any: []string{"prod", "stage"}}},
			wrappers: allWrappers,
		},
		{
			name:     "in_any_quoted_phrases",
			expr:     `in{field=/greeting,any="hello world|goodbye jupiter"}`,
			want:     Selector{In: &InTerm{Field: "/greeting", Any: []string{"hello world", "goodbye jupiter"}}},
			wrappers: allWrappers,
		},
		{
			name:     "in_alias",
			expr:     `in{f=/env,a=prod|stage|dev}`,
			want:     Selector{In: &InTerm{Field: "/env", Any: []string{"prod", "stage", "dev"}}},
			wrappers: allWrappers,
		},
		{
			name:     "exists",
			expr:     `exists{/meta/etag}`,
			want:     Selector{Exists: "/meta/etag"},
			wrappers: allWrappers,
		},
		{
			name:     "exists_recursive",
			expr:     `exists{/meta/.../etag}`,
			want:     Selector{Exists: "/meta/.../etag"},
			wrappers: allWrappers,
		},
	}
}

func selectorPositiveValuesCases() []selectorValuesCase {
	allWrappers := []string{
		"root",
		"and",
		"and_indexed",
		"or",
		"or_indexed",
		"not",
		"and_not",
		"or_not",
		"and_or_indexed",
		"or_and_indexed",
	}
	return []selectorValuesCase{
		{
			name: "eq",
			values: url.Values{
				"eq.field": []string{"/status"},
				"eq.value": []string{"open"},
			},
			want:     Selector{Eq: stringTerm("/status", "open", nil, false, true)},
			wrappers: allWrappers,
		},
		{
			name: "eq_numeric_literal",
			values: url.Values{
				"eq.field": []string{"/count"},
				"eq.value": []string{"42"},
			},
			want:     Selector{Eq: stringTerm("/count", "42", nil, false, true)},
			wrappers: allWrappers,
		},
		{
			name: "eq_aliases",
			values: url.Values{
				"eq.f": []string{"/status"},
				"eq.v": []string{"open"},
			},
			want:     Selector{Eq: stringTerm("/status", "open", nil, false, true)},
			wrappers: allWrappers,
		},
		{
			name: "eq_duplicate_identical_raw_keys",
			values: url.Values{
				"eq.field": []string{"/status", "/status"},
				"eq.value": []string{"open", "open"},
			},
			want:     Selector{Eq: stringTerm("/status", "open", nil, false, true)},
			wrappers: allWrappers,
		},
		{
			name: "contains_any",
			values: url.Values{
				"contains.field": []string{"/msg"},
				"contains.any":   []string{"timeout|error"},
			},
			want:     Selector{Contains: stringTerm("/msg", "", []string{"timeout", "error"}, false, false)},
			wrappers: allWrappers,
		},
		{
			name: "contains_ignore_case",
			values: url.Values{
				"contains.field":      []string{"/msg"},
				"contains.value":      []string{"timeout"},
				"contains.ignoreCase": []string{"t"},
			},
			want:     Selector{Contains: stringTerm("/msg", "timeout", nil, true, true)},
			wrappers: allWrappers,
		},
		{
			name: "contains_alias_any",
			values: url.Values{
				"contains.f": []string{"/msg"},
				"contains.a": []string{"timeout|error"},
			},
			want:     Selector{Contains: stringTerm("/msg", "", []string{"timeout", "error"}, false, false)},
			wrappers: allWrappers,
		},
		{
			name: "range_numeric",
			values: url.Values{
				"range.field": []string{"/progress"},
				"range.gte":   []string{"10"},
			},
			want:     Selector{Range: &RangeTerm{Field: "/progress", GTE: NewNumericRangeBound(10)}},
			wrappers: allWrappers,
		},
		{
			name: "range_datetime_window",
			values: url.Values{
				"range.field": []string{"/timestamp"},
				"range.gte":   []string{"2026-03-05T10:28:21Z"},
				"range.lt":    []string{"2026-03-05T10:30:00Z"},
			},
			want:     Selector{Range: &RangeTerm{Field: "/timestamp", GTE: NewDatetimeRangeBound("2026-03-05T10:28:21Z"), LT: NewDatetimeRangeBound("2026-03-05T10:30:00Z")}},
			wrappers: allWrappers,
		},
		{
			name: "date_after_before",
			values: url.Values{
				"date.field":  []string{"/timestamp"},
				"date.after":  []string{"2025-01-01"},
				"date.before": []string{"2025-01-03"},
			},
			want:     Selector{Date: &DateTerm{Field: "/timestamp", After: "2025-01-01", Before: "2025-01-03"}},
			wrappers: allWrappers,
		},
		{
			name: "date_alias_after_before",
			values: url.Values{
				"date.f": []string{"/timestamp"},
				"date.a": []string{"2025-01-01"},
				"date.b": []string{"2025-01-03"},
			},
			want:     Selector{Date: &DateTerm{Field: "/timestamp", After: "2025-01-01", Before: "2025-01-03"}},
			wrappers: allWrappers,
		},
		{
			name: "date_since",
			values: url.Values{
				"date.field": []string{"/timestamp"},
				"date.since": []string{"yesterday"},
			},
			want:     Selector{Date: &DateTerm{Field: "/timestamp", Since: "yesterday"}},
			wrappers: allWrappers,
		},
		{
			name: "date_value",
			values: url.Values{
				"date.field": []string{"/timestamp"},
				"date.value": []string{"2025-01-01"},
			},
			want:     Selector{Date: &DateTerm{Field: "/timestamp", Value: "2025-01-01"}},
			wrappers: allWrappers,
		},
		{
			name: "in_any",
			values: url.Values{
				"in.field": []string{"/env"},
				"in.any":   []string{"prod|stage"},
			},
			want:     Selector{In: &InTerm{Field: "/env", Any: []string{"prod", "stage"}}},
			wrappers: allWrappers,
		},
		{
			name: "in_any_phrases",
			values: url.Values{
				"in.field": []string{"/greeting"},
				"in.any":   []string{"hello world|goodbye jupiter"},
			},
			want:     Selector{In: &InTerm{Field: "/greeting", Any: []string{"hello world", "goodbye jupiter"}}},
			wrappers: allWrappers,
		},
		{
			name: "in_alias",
			values: url.Values{
				"in.f": []string{"/env"},
				"in.a": []string{"prod|stage|dev"},
			},
			want:     Selector{In: &InTerm{Field: "/env", Any: []string{"prod", "stage", "dev"}}},
			wrappers: allWrappers,
		},
		{
			name: "exists",
			values: url.Values{
				"exists": []string{"/meta/etag"},
			},
			want:     Selector{Exists: "/meta/etag"},
			wrappers: allWrappers,
		},
		{
			name: "exists_recursive",
			values: url.Values{
				"exists": []string{"/meta/.../etag"},
			},
			want:     Selector{Exists: "/meta/.../etag"},
			wrappers: allWrappers,
		},
	}
}

func selectorInvalidExpressionCases() []selectorInvalidExpressionCase {
	allWrappers := []string{
		"root",
		"and",
		"and_indexed",
		"or",
		"or_indexed",
		"not",
		"and_not",
		"or_not",
		"and_or_indexed",
		"or_and_indexed",
	}
	rootOnly := []string{"root"}
	stringOnly := []string{"ParseSelectorString", "ParseSelectorStringOr"}
	return []selectorInvalidExpressionCase{
		{name: "contains_value_and_any", expr: `contains{field=/msg,value=timeout,any=error}`, wrappers: allWrappers},
		{name: "icontains_value_and_any", expr: `icontains{field=/msg,value=timeout,any=error}`, wrappers: allWrappers},
		{name: "contains_duplicate_value_keys", expr: `contains{field=/msg,value=timeout,value=error}`, wrappers: allWrappers},
		{name: "eq_duplicate_field_keys", expr: `eq{field=/status,f=/other,value=open}`, wrappers: allWrappers},
		{name: "contains_empty_any", expr: `contains{field=/msg,any=}`, wrappers: allWrappers},
		{name: "contains_pipe_only_any", expr: `contains{field=/msg,any=||}`, wrappers: allWrappers},
		{name: "in_empty_any", expr: `in{field=/env,any=}`, wrappers: allWrappers},
		{name: "in_whitespace_any", expr: `in{field=/env,any= prod | stage }`, wrappers: allWrappers},
		{name: "in_missing_field", expr: `in{any=prod|stage}`, wrappers: allWrappers},
		{name: "in_missing_any", expr: `in{field=/env}`, wrappers: allWrappers},
		{name: "eq_any_unsupported", expr: `eq{field=/msg,any=foo|bar}`, wrappers: allWrappers},
		{name: "prefix_any_unsupported", expr: `prefix{field=/msg,any=foo|bar}`, wrappers: allWrappers},
		{name: "iprefix_any_unsupported", expr: `iprefix{field=/msg,any=foo|bar}`, wrappers: allWrappers},
		{name: "contains_invalid_ignore_case", expr: `contains{field=/msg,value=timeout,ignoreCase=maybe}`, wrappers: allWrappers},
		{name: "eq_unknown_field", expr: `eq{field=/status,value=open,foo=bar}`, wrappers: allWrappers},
		{name: "range_unknown_field", expr: `range{field=/progress,gte=10,foo=bar}`, wrappers: allWrappers},
		{name: "date_unknown_field", expr: `date{field=/timestamp,after=2025-01-01,foo=bar}`, wrappers: allWrappers},
		{name: "in_unknown_field", expr: `in{field=/env,any=prod|stage,foo=bar}`, wrappers: allWrappers},
		{name: "contains_missing_field", expr: `contains{value=timeout}`, wrappers: allWrappers},
		{name: "eq_missing_field", expr: `eq{value=open}`, wrappers: allWrappers},
		{name: "range_missing_bounds", expr: `range{field=/progress}`, wrappers: allWrappers},
		{name: "range_missing_field", expr: `range{gte=10}`, wrappers: allWrappers},
		{name: "range_mixed_numeric_datetime", expr: `range{field=/progress,gte=10,lt=2025-01-01}`, wrappers: allWrappers},
		{name: "range_invalid_datetime_literal", expr: `range{field=/timestamp,gte=yesterday}`, wrappers: allWrappers},
		{name: "date_since_with_after", expr: `date{field=/timestamp,since=yesterday,after=2025-01-01}`, wrappers: allWrappers},
		{name: "date_after_with_gt", expr: `date{field=/timestamp,after=2025-01-01,gt=2025-01-02}`, wrappers: allWrappers},
		{name: "date_before_with_lt", expr: `date{field=/timestamp,before=2025-01-03,lt=2025-01-02}`, wrappers: allWrappers},
		{name: "date_missing_field", expr: `date{after=2025-01-01}`, wrappers: allWrappers},
		{name: "date_invalid_since", expr: `date{field=/timestamp,since=tomorrowish}`, wrappers: allWrappers},
		{name: "exists_empty", expr: `exists{}`, wrappers: allWrappers},
		{name: "exists_duplicate_payload", expr: `exists{/meta/etag,field=/status}`, wrappers: allWrappers},
		{name: "exists_multiple_bare_values", expr: `exists{/meta/etag,/meta/id}`, wrappers: allWrappers},
		{name: "exists_keyed_value", expr: `exists{field=/meta/etag}`, wrappers: allWrappers},
		{name: "selector_path_empty_segment", expr: `and..eq{field=/status,value=open}`, wrappers: rootOnly},
		{name: "selector_path_unknown_segment", expr: `and.foo.eq{field=/status,value=open}`, wrappers: rootOnly},
		{name: "selector_path_unknown_segment_nested", expr: `or.0.and.foo.exists{/meta/etag}`, wrappers: rootOnly},
		{name: "selector_path_empty_segment_nested", expr: `or.0..eq{field=/status,value=open}`, wrappers: rootOnly},
		{name: "unknown_expression", expr: `nonsense`, wrappers: rootOnly, entrypoint: stringOnly},
		{name: "mixed_valid_and_unknown_expression", expr: `eq{field=/status,value=open},nonsense`, wrappers: rootOnly, entrypoint: stringOnly},
		{name: "unterminated_quote", expr: `eq{field=/status,value="open}`, wrappers: rootOnly, entrypoint: stringOnly},
		{name: "missing_closing_brace", expr: `and.eq{field=/status,value=open`, wrappers: rootOnly, entrypoint: stringOnly},
		{name: "unexpected_closing_brace", expr: `eq{field=/status,value=open}}`, wrappers: rootOnly, entrypoint: stringOnly},
		{name: "shorthand_missing_value", expr: `/count>=`, wrappers: rootOnly, entrypoint: stringOnly},
		{name: "shorthand_invalid_temporal_macro", expr: `/timestamp>=yesterday`, wrappers: rootOnly, entrypoint: stringOnly},
		{name: "explicit_or_conflict", expr: `or.0.eq{field=/status,value=open},or.0.eq{field=/status,value=closed}`, wrappers: rootOnly},
		{name: "explicit_and_conflict", expr: `and.0.eq{field=/status,value=open},and.0.eq{field=/status,value=closed}`, wrappers: rootOnly},
	}
}

func selectorInvalidValuesCases() []selectorInvalidValuesCase {
	return []selectorInvalidValuesCase{
		{
			name: "eq_missing_field",
			values: url.Values{
				"eq.value": []string{"open"},
			},
		},
		{
			name: "contains_value_and_any",
			values: url.Values{
				"contains.field": []string{"/msg"},
				"contains.value": []string{"timeout"},
				"contains.any":   []string{"error"},
			},
		},
		{
			name: "contains_invalid_ignore_case",
			values: url.Values{
				"contains.field":      []string{"/msg"},
				"contains.value":      []string{"timeout"},
				"contains.ignoreCase": []string{"maybe"},
			},
		},
		{
			name: "eq_unknown_field",
			values: url.Values{
				"eq.field": []string{"/status"},
				"eq.value": []string{"open"},
				"eq.foo":   []string{"bar"},
			},
		},
		{
			name: "range_unknown_field",
			values: url.Values{
				"range.field": []string{"/progress"},
				"range.gte":   []string{"10"},
				"range.foo":   []string{"bar"},
			},
		},
		{
			name: "date_unknown_field",
			values: url.Values{
				"date.field": []string{"/timestamp"},
				"date.after": []string{"2025-01-01"},
				"date.foo":   []string{"bar"},
			},
		},
		{
			name: "in_unknown_field",
			values: url.Values{
				"in.field": []string{"/env"},
				"in.any":   []string{"prod|stage"},
				"in.foo":   []string{"bar"},
			},
		},
		{
			name: "eq_alias_conflict",
			values: url.Values{
				"eq.field": []string{"/status"},
				"eq.f":     []string{"/other"},
				"eq.value": []string{"open"},
			},
		},
		{
			name: "eq_duplicate_raw_key_conflict",
			values: url.Values{
				"eq.field": []string{"/status", "/other"},
				"eq.value": []string{"open"},
			},
		},
		{
			name: "range_missing_bounds",
			values: url.Values{
				"range.field": []string{"/progress"},
			},
		},
		{
			name: "range_missing_field",
			values: url.Values{
				"range.gte": []string{"10"},
			},
		},
		{
			name: "range_mixed_numeric_datetime",
			values: url.Values{
				"range.field": []string{"/progress"},
				"range.gte":   []string{"10"},
				"range.lt":    []string{"2025-01-01"},
			},
		},
		{
			name: "date_since_with_after",
			values: url.Values{
				"date.field": []string{"/timestamp"},
				"date.since": []string{"yesterday"},
				"date.after": []string{"2025-01-01"},
			},
		},
		{
			name: "date_missing_field",
			values: url.Values{
				"date.after": []string{"2025-01-01"},
			},
		},
		{
			name: "date_after_with_gt",
			values: url.Values{
				"date.field": []string{"/timestamp"},
				"date.after": []string{"2025-01-01"},
				"date.gt":    []string{"2025-01-02"},
			},
		},
		{
			name: "in_empty_any",
			values: url.Values{
				"in.field": []string{"/env"},
				"in.any":   []string{""},
			},
		},
		{
			name: "in_missing_field",
			values: url.Values{
				"in.any": []string{"prod|stage"},
			},
		},
		{
			name: "in_missing_any",
			values: url.Values{
				"in.field": []string{"/env"},
			},
		},
		{
			name: "exists_duplicate_value_keys",
			values: url.Values{
				"exists":     []string{"/meta/etag"},
				"exists.foo": []string{"bar"},
			},
		},
		{
			name: "selector_path_empty_segment",
			values: url.Values{
				"and..eq.field": []string{"/status"},
				"and..eq.value": []string{"open"},
			},
		},
		{
			name: "selector_path_unknown_segment",
			values: url.Values{
				"and.foo.eq.field": []string{"/status"},
				"and.foo.eq.value": []string{"open"},
			},
		},
		{
			name: "selector_path_unknown_segment_nested",
			values: url.Values{
				"or.0.and.foo.exists": []string{"/meta/etag"},
			},
		},
	}
}

func wrapSelectorValues(base url.Values, wrapper selectorParseWrapper) url.Values {
	out := url.Values{}
	for key, vals := range base {
		wrappedKey := selectorValuesWrappedKey(key, wrapper)
		for _, value := range vals {
			out.Add(wrappedKey, value)
		}
	}
	return out
}

func selectorValuesWrappedKey(key string, wrapper selectorParseWrapper) string {
	switch wrapper.name {
	case "root":
		return key
	case "and":
		return "and." + key
	case "and_indexed":
		return "and.0." + key
	case "or":
		return "or." + key
	case "or_indexed":
		return "or.0." + key
	case "not":
		return "not." + key
	case "and_not":
		return "and.not." + key
	case "or_not":
		return "or.not." + key
	case "and_or_indexed":
		return "and.or.0." + key
	case "or_and_indexed":
		return "or.and.0." + key
	default:
		return key
	}
}

func stringTerm(field, value string, any []string, ignoreCase bool, valueSet bool) *Term {
	return &Term{
		Field:      field,
		Value:      value,
		Any:        any,
		IgnoreCase: ignoreCase,
		valueSet:   valueSet,
	}
}

func assertSelectorJSONEqual(t *testing.T, got, want Selector) {
	t.Helper()
	gotJSON := mustMarshalSelectorJSON(t, canonicalizeSelector(got))
	wantJSON := mustMarshalSelectorJSON(t, canonicalizeSelector(want))
	if gotJSON != wantJSON {
		t.Fatalf("selector mismatch\n got: %s\nwant: %s", gotJSON, wantJSON)
	}
}

func mustMarshalSelectorJSON(t *testing.T, sel Selector) string {
	t.Helper()
	payload, err := json.Marshal(sel)
	if err != nil {
		t.Fatalf("marshal selector: %v", err)
	}
	return string(payload)
}

func canonicalizeSelector(sel Selector) Selector {
	if sel.Not != nil {
		notSel := canonicalizeSelector(*sel.Not)
		sel.Not = &notSel
	}
	for i := range sel.And {
		sel.And[i] = canonicalizeSelector(sel.And[i])
	}
	for i := range sel.Or {
		sel.Or[i] = canonicalizeSelector(sel.Or[i])
	}
	slices.SortFunc(sel.And, func(a, b Selector) int {
		return strings.Compare(mustMarshalSelectorJSONForSort(a), mustMarshalSelectorJSONForSort(b))
	})
	slices.SortFunc(sel.Or, func(a, b Selector) int {
		return strings.Compare(mustMarshalSelectorJSONForSort(a), mustMarshalSelectorJSONForSort(b))
	})
	return sel
}

func mustMarshalSelectorJSONForSort(sel Selector) string {
	payload, err := json.Marshal(sel)
	if err != nil {
		panic(err)
	}
	return string(payload)
}
