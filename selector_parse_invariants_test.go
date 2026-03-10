package lql

import (
	"net/url"
	"slices"
	"strings"
	"testing"
)

func TestSelectorParseConformanceBraceAliasPermutationMatrix(t *testing.T) {
	cases := []struct {
		name       string
		term       string
		entries    map[string]string
		aliases    map[string][]string
		want       Selector
		wrappers   []string
		entrypoint []selectorParseEntryPoint
	}{
		{
			name: "eq",
			term: "eq",
			entries: map[string]string{
				"field": "/status",
				"value": "open",
			},
			aliases: map[string][]string{
				"field": {"field", "f"},
				"value": {"value", "v"},
			},
			want:       Selector{Eq: stringTerm("/status", "open", nil, false, true)},
			wrappers:   []string{"root", "and", "or", "and_or_indexed"},
			entrypoint: selectorStringSingleClauseEntryPoints(),
		},
		{
			name: "contains_ignore_case",
			term: "contains",
			entries: map[string]string{
				"field":      "/msg",
				"value":      "timeout",
				"ignoreCase": "t",
			},
			aliases: map[string][]string{
				"field":      {"field", "f"},
				"value":      {"value", "v"},
				"ignoreCase": {"ignoreCase", "ic"},
			},
			want:       Selector{Contains: stringTerm("/msg", "timeout", nil, true, true)},
			wrappers:   []string{"root", "and", "or", "and_or_indexed"},
			entrypoint: selectorStringSingleClauseEntryPoints(),
		},
		{
			name: "date_after_before",
			term: "date",
			entries: map[string]string{
				"field":  "/timestamp",
				"after":  "2025-01-01",
				"before": "2025-01-03",
			},
			aliases: map[string][]string{
				"field":  {"field", "f"},
				"after":  {"after", "a"},
				"before": {"before", "b"},
			},
			want:       Selector{Date: &DateTerm{Field: "/timestamp", After: "2025-01-01", Before: "2025-01-03"}},
			wrappers:   []string{"root", "and", "or", "and_or_indexed"},
			entrypoint: selectorStringSingleClauseEntryPoints(),
		},
		{
			name: "in_any",
			term: "in",
			entries: map[string]string{
				"field": "/env",
				"any":   "prod|stage|dev",
			},
			aliases: map[string][]string{
				"field": {"field", "f"},
				"any":   {"any", "a"},
			},
			want:       Selector{In: &InTerm{Field: "/env", Any: []string{"prod", "stage", "dev"}}},
			wrappers:   []string{"root", "and", "or", "and_or_indexed"},
			entrypoint: selectorStringSingleClauseEntryPoints(),
		},
	}

	for _, tc := range cases {
		tc := tc
		for _, combo := range assignmentAliasCombinations(tc.entries, tc.aliases) {
			combo := combo
			for _, ordered := range permuteStrings(combo) {
				ordered := ordered
				for _, expr := range shortBraceExpressionVariants(tc.term, ordered) {
					expr := expr
					for _, wrapper := range selectorParseWrappers(tc.wrappers) {
						wrapper := wrapper
						want := wrapper.wrap(tc.want)
						name := strings.Join([]string{tc.name, wrapper.name, sanitizeTestName(expr)}, "/")
						t.Run(name, func(t *testing.T) {
							for _, ep := range tc.entrypoint {
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
}

func TestSelectorParseConformanceValuesAliasPermutationMatrix(t *testing.T) {
	cases := []struct {
		name     string
		term     string
		entries  map[string]string
		aliases  map[string][]string
		want     Selector
		wrappers []string
	}{
		{
			name: "eq",
			term: "eq",
			entries: map[string]string{
				"field": "/status",
				"value": "open",
			},
			aliases: map[string][]string{
				"field": {"field", "f"},
				"value": {"value", "v"},
			},
			want:     Selector{Eq: stringTerm("/status", "open", nil, false, true)},
			wrappers: []string{"root", "and", "or", "and_or_indexed"},
		},
		{
			name: "contains_any",
			term: "contains",
			entries: map[string]string{
				"field": "/msg",
				"any":   "timeout|error",
			},
			aliases: map[string][]string{
				"field": {"field", "f"},
				"any":   {"any", "a"},
			},
			want:     Selector{Contains: stringTerm("/msg", "", []string{"timeout", "error"}, false, false)},
			wrappers: []string{"root", "and", "or", "and_or_indexed"},
		},
		{
			name: "date_after_before",
			term: "date",
			entries: map[string]string{
				"field":  "/timestamp",
				"after":  "2025-01-01",
				"before": "2025-01-03",
			},
			aliases: map[string][]string{
				"field":  {"field", "f"},
				"after":  {"after", "a"},
				"before": {"before", "b"},
			},
			want:     Selector{Date: &DateTerm{Field: "/timestamp", After: "2025-01-01", Before: "2025-01-03"}},
			wrappers: []string{"root", "and", "or", "and_or_indexed"},
		},
		{
			name: "in_any",
			term: "in",
			entries: map[string]string{
				"field": "/env",
				"any":   "prod|stage",
			},
			aliases: map[string][]string{
				"field": {"field", "f"},
				"any":   {"any", "a"},
			},
			want:     Selector{In: &InTerm{Field: "/env", Any: []string{"prod", "stage"}}},
			wrappers: []string{"root", "and", "or", "and_or_indexed"},
		},
	}

	for _, tc := range cases {
		tc := tc
		for _, values := range dottedAliasCombinations(tc.term, tc.entries, tc.aliases) {
			values := values
			for _, wrapper := range selectorParseWrappers(tc.wrappers) {
				wrapper := wrapper
				name := strings.Join([]string{tc.name, wrapper.name, sanitizeTestName(values.Encode())}, "/")
				t.Run(name, func(t *testing.T) {
					got, err := ParseSelectorValues(wrapSelectorValues(values, wrapper))
					if err != nil {
						t.Fatalf("parse values failed: %v", err)
					}
					assertSelectorJSONEqual(t, got, wrapper.wrap(tc.want))
				})
			}
		}
	}
}

func TestSelectorParseConformanceDeepIndexedMergeMatrix(t *testing.T) {
	cases := []struct {
		name string
		expr string
		want Selector
	}{
		{
			name: "and_or_and_merge",
			expr: `and.0.or.0.and.0.eq{field=/status,value=open},and.0.or.0.and.0.range{field=/progress,gte=10}`,
			want: Selector{
				And: []Selector{
					{
						Or: []Selector{
							{
								And: []Selector{
									{
										Eq:    stringTerm("/status", "open", nil, false, true),
										Range: &RangeTerm{Field: "/progress", GTE: NewNumericRangeBound(10)},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "or_and_or_merge",
			expr: `or.0.and.0.or.0.eq{field=/status,value=open},or.0.and.0.or.0.exists{/meta/etag}`,
			want: Selector{
				Or: []Selector{
					{
						And: []Selector{
							{
								Or: []Selector{
									{
										Eq:     stringTerm("/status", "open", nil, false, true),
										Exists: "/meta/etag",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "and_or_not_in",
			expr: `and.0.or.0.not.in{field=/env,any=prod|stage}`,
			want: Selector{
				And: []Selector{
					{
						Or: []Selector{
							{
								Not: &Selector{
									In: &InTerm{Field: "/env", Any: []string{"prod", "stage"}},
								},
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
			got, err := ParseSelectorString(tc.expr)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			assertSelectorJSONEqual(t, got, tc.want)
		})
	}
}

func TestSelectorParseConformanceDeepIndexedConflictMatrix(t *testing.T) {
	cases := []string{
		`and.0.or.0.and.0.eq{field=/status,value=open},and.0.or.0.and.0.eq{field=/status,value=closed}`,
		`or.0.and.0.or.0.exists{/meta/etag},or.0.and.0.or.0.exists{/meta/id}`,
		`and.0.or.0.not.in{field=/env,any=prod|stage},and.0.or.0.not.in{field=/env,any=dev}`,
	}

	for _, expr := range cases {
		expr := expr
		t.Run(sanitizeTestName(expr), func(t *testing.T) {
			if _, err := ParseSelectorString(expr); err == nil {
				t.Fatalf("expected parse conflict for %q", expr)
			}
		})
	}
}

func TestSelectorParseConformanceBraceAndValuesParity(t *testing.T) {
	cases := []struct {
		name   string
		expr   string
		values url.Values
	}{
		{
			name: "eq",
			expr: `eq{field=/status,value=open}`,
			values: url.Values{
				"eq.field": []string{"/status"},
				"eq.value": []string{"open"},
			},
		},
		{
			name: "contains_any",
			expr: `contains{f=/msg,a=timeout|error}`,
			values: url.Values{
				"contains.field": []string{"/msg"},
				"contains.any":   []string{"timeout|error"},
			},
		},
		{
			name: "date_after_before",
			expr: `date{f=/timestamp,a=2025-01-01,b=2025-01-03}`,
			values: url.Values{
				"date.field":  []string{"/timestamp"},
				"date.after":  []string{"2025-01-01"},
				"date.before": []string{"2025-01-03"},
			},
		},
		{
			name: "in_any",
			expr: `in{f=/env,a=prod|stage|dev}`,
			values: url.Values{
				"in.field": []string{"/env"},
				"in.any":   []string{"prod|stage|dev"},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parsedExpr, err := ParseSelectorString(tc.expr)
			if err != nil {
				t.Fatalf("parse expr failed: %v", err)
			}
			parsedValues, err := ParseSelectorValues(tc.values)
			if err != nil {
				t.Fatalf("parse values failed: %v", err)
			}
			assertSelectorJSONEqual(t, parsedExpr, parsedValues)
		})
	}
}

func assignmentAliasCombinations(entries map[string]string, aliases map[string][]string) [][]string {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	var out [][]string
	var walk func(idx int, current []string)
	walk = func(idx int, current []string) {
		if idx == len(keys) {
			out = append(out, append([]string(nil), current...))
			return
		}
		key := keys[idx]
		options := aliases[key]
		if len(options) == 0 {
			options = []string{key}
		}
		for _, alias := range options {
			entry := alias + "=" + entries[key]
			walk(idx+1, append(current, entry))
		}
	}
	walk(0, nil)
	return out
}

func dottedAliasCombinations(term string, entries map[string]string, aliases map[string][]string) []url.Values {
	combinations := assignmentAliasCombinations(entries, aliases)
	out := make([]url.Values, 0, len(combinations))
	for _, combo := range combinations {
		values := url.Values{}
		for _, assignment := range combo {
			parts := strings.SplitN(assignment, "=", 2)
			values.Set(term+"."+parts[0], parts[1])
		}
		out = append(out, values)
	}
	return out
}

func shortBraceExpressionVariants(term string, assignments []string) []string {
	return []string{
		term + "{" + strings.Join(assignments, ",") + "}",
		term + "{ " + strings.Join(assignments, ", ") + " }",
	}
}
