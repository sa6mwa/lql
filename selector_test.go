package lql

import (
	"fmt"
	"net/url"
	"testing"
)

func TestParseSelectorValuesSimple(t *testing.T) {
	values := url.Values{}
	values.Set("eq.field", "/status")
	values.Set("eq.value", "open")
	sel, err := ParseSelectorValues(values)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if sel.Eq == nil || sel.Eq.Field != "/status" || sel.Eq.Value != "open" {
		t.Fatalf("unexpected selector %+v", sel)
	}
}

func TestParseSelectorValuesBrace(t *testing.T) {
	values := url.Values{}
	values.Set("and.eq{field=/status,value=open}", "")
	values.Set("or.eq{field=/owner,value=alice}", "")
	values.Set("or.1.eq{field=/owner,value=bob}", "")
	sel, err := ParseSelectorValues(values)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if len(sel.Or) != 0 {
		t.Fatalf("unexpected selector %+v", sel)
	}
	andClauses, orGroup := splitAndOrClausesSelectors(t, sel)
	if len(andClauses) != 1 {
		t.Fatalf("expected 1 and clause, got %+v", sel)
	}
	if len(orGroup.Or) != 2 {
		t.Fatalf("expected 2 or clauses, got %+v", sel)
	}
}

func TestParseSelectorString(t *testing.T) {
	expr := "eq{field=/status,value=open},or.eq{field=/owner,value=\"alice\"},or.1.eq{field=/owner,value=bob}"
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if len(sel.Or) != 0 {
		t.Fatalf("unexpected selector %+v", sel)
	}
	andClauses, orGroup := splitAndOrClausesSelectors(t, sel)
	if len(andClauses) != 1 {
		t.Fatalf("expected 1 and clause, got %+v", sel)
	}
	if len(orGroup.Or) != 2 {
		t.Fatalf("expected 2 or clauses, got %+v", sel)
	}
}

func TestParseSelectorStringWhitespace(t *testing.T) {
	expr := `and.eq{
field="/hello"
value="hi, world"},and.eq{field=/status value="okili dokili"}`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if len(sel.And) != 2 {
		t.Fatalf("unexpected selector %+v", sel)
	}
}

func TestParseSelectorStringInvalid(t *testing.T) {
	if _, err := ParseSelectorString("and.eq{field=/status,value=open"); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestParseSelectorStringOrClauses(t *testing.T) {
	expr := `/field="value",/status="ok",or.eq{field=/msg,value=ok},or.eq{field=/msg,value=done}`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatalf("expected selector, got %+v", sel)
	}
	if len(sel.Or) != 0 {
		t.Fatalf("expected no top-level or clauses, got %+v", sel)
	}
	andClauses, orGroup := splitAndOrClausesSelectors(t, sel)
	if len(andClauses) != 2 {
		t.Fatalf("expected 2 and clauses, got %+v", sel)
	}
	if len(orGroup.Or) != 2 {
		t.Fatalf("expected 2 or clauses, got %+v", sel)
	}
}

func TestParseSelectorBooleanValue(t *testing.T) {
	expr := `and.eq{field=/flag,value=true},and.eq{field=/other,value=false}`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() || len(sel.And) != 2 {
		t.Fatalf("expected two and clauses, got %+v", sel)
	}
	expected := map[string]string{
		"/flag":  "true",
		"/other": "false",
	}
	for _, clause := range sel.And {
		if clause.Eq == nil {
			t.Fatalf("missing eq clause: %+v", clause)
		}
		got, ok := expected[clause.Eq.Field]
		if !ok {
			t.Fatalf("unexpected field %q", clause.Eq.Field)
		}
		if clause.Eq.Value != got {
			t.Fatalf("field %q expected %q got %q", clause.Eq.Field, got, clause.Eq.Value)
		}
		delete(expected, clause.Eq.Field)
	}
	if len(expected) != 0 {
		t.Fatalf("missing clauses for %v", expected)
	}
}

func TestParseSelectorStringsAndSlice(t *testing.T) {
	sel, err := ParseSelectorStrings([]string{`/status="ok"`, `/msg="done"`})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() || len(sel.And) != 2 {
		t.Fatalf("expected two and clauses, got %+v", sel)
	}
	expected := map[string]string{
		"/status": "ok",
		"/msg":    "done",
	}
	for _, clause := range sel.And {
		if clause.Eq == nil {
			t.Fatalf("missing eq clause: %+v", clause)
		}
		got, ok := expected[clause.Eq.Field]
		if !ok {
			t.Fatalf("unexpected field %q", clause.Eq.Field)
		}
		if clause.Eq.Value != got {
			t.Fatalf("field %q expected %q got %q", clause.Eq.Field, got, clause.Eq.Value)
		}
		delete(expected, clause.Eq.Field)
	}
	if len(expected) != 0 {
		t.Fatalf("missing clauses for %v", expected)
	}
}

func TestParseSelectorStringsOrSlice(t *testing.T) {
	sel, err := ParseSelectorStringsOr([]string{`/status="ok"`, `/msg="done"`})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() || len(sel.Or) != 2 {
		t.Fatalf("expected two or clauses, got %+v", sel)
	}
	expected := map[string]string{
		"/status": "ok",
		"/msg":    "done",
	}
	for _, clause := range sel.Or {
		if clause.Eq == nil {
			t.Fatalf("missing eq clause: %+v", clause)
		}
		got, ok := expected[clause.Eq.Field]
		if !ok {
			t.Fatalf("unexpected field %q", clause.Eq.Field)
		}
		if clause.Eq.Value != got {
			t.Fatalf("field %q expected %q got %q", clause.Eq.Field, got, clause.Eq.Value)
		}
		delete(expected, clause.Eq.Field)
	}
	if len(expected) != 0 {
		t.Fatalf("missing clauses for %v", expected)
	}
}

func ExampleParseSelectorStrings() {
	sel, err := ParseSelectorStrings([]string{`/status="ok"`, `/msg="done"`})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(!sel.IsEmpty(), len(sel.And))
	// Output: true 2
}

func ExampleParseSelectorStringsOr() {
	sel, err := ParseSelectorStringsOr([]string{`/status="ok"`, `/msg="done"`})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(!sel.IsEmpty(), len(sel.Or))
	// Output: true 2
}

func ExampleParseSelectorStringOr() {
	sel, err := ParseSelectorStringOr(`/status="ok",/msg="done"`)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(!sel.IsEmpty(), len(sel.Or))
	// Output: true 2
}

func splitAndOrClausesSelectors(t *testing.T, sel Selector) ([]Selector, Selector) {
	t.Helper()
	var andClauses []Selector
	var orGroup Selector
	for _, clause := range sel.And {
		if len(clause.Or) > 0 {
			if len(orGroup.Or) > 0 {
				t.Fatalf("multiple or groups in selector %+v", sel)
			}
			orGroup = clause
			continue
		}
		andClauses = append(andClauses, clause)
	}
	return andClauses, orGroup
}

func TestParseSelectorStringOrWithCommaSeparated(t *testing.T) {
	sel, err := ParseSelectorStringOr(`/status="ok",/status="done"`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() || len(sel.Or) != 2 {
		t.Fatalf("expected two or clauses, got %+v", sel)
	}
}

func TestParseSelectorStringsOrWithCommaElement(t *testing.T) {
	sel, err := ParseSelectorStringsOr([]string{`/status="ok",/status="done"`})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatalf("expected selector")
	}
	if len(sel.Or) != 1 {
		t.Fatalf("expected one top-level or clause, got %+v", sel)
	}
	if len(sel.Or[0].Or) == 2 {
		return
	}
	andClauses, orGroup := splitAndOrClausesSelectors(t, sel.Or[0])
	if len(andClauses) != 0 {
		t.Fatalf("expected no and clauses, got %+v", andClauses)
	}
	if len(orGroup.Or) != 2 {
		t.Fatalf("expected two or clauses, got %+v", orGroup)
	}
}

func TestParseSelectorStringOrIgnoresEmptyTokens(t *testing.T) {
	sel, err := ParseSelectorStringOr(` ,  /status="ok" ,  `)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatalf("expected selector")
	}
	if sel.Eq == nil || sel.Eq.Field != "/status" || sel.Eq.Value != "ok" {
		t.Fatalf("unexpected selector %+v", sel)
	}
}

func TestParseSelectorStringOrExplicitAndPreserved(t *testing.T) {
	expr := `and.eq{field=/status,value=ok},/msg="done"`
	sel, err := ParseSelectorStringOr(expr)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatalf("expected selector")
	}
	andClauses, orGroup := splitAndOrClausesSelectors(t, sel)
	if len(andClauses) != 1 {
		t.Fatalf("expected 1 and clause, got %+v", andClauses)
	}
	if len(orGroup.Or) != 1 {
		t.Fatalf("expected 1 or clause, got %+v", orGroup)
	}
}

func TestParseSelectorStringImplicitAndConflicts(t *testing.T) {
	expr := `/status="ok",/status="done"`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	andClauses, _ := splitAndOrClausesSelectors(t, sel)
	if len(andClauses) != 2 {
		t.Fatalf("expected 2 and clauses, got %+v", andClauses)
	}
}

func TestParseSelectorStringExplicitAndIndexMerges(t *testing.T) {
	expr := `and.0.eq{field=/status,value=ok},and.0.range{field=/progress,gte=10}`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	andClauses, _ := splitAndOrClausesSelectors(t, sel)
	if len(andClauses) != 1 {
		t.Fatalf("expected 1 and clause, got %+v", andClauses)
	}
	clause := andClauses[0]
	if clause.Eq == nil || clause.Eq.Field != "/status" || clause.Eq.Value != "ok" {
		t.Fatalf("unexpected eq clause %+v", clause)
	}
	if clause.Range == nil || clause.Range.Field != "/progress" {
		t.Fatalf("unexpected range clause %+v", clause)
	}
	if value, ok := clause.Range.GTE.Number(); !ok || value != 10 {
		t.Fatalf("unexpected range clause %+v", clause)
	}
}

func TestParseSelectorStringExplicitOrIndexMerges(t *testing.T) {
	expr := `or.0.eq{field=/status,value=ok},or.0.range{field=/progress,gte=10}`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(sel.Or) != 1 {
		t.Fatalf("expected 1 top-level or clause, got %+v", sel)
	}
	clause := sel.Or[0]
	if clause.Eq == nil || clause.Eq.Field != "/status" || clause.Eq.Value != "ok" {
		t.Fatalf("unexpected eq clause %+v", clause)
	}
	if clause.Range == nil || clause.Range.Field != "/progress" {
		t.Fatalf("unexpected range clause %+v", clause)
	}
	if value, ok := clause.Range.GTE.Number(); !ok || value != 10 {
		t.Fatalf("unexpected range clause %+v", clause)
	}
}

func TestParseSelectorStringExplicitOrIndexConflict(t *testing.T) {
	expr := `or.0.eq{field=/status,value=ok},or.0.eq{field=/status,value=done}`
	if _, err := ParseSelectorString(expr); err == nil {
		t.Fatal("expected parse conflict for duplicate or index")
	}
}

func TestParseSelectorStringExplicitAndIndexConflict(t *testing.T) {
	expr := `and.0.eq{field=/status,value=ok},and.0.eq{field=/status,value=done}`
	if _, err := ParseSelectorString(expr); err == nil {
		t.Fatal("expected parse conflict for duplicate and index")
	}
}

func TestParseSelectorStringsAndIgnoresEmpty(t *testing.T) {
	sel, err := ParseSelectorStrings([]string{"", "   ", `/status="ok"`})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() || len(sel.And) != 1 {
		t.Fatalf("expected one and clause, got %+v", sel)
	}
}

func TestParseSelectorStringsOrNestedAndClause(t *testing.T) {
	sel, err := ParseSelectorStringsOr([]string{`/status="ok",/msg="done"`})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() || len(sel.Or) != 1 {
		t.Fatalf("expected one or clause, got %+v", sel)
	}
	if len(sel.Or[0].And) != 0 || len(sel.Or[0].Or) != 2 {
		t.Fatalf("expected nested or clauses, got %+v", sel.Or[0])
	}
}

func TestParseSelectorShorthand(t *testing.T) {
	t.Run("equality", func(t *testing.T) {
		sel, err := ParseSelectorString(`/status="open"`)
		if err != nil {
			t.Fatalf("parse shorthand eq: %v", err)
		}
		if sel.Eq == nil || sel.Eq.Field != "/status" || sel.Eq.Value != "open" {
			t.Fatalf("unexpected eq selector %+v", sel)
		}
	})

	t.Run("not equal", func(t *testing.T) {
		sel, err := ParseSelectorString(`/type!=critical`)
		if err != nil {
			t.Fatalf("parse shorthand !=: %v", err)
		}
		if sel.Not == nil || sel.Not.Eq == nil || sel.Not.Eq.Field != "/type" || sel.Not.Eq.Value != "critical" {
			t.Fatalf("unexpected not selector %+v", sel)
		}
	})

	t.Run("greater than", func(t *testing.T) {
		sel, err := ParseSelectorString(`/progress/count>10`)
		if err != nil {
			t.Fatalf("parse shorthand >: %v", err)
		}
		if sel.Range == nil || sel.Range.Field != "/progress/count" {
			t.Fatalf("unexpected range selector %+v", sel.Range)
		}
		if value, ok := sel.Range.GT.Number(); !ok || value != 10 {
			t.Fatalf("unexpected range selector %+v", sel.Range)
		}
	})

	t.Run("gte with spaces", func(t *testing.T) {
		sel, err := ParseSelectorString("  /progress/count   >=   42  ")
		if err != nil {
			t.Fatalf("parse shorthand >=: %v", err)
		}
		if sel.Range == nil {
			t.Fatalf("unexpected gte selector %+v", sel.Range)
		}
		if value, ok := sel.Range.GTE.Number(); !ok || value != 42 {
			t.Fatalf("unexpected gte selector %+v", sel.Range)
		}
	})

	t.Run("lte numeric", func(t *testing.T) {
		sel, err := ParseSelectorString(`/battery_mv<=3600`)
		if err != nil {
			t.Fatalf("parse shorthand <=: %v", err)
		}
		if sel.Range == nil {
			t.Fatalf("unexpected lte selector %+v", sel.Range)
		}
		if value, ok := sel.Range.LTE.Number(); !ok || value != 3600 {
			t.Fatalf("unexpected lte selector %+v", sel.Range)
		}
	})

	t.Run("invalid missing value", func(t *testing.T) {
		if _, err := ParseSelectorString(`/count>=`); err == nil {
			t.Fatalf("expected error for missing value")
		}
	})

	t.Run("datetime gte", func(t *testing.T) {
		sel, err := ParseSelectorString(`/timestamp>="2025-01-01"`)
		if err != nil {
			t.Fatalf("parse shorthand datetime >=: %v", err)
		}
		if sel.Range == nil {
			t.Fatalf("expected range clause")
		}
		if value, ok := sel.Range.GTE.DateTime(); !ok || value != "2025-01-01" {
			t.Fatalf("unexpected datetime gte selector %+v", sel.Range)
		}
	})

	t.Run("datetime lt", func(t *testing.T) {
		sel, err := ParseSelectorString(`/timestamp<2025-01-01T12:00:00Z`)
		if err != nil {
			t.Fatalf("parse shorthand datetime <: %v", err)
		}
		if sel.Range == nil {
			t.Fatalf("expected range clause")
		}
		if value, ok := sel.Range.LT.DateTime(); !ok || value != "2025-01-01T12:00:00Z" {
			t.Fatalf("unexpected datetime lt selector %+v", sel.Range)
		}
	})

	t.Run("range macro disallowed", func(t *testing.T) {
		if _, err := ParseSelectorString(`/timestamp>=yesterday`); err == nil {
			t.Fatalf("expected error for relative macro in shorthand range")
		}
	})
}

func TestParseSelectorDateSelectorAliases(t *testing.T) {
	sel, err := ParseSelectorString(`date{f=/timestamp,a=2025-01-01,b=2025-01-03}`)
	if err != nil {
		t.Fatalf("parse date selector: %v", err)
	}
	if sel.Date == nil {
		t.Fatalf("expected date clause")
	}
	if sel.Date.Field != "/timestamp" {
		t.Fatalf("unexpected date field: %+v", sel.Date)
	}
	if sel.Date.After != "2025-01-01" || sel.Date.Before != "2025-01-03" {
		t.Fatalf("unexpected date aliases: %+v", sel.Date)
	}
}

func TestParseSelectorDateSelectorSinceMacro(t *testing.T) {
	sel, err := ParseSelectorString(`date{f=/timestamp,since=yesterday}`)
	if err != nil {
		t.Fatalf("parse date selector since: %v", err)
	}
	if sel.Date == nil || sel.Date.Since != "yesterday" {
		t.Fatalf("unexpected date since clause %+v", sel.Date)
	}
}

func TestParseSelectorInlineAliases(t *testing.T) {
	expr := `eq{f=/status,v=ok},in{f=/env,a=prod|stage}`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatalf("expected selector")
	}
	andClauses, orGroup := splitAndOrClausesSelectors(t, sel)
	if len(orGroup.Or) != 0 {
		t.Fatalf("expected no or clauses, got %+v", sel)
	}
	if len(andClauses) != 2 {
		t.Fatalf("expected 2 and clauses, got %+v", andClauses)
	}
}

func TestParseSelectorContainsVariants(t *testing.T) {
	t.Run("contains with ic alias true", func(t *testing.T) {
		sel, err := ParseSelectorString(`contains{f=/msg,v=timeout,ic=t}`)
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		if sel.Contains == nil {
			t.Fatalf("expected contains clause, got %+v", sel)
		}
		if sel.Contains.Field != "/msg" || sel.Contains.Value != "timeout" {
			t.Fatalf("unexpected contains clause %+v", sel.Contains)
		}
		if !sel.Contains.IgnoreCase {
			t.Fatalf("expected contains ignoreCase=true, got %+v", sel.Contains)
		}
	})

	t.Run("contains with ignoreCase false shorthand", func(t *testing.T) {
		sel, err := ParseSelectorString(`contains{field=/msg,value=timeout,ignoreCase=f}`)
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		if sel.Contains == nil {
			t.Fatalf("expected contains clause, got %+v", sel)
		}
		if sel.Contains.IgnoreCase {
			t.Fatalf("expected contains ignoreCase=false, got %+v", sel.Contains)
		}
	})

	t.Run("icontains clause", func(t *testing.T) {
		sel, err := ParseSelectorString(`icontains{field=/msg,value=timeout}`)
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		if sel.IContains == nil || sel.IContains.Field != "/msg" || sel.IContains.Value != "timeout" {
			t.Fatalf("unexpected icontains clause %+v", sel)
		}
	})

	t.Run("iprefix clause", func(t *testing.T) {
		sel, err := ParseSelectorString(`iprefix{field=/service,value=auth-}`)
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		if sel.IPrefix == nil || sel.IPrefix.Field != "/service" || sel.IPrefix.Value != "auth-" {
			t.Fatalf("unexpected iprefix clause %+v", sel)
		}
	})

	t.Run("invalid ignoreCase value", func(t *testing.T) {
		if _, err := ParseSelectorString(`contains{field=/msg,value=timeout,ignoreCase=maybe}`); err == nil {
			t.Fatal("expected parse error for invalid ignoreCase value")
		}
	})
}

func TestParseSelectorContainsAnyVariants(t *testing.T) {
	t.Run("contains any stays single clause with any values", func(t *testing.T) {
		sel, err := ParseSelectorString(`contains{f=/msg,a=timeout|error}`)
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		if sel.Contains == nil {
			t.Fatalf("expected contains clause, got %+v", sel)
		}
		if len(sel.Contains.Any) != 2 {
			t.Fatalf("expected contains.any with 2 values, got %+v", sel.Contains)
		}
		if sel.Contains.Any[0] != "timeout" || sel.Contains.Any[1] != "error" {
			t.Fatalf("unexpected contains.any values: %+v", sel.Contains.Any)
		}
	})

	t.Run("icontains any stays single clause with any values", func(t *testing.T) {
		sel, err := ParseSelectorString(`icontains{field=/msg,any=timeout|error}`)
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		if sel.IContains == nil {
			t.Fatalf("expected icontains clause, got %+v", sel)
		}
		if len(sel.IContains.Any) != 2 {
			t.Fatalf("expected icontains.any with 2 values, got %+v", sel.IContains)
		}
		if sel.IContains.Any[0] != "timeout" || sel.IContains.Any[1] != "error" {
			t.Fatalf("unexpected icontains.any values: %+v", sel.IContains.Any)
		}
	})

	t.Run("contains any single keyword keeps any form", func(t *testing.T) {
		sel, err := ParseSelectorString(`contains{f=/msg,a=timeout}`)
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		if sel.Contains == nil || len(sel.Contains.Any) != 1 || sel.Contains.Any[0] != "timeout" {
			t.Fatalf("expected contains.any with timeout, got %+v", sel)
		}
	})
}

func TestParseSelectorContainsAnyFailures(t *testing.T) {
	cases := []string{
		`contains{f=/msg,v=timeout,a=error}`,
		`contains{f=/msg,v="",a=error}`,
		`icontains{f=/msg,value=timeout,any=error}`,
		`contains{f=/msg,a=}`,
		`contains{f=/msg,a=||}`,
		`eq{f=/msg,a=foo|bar}`,
		`prefix{f=/msg,a=foo|bar}`,
		`iprefix{f=/msg,a=foo|bar}`,
	}
	for _, expr := range cases {
		expr := expr
		t.Run(expr, func(t *testing.T) {
			if _, err := ParseSelectorString(expr); err == nil {
				t.Fatalf("expected parse error for %q", expr)
			}
		})
	}
}

func TestParseSelectorMatchAllAliases(t *testing.T) {
	cases := []string{"", "{}", ".", "/"}
	for _, expr := range cases {
		expr := expr
		t.Run(expr, func(t *testing.T) {
			sel, err := ParseSelectorString(expr)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			if !sel.IsEmpty() {
				t.Fatalf("expected match-all selector alias %q to produce empty selector, got %+v", expr, sel)
			}
		})
	}
}

func TestParseSelectorStringTermEmptyValueSimplifiesToMatchAll(t *testing.T) {
	cases := []string{
		`contains{f=/,v=""}`,
		`icontains{f=/,v=""}`,
		`prefix{f=/,v=""}`,
		`iprefix{f=/,v=""}`,
		`contains{f=/*,v=""}`,
		`icontains{f=/...,v=""}`,
		`contains{f=/*}`,
		`icontains{f=/...}`,
	}
	for _, expr := range cases {
		expr := expr
		t.Run(expr, func(t *testing.T) {
			sel, err := ParseSelectorString(expr)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			if !sel.IsEmpty() {
				t.Fatalf("expected match-all selector for %q, got %+v", expr, sel)
			}
		})
	}
}

func TestParseSelectorStringTermEmptyValueKeepsFieldScopedClauses(t *testing.T) {
	cases := []string{
		`contains{f=/msg}`,
		`contains{f=/msg,v=""}`,
		`icontains{f=/msg}`,
		`prefix{f=/name}`,
		`iprefix{f=/name,v=""}`,
		`contains{f=/root/*}`,
	}
	for _, expr := range cases {
		expr := expr
		t.Run(expr, func(t *testing.T) {
			sel, err := ParseSelectorString(expr)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}
			if sel.IsEmpty() {
				t.Fatalf("expected field-scoped selector for %q, got match-all", expr)
			}
		})
	}
}

func TestParseSelectorInAnyAliases(t *testing.T) {
	expr := `in{f=/env,a=prod|stage|dev}`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.In == nil || len(sel.In.Any) != 3 {
		t.Fatalf("expected in.any with 3 entries, got %+v", sel)
	}
}

func TestParseSelectorInAnyEmpty(t *testing.T) {
	expr := `in{field=/env,any=}`
	if _, err := ParseSelectorString(expr); err == nil {
		t.Fatal("expected parse error for empty in.any")
	}
}

func TestParseSelectorInAnyWhitespace(t *testing.T) {
	expr := `in{field=/env,any= prod | stage }`
	if _, err := ParseSelectorString(expr); err == nil {
		t.Fatal("expected parse error for whitespace-separated any values")
	}
}

func TestParseSelectorStringRegression(t *testing.T) {
	inputs := map[string]string{
		"NegativeIndex":  "And.-1",
		"GarbageUnicode": "ANd.\u0970\x8b.30zz v1\nst",
		"HugeIndex":      "ANd.066666666000",
	}
	for name, input := range inputs {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("parse panicked for %q: %v", input, r)
				}
			}()
			_, _ = ParseSelectorString(input)
		})
	}
}
