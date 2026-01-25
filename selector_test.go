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
	if clause.Range == nil || clause.Range.Field != "/progress" || clause.Range.GTE == nil || *clause.Range.GTE != 10 {
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
	if clause.Range == nil || clause.Range.Field != "/progress" || clause.Range.GTE == nil || *clause.Range.GTE != 10 {
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
		if sel.Range == nil || sel.Range.Field != "/progress/count" || sel.Range.GT == nil || *sel.Range.GT != 10 {
			t.Fatalf("unexpected range selector %+v", sel.Range)
		}
	})

	t.Run("gte with spaces", func(t *testing.T) {
		sel, err := ParseSelectorString("  /progress/count   >=   42  ")
		if err != nil {
			t.Fatalf("parse shorthand >=: %v", err)
		}
		if sel.Range == nil || sel.Range.GTE == nil || *sel.Range.GTE != 42 {
			t.Fatalf("unexpected gte selector %+v", sel.Range)
		}
	})

	t.Run("lte numeric", func(t *testing.T) {
		sel, err := ParseSelectorString(`/battery_mv<=3600`)
		if err != nil {
			t.Fatalf("parse shorthand <=: %v", err)
		}
		if sel.Range == nil || sel.Range.LTE == nil || *sel.Range.LTE != 3600 {
			t.Fatalf("unexpected lte selector %+v", sel.Range)
		}
	})

	t.Run("invalid missing value", func(t *testing.T) {
		if _, err := ParseSelectorString(`/count>=`); err == nil {
			t.Fatalf("expected error for missing value")
		}
	})
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
