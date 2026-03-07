package lql

import "testing"

func TestNewNumericRangeBound(t *testing.T) {
	bound := NewNumericRangeBound(42.5)
	if bound == nil {
		t.Fatalf("expected bound")
	}
	value, ok := bound.Number()
	if !ok || value != 42.5 {
		t.Fatalf("unexpected numeric bound: value=%v ok=%v", value, ok)
	}
	if text, ok := bound.DateTime(); ok || text != "" {
		t.Fatalf("expected numeric-only bound, got datetime=%q ok=%v", text, ok)
	}
}

func TestNewDatetimeRangeBound(t *testing.T) {
	bound := NewDatetimeRangeBound(" 2026-03-05T11:28:21+01:00 ")
	if bound == nil {
		t.Fatalf("expected bound")
	}
	text, ok := bound.DateTime()
	if !ok || text != "2026-03-05T11:28:21+01:00" {
		t.Fatalf("unexpected datetime bound: value=%q ok=%v", text, ok)
	}
	if value, ok := bound.Number(); ok || value != 0 {
		t.Fatalf("expected datetime-only bound, got number=%v ok=%v", value, ok)
	}
	if !bound.temporal.ready || !bound.temporal.valid {
		t.Fatalf("expected temporal cache primed for datetime bound: %+v", bound.temporal)
	}
}

func TestNewDatetimeRangeBoundWhitespaceOnly(t *testing.T) {
	bound := NewDatetimeRangeBound("   ")
	if bound == nil {
		t.Fatalf("expected bound")
	}
	if text, ok := bound.DateTime(); ok || text != "" {
		t.Fatalf("expected empty datetime bound, got value=%q ok=%v", text, ok)
	}
	if bound.temporal.ready {
		t.Fatalf("expected no temporal cache for empty datetime bound")
	}
}
