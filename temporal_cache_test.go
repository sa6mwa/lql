package lql

import (
	"testing"
	"time"
)

func TestTermTemporalLiteralValueUsesCache(t *testing.T) {
	cached := temporalValue{
		instant:  time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC),
		year:     2025,
		month:    time.January,
		day:      2,
		dateOnly: false,
	}
	term := &Term{
		Value: "not-a-date",
		temporal: temporalLiteralCache{
			raw:   "not-a-date",
			value: cached,
			ready: true,
			valid: true,
		},
	}

	got, ok := termTemporalLiteralValue(term)
	if !ok {
		t.Fatalf("expected cached temporal value")
	}
	if !got.instant.Equal(cached.instant) || got.dateOnly != cached.dateOnly {
		t.Fatalf("unexpected cached temporal result: %+v", got)
	}
}

func TestCompileTemporalRangeBoundsUsesCache(t *testing.T) {
	cached := temporalValue{
		instant:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		year:     2025,
		month:    time.January,
		day:      1,
		dateOnly: true,
	}
	term := &RangeTerm{
		Field: "/timestamp",
		GTE: &RangeBound{
			datetime: "not-a-date",
			temporal: temporalLiteralCache{
				raw:   "not-a-date",
				value: cached,
				ready: true,
				valid: true,
			},
		},
	}

	compiled, ok := compileTemporalRangeBounds(term)
	if !ok {
		t.Fatalf("expected temporal range to compile from cached bound")
	}
	if !compiled.hasGTE || !compiled.gte.instant.Equal(cached.instant) {
		t.Fatalf("unexpected compiled temporal bounds: %+v", compiled)
	}
}

func TestCompileDateTermUsesCache(t *testing.T) {
	cached := temporalValue{
		instant:  time.Date(2026, 3, 5, 10, 28, 21, 0, time.UTC),
		year:     2026,
		month:    time.March,
		day:      5,
		dateOnly: false,
	}
	term := &DateTerm{
		Field: "/timestamp",
		After: "invalid-datetime",
		afterTemporal: temporalLiteralCache{
			raw:   "invalid-datetime",
			value: cached,
			ready: true,
			valid: true,
		},
	}

	compiled, ok := compileDateTerm(term, time.Now())
	if !ok {
		t.Fatalf("expected date selector to compile from cached bound")
	}
	if compiled.gt == nil || !compiled.gt.instant.Equal(cached.instant) {
		t.Fatalf("unexpected compiled date term: %+v", compiled)
	}
}

func TestParseTemporalLiteralOrMacroTodayYesterdayUseUTC(t *testing.T) {
	loc := time.FixedZone("UTC+2", 2*60*60)
	now := time.Date(2026, 3, 7, 0, 30, 0, 0, loc) // 2026-03-06T22:30:00Z

	today, ok := parseTemporalLiteralOrMacro("today", now)
	if !ok {
		t.Fatalf("expected today macro to parse")
	}
	expectedToday := time.Date(2026, 3, 6, 0, 0, 0, 0, time.UTC)
	if !today.instant.Equal(expectedToday) || !today.dateOnly {
		t.Fatalf("unexpected today macro value: %+v", today)
	}

	yesterday, ok := parseTemporalLiteralOrMacro("yesterday", now)
	if !ok {
		t.Fatalf("expected yesterday macro to parse")
	}
	expectedYesterday := time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC)
	if !yesterday.instant.Equal(expectedYesterday) || !yesterday.dateOnly {
		t.Fatalf("unexpected yesterday macro value: %+v", yesterday)
	}
}

func TestCompileDateSinceCachedTodayMacroUsesUTC(t *testing.T) {
	term := &DateTerm{Field: "/timestamp", Since: "today"}
	term.primeTemporalCaches()

	loc := time.FixedZone("UTC+2", 2*60*60)
	now := time.Date(2026, 3, 7, 0, 30, 0, 0, loc) // 2026-03-06T22:30:00Z

	got, ok := compileDateSince(term, now)
	if !ok {
		t.Fatalf("expected since=today to compile")
	}
	expected := time.Date(2026, 3, 6, 0, 0, 0, 0, time.UTC)
	if !got.instant.Equal(expected) || !got.dateOnly {
		t.Fatalf("unexpected since=today value: %+v", got)
	}
}
