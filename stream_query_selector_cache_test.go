package lql

import "testing"

func TestSelectorPlanCacheHit(t *testing.T) {
	cache := newSelectorPlanCache(8)
	selector := Selector{Eq: &Term{Field: "/status", Value: "open"}}

	plan, err := NewQueryStreamPlan(selector)
	if err != nil {
		t.Fatalf("new plan: %v", err)
	}
	cache.put(selector, plan)

	got, ok := cache.get(Selector{Eq: &Term{Field: "/status", Value: "open"}})
	if !ok {
		t.Fatalf("expected cache hit")
	}
	if got.IsZero() {
		t.Fatalf("cache returned zero plan")
	}
}

func TestSelectorPlanCacheCollisionSafety(t *testing.T) {
	cache := newSelectorPlanCache(8)
	cache.hash = func(Selector) uint64 { return 1 }

	a := Selector{Eq: &Term{Field: "/kind", Value: "a"}}
	b := Selector{Eq: &Term{Field: "/kind", Value: "b"}}
	planA, err := NewQueryStreamPlan(a)
	if err != nil {
		t.Fatalf("new plan a: %v", err)
	}
	planB, err := NewQueryStreamPlan(b)
	if err != nil {
		t.Fatalf("new plan b: %v", err)
	}
	cache.put(a, planA)
	cache.put(b, planB)

	gotA, ok := cache.get(a)
	if !ok || gotA.IsZero() {
		t.Fatalf("expected hit for a")
	}
	gotB, ok := cache.get(b)
	if !ok || gotB.IsZero() {
		t.Fatalf("expected hit for b")
	}

	if gotA.template == gotB.template {
		t.Fatalf("collision returned same plan template for distinct selectors")
	}
}

func TestSelectorPlanCacheBounded(t *testing.T) {
	cache := newSelectorPlanCache(1)
	a := Selector{Eq: &Term{Field: "/kind", Value: "a"}}
	b := Selector{Eq: &Term{Field: "/kind", Value: "b"}}
	planA, err := NewQueryStreamPlan(a)
	if err != nil {
		t.Fatalf("new plan a: %v", err)
	}
	planB, err := NewQueryStreamPlan(b)
	if err != nil {
		t.Fatalf("new plan b: %v", err)
	}
	cache.put(a, planA)
	cache.put(b, planB)

	if cache.size != 1 {
		t.Fatalf("expected bounded cache size 1, got %d", cache.size)
	}
	if _, ok := cache.get(a); !ok {
		t.Fatalf("expected first plan retained")
	}
}
