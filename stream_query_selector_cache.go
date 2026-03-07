package lql

import (
	"math"
	"sync"
)

const defaultQueryStreamSelectorPlanCacheEntries = 256

var queryStreamSelectorPlanCache = newSelectorPlanCache(defaultQueryStreamSelectorPlanCacheEntries)

type selectorPlanCache struct {
	mu         sync.RWMutex
	maxEntries int
	size       int
	buckets    map[uint64][]selectorPlanCacheEntry
	hash       func(Selector) uint64
}

type selectorPlanCacheEntry struct {
	selector Selector
	plan     QueryStreamPlan
}

func newSelectorPlanCache(maxEntries int) *selectorPlanCache {
	if maxEntries < 0 {
		maxEntries = 0
	}
	return &selectorPlanCache{
		maxEntries: maxEntries,
		buckets:    make(map[uint64][]selectorPlanCacheEntry, maxEntries),
		hash:       hashSelector,
	}
}

func (c *selectorPlanCache) get(selector Selector) (QueryStreamPlan, bool) {
	if c == nil || c.maxEntries == 0 {
		return QueryStreamPlan{}, false
	}
	h := c.hash(selector)
	c.mu.RLock()
	entries := c.buckets[h]
	for i := range entries {
		if selectorEqual(entries[i].selector, selector) {
			plan := entries[i].plan
			c.mu.RUnlock()
			return plan, true
		}
	}
	c.mu.RUnlock()
	return QueryStreamPlan{}, false
}

func (c *selectorPlanCache) put(selector Selector, plan QueryStreamPlan) {
	if c == nil || c.maxEntries == 0 || plan.IsZero() {
		return
	}
	h := c.hash(selector)
	c.mu.Lock()
	defer c.mu.Unlock()
	entries := c.buckets[h]
	for i := range entries {
		if selectorEqual(entries[i].selector, selector) {
			return
		}
	}
	if c.size >= c.maxEntries {
		return
	}
	c.buckets[h] = append(entries, selectorPlanCacheEntry{
		selector: cloneSelector(selector),
		plan:     plan,
	})
	c.size++
}

func hashSelector(selector Selector) uint64 {
	h := uint64(1469598103934665603)
	hashSelectorInto(&h, selector)
	return h
}

func hashSelectorInto(h *uint64, selector Selector) {
	hashByte(h, byte(len(selector.And)))
	for i := range selector.And {
		hashSelectorInto(h, selector.And[i])
	}
	hashByte(h, byte(len(selector.Or)))
	for i := range selector.Or {
		hashSelectorInto(h, selector.Or[i])
	}
	if selector.Not == nil {
		hashByte(h, 0)
	} else {
		hashByte(h, 1)
		hashSelectorInto(h, *selector.Not)
	}
	hashTerm(h, selector.Eq)
	hashTerm(h, selector.Contains)
	hashTerm(h, selector.IContains)
	hashTerm(h, selector.Prefix)
	hashTerm(h, selector.IPrefix)
	hashRangeTerm(h, selector.Range)
	hashDateTerm(h, selector.Date)
	hashInTerm(h, selector.In)
	hashString(h, selector.Exists)
}

func hashTerm(h *uint64, term *Term) {
	if term == nil {
		hashByte(h, 0)
		return
	}
	hashByte(h, 1)
	hashString(h, term.Field)
	hashString(h, term.Value)
	hashByte(h, byte(len(term.Any)))
	for i := range term.Any {
		hashString(h, term.Any[i])
	}
	if term.IgnoreCase {
		hashByte(h, 1)
	} else {
		hashByte(h, 0)
	}
}

func hashRangeTerm(h *uint64, term *RangeTerm) {
	if term == nil {
		hashByte(h, 0)
		return
	}
	hashByte(h, 1)
	hashString(h, term.Field)
	hashRangeBound(h, term.GTE)
	hashRangeBound(h, term.GT)
	hashRangeBound(h, term.LTE)
	hashRangeBound(h, term.LT)
}

func hashDateTerm(h *uint64, term *DateTerm) {
	if term == nil {
		hashByte(h, 0)
		return
	}
	hashByte(h, 1)
	hashString(h, term.Field)
	hashString(h, term.Value)
	hashString(h, term.Since)
	hashString(h, term.After)
	hashString(h, term.Before)
	hashString(h, term.GTE)
	hashString(h, term.GT)
	hashString(h, term.LTE)
	hashString(h, term.LT)
}

func hashInTerm(h *uint64, term *InTerm) {
	if term == nil {
		hashByte(h, 0)
		return
	}
	hashByte(h, 1)
	hashString(h, term.Field)
	hashByte(h, byte(len(term.Any)))
	for i := range term.Any {
		hashString(h, term.Any[i])
	}
}

func hashRangeBound(h *uint64, bound *RangeBound) {
	if bound == nil {
		hashByte(h, 0)
		return
	}
	hashByte(h, 1)
	if value, ok := bound.Number(); ok {
		hashByte(h, 1)
		hashUint64(h, math.Float64bits(value))
		return
	}
	if value, ok := bound.DateTime(); ok {
		hashByte(h, 2)
		hashString(h, value)
		return
	}
	hashByte(h, 3)
}

func hashString(h *uint64, value string) {
	hashUint64(h, uint64(len(value)))
	for i := 0; i < len(value); i++ {
		hashByte(h, value[i])
	}
}

func hashUint64(h *uint64, value uint64) {
	hashByte(h, byte(value))
	hashByte(h, byte(value>>8))
	hashByte(h, byte(value>>16))
	hashByte(h, byte(value>>24))
	hashByte(h, byte(value>>32))
	hashByte(h, byte(value>>40))
	hashByte(h, byte(value>>48))
	hashByte(h, byte(value>>56))
}

func hashByte(h *uint64, b byte) {
	const prime = uint64(1099511628211)
	*h ^= uint64(b)
	*h *= prime
}

func selectorEqual(a, b Selector) bool {
	if len(a.And) != len(b.And) || len(a.Or) != len(b.Or) {
		return false
	}
	for i := range a.And {
		if !selectorEqual(a.And[i], b.And[i]) {
			return false
		}
	}
	for i := range a.Or {
		if !selectorEqual(a.Or[i], b.Or[i]) {
			return false
		}
	}
	if (a.Not == nil) != (b.Not == nil) {
		return false
	}
	if a.Not != nil && !selectorEqual(*a.Not, *b.Not) {
		return false
	}
	if !termEqual(a.Eq, b.Eq) ||
		!termEqual(a.Contains, b.Contains) ||
		!termEqual(a.IContains, b.IContains) ||
		!termEqual(a.Prefix, b.Prefix) ||
		!termEqual(a.IPrefix, b.IPrefix) ||
		!rangeTermEqual(a.Range, b.Range) ||
		!dateTermEqual(a.Date, b.Date) ||
		!inTermEqual(a.In, b.In) {
		return false
	}
	return a.Exists == b.Exists
}

func termEqual(a, b *Term) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == nil {
		return true
	}
	if a.Field != b.Field || a.Value != b.Value || a.IgnoreCase != b.IgnoreCase || len(a.Any) != len(b.Any) {
		return false
	}
	for i := range a.Any {
		if a.Any[i] != b.Any[i] {
			return false
		}
	}
	return true
}

func rangeTermEqual(a, b *RangeTerm) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == nil {
		return true
	}
	return a.Field == b.Field &&
		rangeBoundEqual(a.GTE, b.GTE) &&
		rangeBoundEqual(a.GT, b.GT) &&
		rangeBoundEqual(a.LTE, b.LTE) &&
		rangeBoundEqual(a.LT, b.LT)
}

func dateTermEqual(a, b *DateTerm) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == nil {
		return true
	}
	return a.Field == b.Field &&
		a.Value == b.Value &&
		a.Since == b.Since &&
		a.After == b.After &&
		a.Before == b.Before &&
		a.GTE == b.GTE &&
		a.GT == b.GT &&
		a.LTE == b.LTE &&
		a.LT == b.LT
}

func inTermEqual(a, b *InTerm) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == nil {
		return true
	}
	if a.Field != b.Field || len(a.Any) != len(b.Any) {
		return false
	}
	for i := range a.Any {
		if a.Any[i] != b.Any[i] {
			return false
		}
	}
	return true
}

func rangeBoundEqual(a, b *RangeBound) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == nil {
		return true
	}
	an, aok := a.Number()
	bn, bok := b.Number()
	if aok || bok {
		return aok == bok && an == bn
	}
	ad, aok := a.DateTime()
	bd, bok := b.DateTime()
	return aok == bok && ad == bd
}

func cloneSelector(selector Selector) Selector {
	out := Selector{
		Exists: selector.Exists,
	}
	if len(selector.And) > 0 {
		out.And = make([]Selector, len(selector.And))
		for i := range selector.And {
			out.And[i] = cloneSelector(selector.And[i])
		}
	}
	if len(selector.Or) > 0 {
		out.Or = make([]Selector, len(selector.Or))
		for i := range selector.Or {
			out.Or[i] = cloneSelector(selector.Or[i])
		}
	}
	if selector.Not != nil {
		not := cloneSelector(*selector.Not)
		out.Not = &not
	}
	out.Eq = cloneTerm(selector.Eq)
	out.Contains = cloneTerm(selector.Contains)
	out.IContains = cloneTerm(selector.IContains)
	out.Prefix = cloneTerm(selector.Prefix)
	out.IPrefix = cloneTerm(selector.IPrefix)
	out.Range = cloneRangeTerm(selector.Range)
	out.Date = cloneDateTerm(selector.Date)
	out.In = cloneInTerm(selector.In)
	return out
}

func cloneTerm(term *Term) *Term {
	if term == nil {
		return nil
	}
	clone := *term
	if len(term.Any) > 0 {
		clone.Any = append(make([]string, 0, len(term.Any)), term.Any...)
	}
	return &clone
}

func cloneRangeTerm(term *RangeTerm) *RangeTerm {
	if term == nil {
		return nil
	}
	clone := &RangeTerm{Field: term.Field}
	if term.GTE != nil {
		clone.GTE = cloneRangeBound(term.GTE)
	}
	if term.GT != nil {
		clone.GT = cloneRangeBound(term.GT)
	}
	if term.LTE != nil {
		clone.LTE = cloneRangeBound(term.LTE)
	}
	if term.LT != nil {
		clone.LT = cloneRangeBound(term.LT)
	}
	return clone
}

func cloneRangeBound(bound *RangeBound) *RangeBound {
	if bound == nil {
		return nil
	}
	if value, ok := bound.Number(); ok {
		return NewNumericRangeBound(value)
	}
	if value, ok := bound.DateTime(); ok {
		return NewDatetimeRangeBound(value)
	}
	return &RangeBound{}
}

func cloneDateTerm(term *DateTerm) *DateTerm {
	if term == nil {
		return nil
	}
	clone := *term
	return &clone
}

func cloneInTerm(term *InTerm) *InTerm {
	if term == nil {
		return nil
	}
	clone := &InTerm{Field: term.Field}
	if len(term.Any) > 0 {
		clone.Any = append(make([]string, 0, len(term.Any)), term.Any...)
	}
	return clone
}
