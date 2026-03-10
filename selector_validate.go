package lql

import (
	"fmt"
	"time"
)

func validateSelectorSemantics(sel Selector) error {
	if sel.Eq != nil {
		sel.Eq.primeTemporalCache()
	}
	if err := validateTermAnyUsage("eq", sel.Eq, false); err != nil {
		return err
	}
	if err := validateTermAnyUsage("contains", sel.Contains, true); err != nil {
		return err
	}
	if err := validateTermAnyUsage("icontains", sel.IContains, true); err != nil {
		return err
	}
	if err := validateTermAnyUsage("prefix", sel.Prefix, false); err != nil {
		return err
	}
	if err := validateTermAnyUsage("iprefix", sel.IPrefix, false); err != nil {
		return err
	}
	if err := validateRangeTerm(sel.Range); err != nil {
		return err
	}
	if err := validateDateTerm(sel.Date, time.Now()); err != nil {
		return err
	}
	if err := validateInTerm(sel.In); err != nil {
		return err
	}
	if sel.Not != nil {
		if err := validateSelectorSemantics(*sel.Not); err != nil {
			return err
		}
	}
	for _, child := range sel.And {
		if err := validateSelectorSemantics(child); err != nil {
			return err
		}
	}
	for _, child := range sel.Or {
		if err := validateSelectorSemantics(child); err != nil {
			return err
		}
	}
	return nil
}

func validateInTerm(term *InTerm) error {
	if term == nil {
		return nil
	}
	if term.Field == "" {
		return fmt.Errorf("in selector field required")
	}
	if len(term.Any) == 0 {
		return fmt.Errorf("in selector requires at least one any value")
	}
	for _, candidate := range term.Any {
		if candidate == "" {
			return fmt.Errorf("in selector requires non-empty any values")
		}
	}
	return nil
}

func validateTermAnyUsage(op string, term *Term, allowAny bool) error {
	if term == nil || len(term.Any) == 0 {
		return nil
	}
	if !allowAny {
		return fmt.Errorf("%s selector does not support any", op)
	}
	if term.valueSet || term.Value != "" {
		return fmt.Errorf("%s selector cannot set both value and any", op)
	}
	return nil
}

func validateRangeTerm(term *RangeTerm) error {
	if term == nil {
		return nil
	}
	if term.Field == "" {
		return fmt.Errorf("range selector field required")
	}
	bounds := []*RangeBound{term.GTE, term.GT, term.LTE, term.LT}
	mode := rangeModeInvalid
	count := 0
	for _, bound := range bounds {
		if bound == nil {
			continue
		}
		bound.primeTemporalCache()
		count++
		if _, ok := bound.Number(); ok {
			if mode == rangeModeTemporal {
				return fmt.Errorf("range selector cannot mix numeric and datetime bounds")
			}
			mode = rangeModeNumeric
			continue
		}
		text, ok := bound.DateTime()
		if !ok {
			return fmt.Errorf("range selector bound missing value")
		}
		if _, parsed := temporalRangeBoundValue(bound); !parsed {
			return fmt.Errorf("range selector datetime bound %q invalid", text)
		}
		if mode == rangeModeNumeric {
			return fmt.Errorf("range selector cannot mix numeric and datetime bounds")
		}
		mode = rangeModeTemporal
	}
	if count == 0 {
		return fmt.Errorf("range selector requires at least one bound")
	}
	return nil
}

func validateDateTerm(term *DateTerm, now time.Time) error {
	if term == nil {
		return nil
	}
	term.primeTemporalCaches()
	if term.Field == "" {
		return fmt.Errorf("date selector field required")
	}
	if term.After != "" && term.GT != "" {
		return fmt.Errorf("date selector cannot set both after and gt")
	}
	if term.Before != "" && term.LT != "" {
		return fmt.Errorf("date selector cannot set both before and lt")
	}
	if term.Since != "" && (term.After != "" || term.GT != "" || term.GTE != "") {
		return fmt.Errorf("date selector since cannot be combined with after/gt/gte")
	}

	count := 0
	if term.Value != "" {
		if _, ok := temporalFromCacheOrParse(term.Value, term.valueTemporal); !ok {
			return fmt.Errorf("date selector value %q invalid", term.Value)
		}
		count++
	}
	for _, bound := range []struct {
		raw   string
		cache temporalLiteralCache
	}{
		{raw: term.After, cache: term.afterTemporal},
		{raw: term.Before, cache: term.beforeTemporal},
		{raw: term.GTE, cache: term.gteTemporal},
		{raw: term.GT, cache: term.gtTemporal},
		{raw: term.LTE, cache: term.lteTemporal},
		{raw: term.LT, cache: term.ltTemporal},
	} {
		if bound.raw == "" {
			continue
		}
		if _, ok := temporalFromCacheOrParse(bound.raw, bound.cache); !ok {
			return fmt.Errorf("date selector bound %q invalid", bound.raw)
		}
		count++
	}
	if term.Since != "" {
		if _, ok := compileDateSince(term, now); !ok {
			return fmt.Errorf("date selector since %q invalid", term.Since)
		}
		count++
	}
	if count == 0 {
		return fmt.Errorf("date selector requires at least one comparator")
	}
	return nil
}
