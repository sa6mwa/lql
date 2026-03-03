package lql

import "fmt"

func validateSelectorSemantics(sel Selector) error {
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
