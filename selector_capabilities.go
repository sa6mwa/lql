package lql

import "sort"

// SelectorCapabilities reports feature families used by a selector AST.
type SelectorCapabilities struct {
	And      bool
	Or       bool
	Not      bool
	Eq       bool
	Range    bool
	In       bool
	Prefix   bool
	Contains bool
	Exists   bool

	WildcardPath  bool
	RecursivePath bool
}

// SelectorExecutionTraits reports selector planning hints for stream execution.
type SelectorExecutionTraits struct {
	UsesContainsLike   bool
	UsesRecursivePath  bool
	UsesWildcardPath   bool
	RequiresObjectRoot bool
	// EarlyNonMatchLikely is a best-effort heuristic for low-match workloads.
	EarlyNonMatchLikely bool
}

// Families returns sorted selector clause families referenced by the selector.
func (c SelectorCapabilities) Families() []string {
	families := make([]string, 0, 9)
	if c.And {
		families = append(families, "and")
	}
	if c.Or {
		families = append(families, "or")
	}
	if c.Not {
		families = append(families, "not")
	}
	if c.Eq {
		families = append(families, "eq")
	}
	if c.Range {
		families = append(families, "range")
	}
	if c.In {
		families = append(families, "in")
	}
	if c.Prefix {
		families = append(families, "prefix")
	}
	if c.Contains {
		families = append(families, "contains")
	}
	if c.Exists {
		families = append(families, "exists")
	}
	sort.Strings(families)
	return families
}

// InspectSelectorCapabilities reports selector feature families and path complexity.
func InspectSelectorCapabilities(sel Selector) SelectorCapabilities {
	var capabilities SelectorCapabilities
	inspectSelectorCapabilities(&capabilities, sel)
	return capabilities
}

// InspectSelectorExecutionTraits reports execution planning hints for selector evaluation.
func InspectSelectorExecutionTraits(sel Selector) SelectorExecutionTraits {
	capabilities := InspectSelectorCapabilities(sel)
	traits := SelectorExecutionTraits{
		UsesContainsLike:   capabilities.Contains || capabilities.Prefix,
		UsesRecursivePath:  capabilities.RecursivePath,
		UsesWildcardPath:   capabilities.WildcardPath,
		RequiresObjectRoot: !sel.IsEmpty(),
	}
	traits.EarlyNonMatchLikely = traits.RequiresObjectRoot && !traits.UsesRecursivePath && !traits.UsesContainsLike
	return traits
}

func inspectSelectorCapabilities(capabilities *SelectorCapabilities, sel Selector) {
	if capabilities == nil {
		return
	}
	if len(sel.And) > 0 {
		capabilities.And = true
		for _, child := range sel.And {
			inspectSelectorCapabilities(capabilities, child)
		}
	}
	if len(sel.Or) > 0 {
		capabilities.Or = true
		for _, child := range sel.Or {
			inspectSelectorCapabilities(capabilities, child)
		}
	}
	if sel.Not != nil {
		capabilities.Not = true
		inspectSelectorCapabilities(capabilities, *sel.Not)
	}
	if sel.Eq != nil {
		capabilities.Eq = true
		inspectSelectorPathComplexity(capabilities, sel.Eq.Field)
	}
	if sel.Range != nil {
		capabilities.Range = true
		inspectSelectorPathComplexity(capabilities, sel.Range.Field)
	}
	if sel.In != nil {
		capabilities.In = true
		inspectSelectorPathComplexity(capabilities, sel.In.Field)
	}
	if sel.Prefix != nil {
		capabilities.Prefix = true
		inspectSelectorPathComplexity(capabilities, sel.Prefix.Field)
	}
	if sel.IPrefix != nil {
		capabilities.Prefix = true
		inspectSelectorPathComplexity(capabilities, sel.IPrefix.Field)
	}
	if sel.Contains != nil {
		capabilities.Contains = true
		inspectSelectorPathComplexity(capabilities, sel.Contains.Field)
	}
	if sel.IContains != nil {
		capabilities.Contains = true
		inspectSelectorPathComplexity(capabilities, sel.IContains.Field)
	}
	if sel.Exists != "" {
		capabilities.Exists = true
		inspectSelectorPathComplexity(capabilities, sel.Exists)
	}
}

func inspectSelectorPathComplexity(capabilities *SelectorCapabilities, field string) {
	segments, err := selectorPathSegments(field)
	if err != nil {
		return
	}
	for _, segment := range segments {
		switch segment {
		case "*", "[]", "**", "...":
			capabilities.WildcardPath = true
		}
		switch segment {
		case "**", "...":
			capabilities.RecursivePath = true
		}
	}
}
