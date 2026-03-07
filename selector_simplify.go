package lql

type selectorTruth uint8

const (
	selectorTruthExpression selectorTruth = iota
	selectorTruthTrue
	selectorTruthFalse
)

func simplifySelector(sel Selector) Selector {
	simplified, truth := simplifySelectorNode(sel)
	switch truth {
	case selectorTruthTrue:
		return Selector{}
	case selectorTruthFalse:
		return selectorAlwaysFalse()
	default:
		return simplified
	}
}

func simplifySelectorNode(sel Selector) (Selector, selectorTruth) {
	if len(sel.Or) > 0 {
		branches := make([]Selector, 0, len(sel.Or))
		for _, branch := range sel.Or {
			simplifiedBranch, truth := simplifySelectorNode(branch)
			switch truth {
			case selectorTruthTrue:
				return Selector{}, selectorTruthTrue
			case selectorTruthFalse:
				continue
			default:
				branches = append(branches, simplifiedBranch)
			}
		}
		switch len(branches) {
		case 0:
			return Selector{}, selectorTruthFalse
		default:
			return Selector{Or: branches}, selectorTruthExpression
		}
	}

	out := Selector{}

	if sel.Not != nil {
		simplifiedNot, truth := simplifySelectorNode(*sel.Not)
		switch truth {
		case selectorTruthTrue:
			return Selector{}, selectorTruthFalse
		case selectorTruthFalse:
		default:
			out.Not = &simplifiedNot
		}
	}

	if sel.Eq != nil {
		out.Eq = sel.Eq
	}
	if sel.Contains != nil {
		if !selectorStringClauseMatchesAll(sel.Contains) {
			out.Contains = sel.Contains
		}
	}
	if sel.IContains != nil {
		if !selectorStringClauseMatchesAll(sel.IContains) {
			out.IContains = sel.IContains
		}
	}
	if sel.Prefix != nil && !selectorStringClauseMatchesAll(sel.Prefix) {
		out.Prefix = sel.Prefix
	}
	if sel.IPrefix != nil && !selectorStringClauseMatchesAll(sel.IPrefix) {
		out.IPrefix = sel.IPrefix
	}
	if sel.Range != nil {
		out.Range = sel.Range
	}
	if sel.Date != nil {
		out.Date = sel.Date
	}
	if sel.In != nil {
		out.In = sel.In
	}
	if sel.Exists != "" {
		out.Exists = sel.Exists
	}

	if len(sel.And) > 0 {
		andClauses := make([]Selector, 0, len(sel.And))
		for _, child := range sel.And {
			simplifiedChild, truth := simplifySelectorNode(child)
			switch truth {
			case selectorTruthTrue:
				continue
			case selectorTruthFalse:
				return Selector{}, selectorTruthFalse
			default:
				andClauses = append(andClauses, simplifiedChild)
			}
		}
		if len(andClauses) > 0 {
			out.And = andClauses
		}
	}

	if out.IsEmpty() {
		return Selector{}, selectorTruthTrue
	}
	return out, selectorTruthExpression
}

func selectorStringClauseMatchesAll(term *Term) bool {
	if term == nil || len(term.Any) > 0 || term.Value != "" {
		return false
	}
	segments, err := selectorPathSegments(term.Field)
	if err != nil {
		return false
	}
	if len(segments) == 0 {
		return true
	}
	if len(segments) != 1 {
		return false
	}
	switch segments[0] {
	case "*", "**", "...":
		return true
	default:
		return false
	}
}

func selectorAlwaysFalse() Selector {
	return Selector{
		And: []Selector{
			{
				Not: &Selector{},
			},
		},
	}
}
