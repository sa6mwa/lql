package lql

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// Matches reports whether doc satisfies sel. A nil document never matches.
func Matches(sel Selector, doc map[string]any) bool {
	if doc == nil {
		return false
	}
	sel = simplifySelector(sel)
	return matchSelector(sel, doc)
}

// MatchesValue reports whether value satisfies sel. Only JSON objects match.
func MatchesValue(sel Selector, value any) bool {
	doc, ok := value.(map[string]any)
	if !ok {
		return false
	}
	return Matches(sel, doc)
}

func matchSelector(sel Selector, doc map[string]any) bool {
	if len(sel.Or) > 0 {
		for _, branch := range sel.Or {
			if matchSelector(branch, doc) {
				return true
			}
		}
		return false
	}
	if sel.Not != nil && matchSelector(*sel.Not, doc) {
		return false
	}
	if sel.Eq != nil && !matchEq(sel.Eq, doc) {
		return false
	}
	if sel.Contains != nil && !matchContains(sel.Contains, doc) {
		return false
	}
	if sel.IContains != nil && !matchIContains(sel.IContains, doc) {
		return false
	}
	if sel.Prefix != nil && !matchPrefix(sel.Prefix, doc) {
		return false
	}
	if sel.IPrefix != nil && !matchIPrefix(sel.IPrefix, doc) {
		return false
	}
	if sel.Range != nil && !matchRange(sel.Range, doc) {
		return false
	}
	if sel.Date != nil && !matchDate(sel.Date, doc) {
		return false
	}
	if sel.In != nil && !matchIn(sel.In, doc) {
		return false
	}
	if sel.Exists != "" && !matchExists(sel.Exists, doc) {
		return false
	}
	for _, clause := range sel.And {
		if !matchSelector(clause, doc) {
			return false
		}
	}
	return true
}

func matchEq(term *Term, doc map[string]any) bool {
	if term == nil || term.Field == "" {
		return false
	}
	values, ok := valuesAtPath(doc, term.Field)
	if !ok {
		return false
	}
	queryTemporal, queryTemporalOK := termTemporalLiteralValue(term)
	for _, value := range values {
		current, ok := valueToString(value)
		if !ok {
			continue
		}
		if current == term.Value {
			return true
		}
		if !queryTemporalOK {
			continue
		}
		candidateTemporal, candidateTemporalOK := parseTemporalLiteral(current)
		if !candidateTemporalOK {
			continue
		}
		if temporalEqual(candidateTemporal, queryTemporal) {
			return true
		}
	}
	return false
}

func matchPrefix(term *Term, doc map[string]any) bool {
	return matchStringTerm(term, doc, strings.HasPrefix, false)
}

func matchContains(term *Term, doc map[string]any) bool {
	return matchStringTerm(term, doc, strings.Contains, false)
}

func matchIPrefix(term *Term, doc map[string]any) bool {
	return matchStringTerm(term, doc, strings.HasPrefix, true)
}

func matchIContains(term *Term, doc map[string]any) bool {
	return matchStringTerm(term, doc, strings.Contains, true)
}

func matchStringTerm(term *Term, doc map[string]any, matcher func(string, string) bool, forceIgnoreCase bool) bool {
	if term == nil || term.Field == "" {
		return false
	}
	values, ok := valuesAtPath(doc, term.Field)
	if !ok {
		return false
	}
	// Omitted value acts as a path assertion for string-term selectors.
	if len(term.Any) == 0 && !term.valueSet && term.Value == "" {
		return true
	}
	ignoreCase := forceIgnoreCase || term.IgnoreCase
	needles := make([]string, 0, 1+len(term.Any))
	if len(term.Any) > 0 {
		needles = append(needles, term.Any...)
	} else {
		needles = append(needles, term.Value)
	}
	if ignoreCase {
		for i := range needles {
			needles[i] = strings.ToLower(needles[i])
		}
	}
	for _, value := range values {
		str, ok := valueToString(value)
		if !ok {
			continue
		}
		if ignoreCase {
			str = strings.ToLower(str)
		}
		for _, needle := range needles {
			if matcher(str, needle) {
				return true
			}
		}
	}
	return false
}

func matchRange(term *RangeTerm, doc map[string]any) bool {
	if term == nil || term.Field == "" {
		return false
	}
	values, ok := valuesAtPath(doc, term.Field)
	if !ok {
		return false
	}
	switch determineRangeMode(term) {
	case rangeModeNumeric:
		bounds, ok := compileNumericRangeBounds(term)
		if !ok {
			return false
		}
		for _, value := range values {
			num, ok := valueToFloat(value)
			if !ok {
				continue
			}
			if numericRangeMatches(bounds, num) {
				return true
			}
		}
	case rangeModeTemporal:
		bounds, ok := compileTemporalRangeBounds(term)
		if !ok {
			return false
		}
		for _, value := range values {
			current, ok := valueToString(value)
			if !ok {
				continue
			}
			candidate, ok := parseTemporalLiteral(current)
			if !ok {
				continue
			}
			if temporalRangeMatches(bounds, candidate) {
				return true
			}
		}
	default:
		return false
	}
	return false
}

func matchDate(term *DateTerm, doc map[string]any) bool {
	if term == nil || term.Field == "" {
		return false
	}
	compiled, ok := compileDateTerm(term, time.Now())
	if !ok {
		return false
	}
	values, ok := valuesAtPath(doc, term.Field)
	if !ok {
		return false
	}
	for _, value := range values {
		current, ok := valueToString(value)
		if !ok {
			continue
		}
		candidate, ok := parseTemporalLiteral(current)
		if !ok {
			continue
		}
		if dateTermMatches(compiled, candidate) {
			return true
		}
	}
	return false
}

func matchIn(term *InTerm, doc map[string]any) bool {
	if term == nil || term.Field == "" || len(term.Any) == 0 {
		return false
	}
	values, ok := valuesAtPath(doc, term.Field)
	if !ok {
		return false
	}
	for _, value := range values {
		str, ok := valueToString(value)
		if !ok {
			continue
		}
		for _, candidate := range term.Any {
			if str == candidate {
				return true
			}
		}
	}
	return false
}

func matchExists(field string, doc map[string]any) bool {
	if field == "" {
		return false
	}
	values, ok := valuesAtPath(doc, field)
	if !ok {
		return false
	}
	for _, value := range values {
		if value != nil {
			return true
		}
	}
	return false
}

func valuesAtPath(root any, field string) ([]any, bool) {
	if field == "" {
		return nil, false
	}
	segments, err := selectorPathSegments(field)
	if err != nil || len(segments) == 0 {
		return nil, false
	}
	nodes := []any{root}
	for _, segment := range segments {
		if len(nodes) == 0 {
			return nil, false
		}
		var next []any
		for _, node := range nodes {
			switch segment {
			case "*":
				if v, ok := node.(map[string]any); ok {
					for _, child := range v {
						next = append(next, child)
					}
				}
			case "[]":
				if arr, ok := node.([]any); ok {
					next = append(next, arr...)
				}
			case "**":
				switch v := node.(type) {
				case map[string]any:
					for _, child := range v {
						next = append(next, child)
					}
				case []any:
					next = append(next, v...)
				}
			case "...":
				next = append(next, collectDescendants(node, true)...)
			default:
				switch v := node.(type) {
				case map[string]any:
					child, ok := v[segment]
					if ok {
						next = append(next, child)
					}
				case []any:
					index, err := strconv.Atoi(segment)
					if err != nil || index < 0 || index >= len(v) {
						continue
					}
					next = append(next, v[index])
				}
			}
		}
		nodes = next
	}
	if len(nodes) == 0 {
		return nil, false
	}
	return nodes, true
}

func collectDescendants(node any, includeSelf bool) []any {
	if node == nil {
		return nil
	}
	var out []any
	stack := []any{node}
	if !includeSelf {
		stack = stack[:0]
		switch v := node.(type) {
		case map[string]any:
			for _, child := range v {
				stack = append(stack, child)
			}
		case []any:
			stack = append(stack, v...)
		}
	}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		out = append(out, n)
		switch v := n.(type) {
		case map[string]any:
			for _, child := range v {
				stack = append(stack, child)
			}
		case []any:
			stack = append(stack, v...)
		}
	}
	return out
}

func valueToString(v any) (string, bool) {
	switch val := v.(type) {
	case string:
		return val, true
	case json.Number:
		return val.String(), true
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64), true
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32), true
	case int:
		return strconv.Itoa(val), true
	case int64:
		return strconv.FormatInt(val, 10), true
	case int32:
		return strconv.FormatInt(int64(val), 10), true
	case uint:
		return strconv.FormatUint(uint64(val), 10), true
	case uint64:
		return strconv.FormatUint(val, 10), true
	case uint32:
		return strconv.FormatUint(uint64(val), 10), true
	case bool:
		if val {
			return "true", true
		}
		return "false", true
	default:
		return "", false
	}
}

func valueToFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case json.Number:
		f, err := val.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint64:
		return float64(val), true
	case uint32:
		return float64(val), true
	case string:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}
