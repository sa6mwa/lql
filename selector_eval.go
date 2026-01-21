package lql

import (
	"encoding/json"
	"strconv"
	"strings"

	"pkt.systems/lql/jsonpointer"
)

// Matches reports whether doc satisfies sel. A nil document never matches.
func Matches(sel Selector, doc map[string]any) bool {
	if doc == nil {
		return false
	}
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
	if sel.Prefix != nil && !matchPrefix(sel.Prefix, doc) {
		return false
	}
	if sel.Range != nil && !matchRange(sel.Range, doc) {
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
	value, ok := valueAtPath(doc, term.Field)
	if !ok {
		return false
	}
	current, ok := valueToString(value)
	if !ok {
		return false
	}
	return current == term.Value
}

func matchPrefix(term *Term, doc map[string]any) bool {
	if term == nil || term.Field == "" {
		return false
	}
	value, ok := valueAtPath(doc, term.Field)
	if !ok {
		return false
	}
	str, ok := valueToString(value)
	if !ok {
		return false
	}
	return strings.HasPrefix(str, term.Value)
}

func matchRange(term *RangeTerm, doc map[string]any) bool {
	if term == nil || term.Field == "" {
		return false
	}
	value, ok := valueAtPath(doc, term.Field)
	if !ok {
		return false
	}
	num, ok := valueToFloat(value)
	if !ok {
		return false
	}
	if term.GTE != nil && num < *term.GTE {
		return false
	}
	if term.GT != nil && num <= *term.GT {
		return false
	}
	if term.LTE != nil && num > *term.LTE {
		return false
	}
	if term.LT != nil && num >= *term.LT {
		return false
	}
	return true
}

func matchIn(term *InTerm, doc map[string]any) bool {
	if term == nil || term.Field == "" || len(term.Any) == 0 {
		return false
	}
	value, ok := valueAtPath(doc, term.Field)
	if !ok {
		return false
	}
	str, ok := valueToString(value)
	if !ok {
		return false
	}
	for _, candidate := range term.Any {
		if str == candidate {
			return true
		}
	}
	return false
}

func matchExists(field string, doc map[string]any) bool {
	if field == "" {
		return false
	}
	value, ok := valueAtPath(doc, field)
	if !ok {
		return false
	}
	return value != nil
}

func valueAtPath(root any, field string) (any, bool) {
	if field == "" {
		return nil, false
	}
	segments, err := jsonpointer.Split(field)
	if err != nil || len(segments) == 0 {
		return nil, false
	}
	current := root
	for _, segment := range segments {
		switch node := current.(type) {
		case map[string]any:
			val, ok := node[segment]
			if !ok {
				return nil, false
			}
			current = val
		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(node) {
				return nil, false
			}
			current = node[index]
		default:
			return nil, false
		}
	}
	return current, true
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
