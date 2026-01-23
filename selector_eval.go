package lql

import (
	"encoding/json"
	"strconv"
	"strings"
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
	values, ok := valuesAtPath(doc, term.Field)
	if !ok {
		return false
	}
	for _, value := range values {
		current, ok := valueToString(value)
		if !ok {
			continue
		}
		if current == term.Value {
			return true
		}
	}
	return false
}

func matchPrefix(term *Term, doc map[string]any) bool {
	if term == nil || term.Field == "" {
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
		if strings.HasPrefix(str, term.Value) {
			return true
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
	for _, value := range values {
		num, ok := valueToFloat(value)
		if !ok {
			continue
		}
		if term.GTE != nil && num < *term.GTE {
			continue
		}
		if term.GT != nil && num <= *term.GT {
			continue
		}
		if term.LTE != nil && num > *term.LTE {
			continue
		}
		if term.LT != nil && num >= *term.LT {
			continue
		}
		return true
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
