package lql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Selector represents the recursive selector AST.
type Selector struct {
	And       []Selector `json:"and,omitempty"`
	Or        []Selector `json:"or,omitempty"`
	Not       *Selector  `json:"not,omitempty"`
	Eq        *Term      `json:"eq,omitempty"`
	Contains  *Term      `json:"contains,omitempty"`
	IContains *Term      `json:"icontains,omitempty"`
	Prefix    *Term      `json:"prefix,omitempty"`
	IPrefix   *Term      `json:"iprefix,omitempty"`
	Range     *RangeTerm `json:"range,omitempty"`
	In        *InTerm    `json:"in,omitempty"`
	Exists    string     `json:"exists,omitempty"`
}

// IsEmpty reports whether the selector contains any clauses.
func (s Selector) IsEmpty() bool {
	if len(s.And) > 0 || len(s.Or) > 0 {
		return false
	}
	if s.Not != nil && !s.Not.IsEmpty() {
		return false
	}
	if s.Eq != nil || s.Contains != nil || s.IContains != nil || s.Prefix != nil || s.IPrefix != nil || s.Range != nil || s.In != nil {
		return false
	}
	return s.Exists == ""
}

// Term represents a simple field/value predicate.
type Term struct {
	Field      string   `json:"field"`
	Value      string   `json:"value"`
	Any        []string `json:"any,omitempty"`
	IgnoreCase bool     `json:"ignoreCase,omitempty"`
	valueSet   bool
}

// UnmarshalJSON accepts string/bool/number for value and converts to string.
func (t *Term) UnmarshalJSON(data []byte) error {
	type alias struct {
		Field      string          `json:"field"`
		Value      json.RawMessage `json:"value"`
		Any        json.RawMessage `json:"any,omitempty"`
		IgnoreCase interface{}     `json:"ignoreCase,omitempty"`
	}
	var tmp alias
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	if tmp.Field == "" {
		return fmt.Errorf("term field required")
	}
	t.Field = tmp.Field
	t.Value = ""
	t.Any = nil
	t.valueSet = len(tmp.Value) > 0
	if t.valueSet {
		value, err := parseTermValueRaw(tmp.Value)
		if err != nil {
			return err
		}
		t.Value = value
	}
	if len(tmp.Any) > 0 {
		any, err := parseTermAnyRaw(tmp.Any)
		if err != nil {
			return err
		}
		t.Any = any
	}
	if tmp.IgnoreCase != nil {
		ignoreCase, err := parseIgnoreCaseValue(tmp.IgnoreCase)
		if err != nil {
			return err
		}
		t.IgnoreCase = ignoreCase
	}
	return nil
}

func parseTermValueRaw(raw json.RawMessage) (string, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	return termValueToString(value), nil
}

func parseTermAnyRaw(raw json.RawMessage) ([]string, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	switch v := value.(type) {
	case string:
		any, err := parseInAny(v)
		if err != nil {
			return nil, fmt.Errorf("term any requires values")
		}
		return any, nil
	case []any:
		out := make([]string, 0, len(v))
		for _, candidate := range v {
			item := strings.TrimSpace(termValueToString(candidate))
			if item == "" {
				continue
			}
			out = append(out, item)
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("term any requires values")
		}
		return out, nil
	default:
		item := strings.TrimSpace(termValueToString(v))
		if item == "" {
			return nil, fmt.Errorf("term any requires values")
		}
		return []string{item}, nil
	}
}

func termValueToString(v any) string {
	switch value := v.(type) {
	case string:
		return value
	case bool:
		if value {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64)
	case nil:
		return ""
	default:
		return fmt.Sprint(value)
	}
}

func parseIgnoreCaseValue(v any) (bool, error) {
	switch value := v.(type) {
	case bool:
		return value, nil
	case string:
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "true", "t":
			return true, nil
		case "false", "f":
			return false, nil
		default:
			return false, fmt.Errorf("term ignoreCase must be true/false/t/f")
		}
	default:
		return false, fmt.Errorf("term ignoreCase must be boolean")
	}
}

// RangeTerm captures numeric/timestamp bounds.
type RangeTerm struct {
	Field string   `json:"field"`
	GTE   *float64 `json:"gte,omitempty"`
	GT    *float64 `json:"gt,omitempty"`
	LTE   *float64 `json:"lte,omitempty"`
	LT    *float64 `json:"lt,omitempty"`
}

// InTerm represents a small set membership filter.
type InTerm struct {
	Field string   `json:"field"`
	Any   []string `json:"any"`
}

// UnmarshalJSON ensures empty selectors stay zeroed without nil pointers.
func (s *Selector) UnmarshalJSON(data []byte) error {
	type alias Selector
	var tmp alias
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	*s = Selector(tmp)
	return nil
}
