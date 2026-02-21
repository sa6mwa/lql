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
	Field      string `json:"field"`
	Value      string `json:"value"`
	IgnoreCase bool   `json:"ignoreCase,omitempty"`
}

// UnmarshalJSON accepts string/bool/number for value and converts to string.
func (t *Term) UnmarshalJSON(data []byte) error {
	type alias struct {
		Field      string      `json:"field"`
		Value      interface{} `json:"value"`
		IgnoreCase interface{} `json:"ignoreCase,omitempty"`
	}
	var tmp alias
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	if tmp.Field == "" {
		return fmt.Errorf("term field required")
	}
	t.Field = tmp.Field
	switch v := tmp.Value.(type) {
	case string:
		t.Value = v
	case bool:
		if v {
			t.Value = "true"
		} else {
			t.Value = "false"
		}
	case float64:
		t.Value = strconv.FormatFloat(v, 'f', -1, 64)
	case nil:
		t.Value = ""
	default:
		t.Value = fmt.Sprint(v)
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
