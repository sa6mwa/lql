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
	Date      *DateTerm  `json:"date,omitempty"`
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
	if s.Eq != nil || s.Contains != nil || s.IContains != nil || s.Prefix != nil || s.IPrefix != nil || s.Range != nil || s.Date != nil || s.In != nil {
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
	temporal   temporalLiteralCache
}

// MarshalJSON preserves omitted-value intent for transport round-trips.
func (t Term) MarshalJSON() ([]byte, error) {
	type alias struct {
		Field      string   `json:"field"`
		Value      *string  `json:"value,omitempty"`
		Any        []string `json:"any,omitempty"`
		IgnoreCase bool     `json:"ignoreCase,omitempty"`
	}
	out := alias{
		Field:      t.Field,
		Any:        t.Any,
		IgnoreCase: t.IgnoreCase,
	}
	if t.valueSet || t.Value != "" {
		value := t.Value
		out.Value = &value
	}
	return json.Marshal(out)
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
	t.temporal = temporalLiteralCache{}
	t.valueSet = len(tmp.Value) > 0
	if t.valueSet {
		value, err := parseTermValueRaw(tmp.Value)
		if err != nil {
			return err
		}
		t.Value = value
		t.primeTemporalCache()
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

// RangeTerm captures numeric or datetime bounds.
type RangeTerm struct {
	Field string      `json:"field"`
	GTE   *RangeBound `json:"gte,omitempty"`
	GT    *RangeBound `json:"gt,omitempty"`
	LTE   *RangeBound `json:"lte,omitempty"`
	LT    *RangeBound `json:"lt,omitempty"`
}

// DateTerm captures explicit datetime selector parameters.
type DateTerm struct {
	Field  string `json:"field"`
	Value  string `json:"value,omitempty"`
	Since  string `json:"since,omitempty"`
	After  string `json:"after,omitempty"`
	Before string `json:"before,omitempty"`
	GTE    string `json:"gte,omitempty"`
	GT     string `json:"gt,omitempty"`
	LTE    string `json:"lte,omitempty"`
	LT     string `json:"lt,omitempty"`

	valueTemporal  temporalLiteralCache
	afterTemporal  temporalLiteralCache
	beforeTemporal temporalLiteralCache
	gteTemporal    temporalLiteralCache
	gtTemporal     temporalLiteralCache
	lteTemporal    temporalLiteralCache
	ltTemporal     temporalLiteralCache
	sinceTemporal  temporalLiteralCache
	sinceMode      sinceMacroKind
	sinceModeReady bool
	sinceRaw       string
}

// RangeBound stores a numeric or datetime literal.
type RangeBound struct {
	number   float64
	hasNum   bool
	datetime string
	temporal temporalLiteralCache
}

type temporalLiteralCache struct {
	raw   string
	value temporalValue
	ready bool
	valid bool
}

type sinceMacroKind uint8

const (
	sinceMacroUnknown sinceMacroKind = iota
	sinceMacroNone
	sinceMacroNow
	sinceMacroToday
	sinceMacroYesterday
	sinceMacroLiteral
)

// Number returns the numeric bound when present.
func (b *RangeBound) Number() (float64, bool) {
	if b == nil || !b.hasNum {
		return 0, false
	}
	return b.number, true
}

// DateTime returns the datetime bound literal when present.
func (b *RangeBound) DateTime() (string, bool) {
	if b == nil || b.datetime == "" {
		return "", false
	}
	return b.datetime, true
}

// NewNumericRangeBound builds a numeric range bound for RangeTerm fields.
func NewNumericRangeBound(v float64) *RangeBound {
	return &RangeBound{number: v, hasNum: true}
}

// NewDatetimeRangeBound builds a datetime range bound for RangeTerm fields.
// The value is trimmed; semantic validation occurs during selector validation/eval.
func NewDatetimeRangeBound(v string) *RangeBound {
	bound := &RangeBound{datetime: strings.TrimSpace(v)}
	bound.primeTemporalCache()
	return bound
}

// MarshalJSON writes numeric bounds as JSON numbers and datetime bounds as strings.
func (r RangeTerm) MarshalJSON() ([]byte, error) {
	if r.Field == "" {
		return nil, fmt.Errorf("range field required")
	}
	payload := map[string]any{
		"field": r.Field,
	}
	if r.GTE != nil {
		value, err := rangeBoundJSONValue(r.GTE)
		if err != nil {
			return nil, err
		}
		payload["gte"] = value
	}
	if r.GT != nil {
		value, err := rangeBoundJSONValue(r.GT)
		if err != nil {
			return nil, err
		}
		payload["gt"] = value
	}
	if r.LTE != nil {
		value, err := rangeBoundJSONValue(r.LTE)
		if err != nil {
			return nil, err
		}
		payload["lte"] = value
	}
	if r.LT != nil {
		value, err := rangeBoundJSONValue(r.LT)
		if err != nil {
			return nil, err
		}
		payload["lt"] = value
	}
	return json.Marshal(payload)
}

func rangeBoundJSONValue(bound *RangeBound) (any, error) {
	if bound == nil {
		return nil, nil
	}
	if bound.hasNum && bound.datetime != "" {
		return nil, fmt.Errorf("range bound cannot be both numeric and datetime")
	}
	if bound.hasNum {
		return bound.number, nil
	}
	if bound.datetime != "" {
		return bound.datetime, nil
	}
	return nil, fmt.Errorf("range bound missing value")
}

// UnmarshalJSON accepts numeric or datetime strings for each range bound.
func (r *RangeTerm) UnmarshalJSON(data []byte) error {
	type alias struct {
		Field string          `json:"field"`
		GTE   json.RawMessage `json:"gte,omitempty"`
		GT    json.RawMessage `json:"gt,omitempty"`
		LTE   json.RawMessage `json:"lte,omitempty"`
		LT    json.RawMessage `json:"lt,omitempty"`
	}
	var tmp alias
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	if tmp.Field == "" {
		return fmt.Errorf("range field required")
	}
	r.Field = tmp.Field

	var err error
	r.GTE, err = parseRangeBoundRaw(tmp.GTE)
	if err != nil {
		return fmt.Errorf("range gte: %w", err)
	}
	r.GT, err = parseRangeBoundRaw(tmp.GT)
	if err != nil {
		return fmt.Errorf("range gt: %w", err)
	}
	r.LTE, err = parseRangeBoundRaw(tmp.LTE)
	if err != nil {
		return fmt.Errorf("range lte: %w", err)
	}
	r.LT, err = parseRangeBoundRaw(tmp.LT)
	if err != nil {
		return fmt.Errorf("range lt: %w", err)
	}
	return nil
}

func parseRangeBoundRaw(raw json.RawMessage) (*RangeBound, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var numeric float64
	if err := json.Unmarshal(raw, &numeric); err == nil {
		return NewNumericRangeBound(numeric), nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		text = strings.TrimSpace(text)
		if text == "" {
			return nil, fmt.Errorf("value required")
		}
		return NewDatetimeRangeBound(text), nil
	}
	return nil, fmt.Errorf("value must be numeric or datetime string")
}

func (t *Term) primeTemporalCache() {
	if t == nil || t.temporal.ready || t.Value == "" {
		return
	}
	t.temporal = newTemporalLiteralCache(t.Value)
}

func (b *RangeBound) primeTemporalCache() {
	if b == nil || b.temporal.ready || b.datetime == "" {
		return
	}
	b.temporal = newTemporalLiteralCache(b.datetime)
}

func (t *DateTerm) primeTemporalCaches() {
	if t == nil {
		return
	}
	if !t.valueTemporal.ready && t.Value != "" {
		t.valueTemporal = newTemporalLiteralCache(t.Value)
	}
	if !t.afterTemporal.ready && t.After != "" {
		t.afterTemporal = newTemporalLiteralCache(t.After)
	}
	if !t.beforeTemporal.ready && t.Before != "" {
		t.beforeTemporal = newTemporalLiteralCache(t.Before)
	}
	if !t.gteTemporal.ready && t.GTE != "" {
		t.gteTemporal = newTemporalLiteralCache(t.GTE)
	}
	if !t.gtTemporal.ready && t.GT != "" {
		t.gtTemporal = newTemporalLiteralCache(t.GT)
	}
	if !t.lteTemporal.ready && t.LTE != "" {
		t.lteTemporal = newTemporalLiteralCache(t.LTE)
	}
	if !t.ltTemporal.ready && t.LT != "" {
		t.ltTemporal = newTemporalLiteralCache(t.LT)
	}
	if t.sinceModeReady {
		return
	}
	trimmed := strings.TrimSpace(t.Since)
	t.sinceRaw = trimmed
	if trimmed == "" {
		t.sinceMode = sinceMacroNone
		t.sinceModeReady = true
		return
	}
	switch strings.ToLower(trimmed) {
	case "now":
		t.sinceMode = sinceMacroNow
	case "today":
		t.sinceMode = sinceMacroToday
	case "yesterday":
		t.sinceMode = sinceMacroYesterday
	default:
		t.sinceMode = sinceMacroLiteral
		t.sinceTemporal = newTemporalLiteralCache(trimmed)
	}
	t.sinceModeReady = true
}

func newTemporalLiteralCache(raw string) temporalLiteralCache {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return temporalLiteralCache{}
	}
	value, ok := parseTemporalLiteral(trimmed)
	return temporalLiteralCache{
		raw:   trimmed,
		value: value,
		ready: true,
		valid: ok,
	}
}

// UnmarshalJSON decodes the date selector term and primes temporal caches.
func (t *DateTerm) UnmarshalJSON(data []byte) error {
	type alias DateTerm
	var tmp alias
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	*t = DateTerm(tmp)
	t.primeTemporalCaches()
	return nil
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
