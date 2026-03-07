package lql

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func FuzzParseSelectorString(f *testing.F) {
	seed := []string{
		"",
		`/status="open"`,
		`/type!=critical`,
		`/progress/count>=42`,
		`/battery_mv<=3600`,
		"eq{field=/status,value=open}",
		"eq{f=/status,v=ok}",
		"not.eq{field=/status,value=closed}",
		"and.eq{field=/status,value=open},and.eq{field=/owner,value=alice}",
		"or.eq{field=/status,value=open},or.eq{field=/status,value=processing}",
		"or.0.eq{field=/status,value=ok},or.0.range{field=/progress,gte=10}",
		"and.0.eq{field=/status,value=ok},and.0.range{field=/progress,gte=10}",
		"contains{field=/msg,value=timeout}",
		"contains{field=/msg,value=timeout,ic=t}",
		"contains{field=/msg,value=timeout,ignoreCase=f}",
		"contains{field=/msg,any=timeout|error}",
		"icontains{field=/msg,value=timeout}",
		"icontains{field=/msg,any=timeout|error}",
		"prefix{field=/service,value=auth}",
		"iprefix{field=/service,value=auth}",
		"in{field=/env,any=prod|stage}",
		"in{f=/env,a=prod|stage|dev}",
		"range{field=/latency,gte=50,lte=300}",
		`/timestamp="2026-03-05"`,
		`/timestamp>=2026-03-05T10:28:21Z`,
		`range{field=/timestamp,gte=2026-03-05T10:28:21Z,lt=2026-03-05T10:30:00Z}`,
		`date{field=/timestamp,after=2026-03-05T10:28:21Z,before=2026-03-05T10:30:00Z}`,
		`date{f=/timestamp,a=2026-03-05T10:28:21Z,b=2026-03-05T10:30:00Z}`,
		`date{f=/timestamp,since=yesterday}`,
		"exists{/meta/etag}",
		"exists{/items/**/sku}",
		"/items[]/sku=\"B\"",
		"/items/**/sku=\"B\"",
		"/groups/.../sku=\"B\"",
		"and.eq{field=/status,value=open},and.range{field=/progress,gte=10},exists{/meta/etag}",
		"or.eq{field=/status,value=open},or.eq{field=/status,value=closed},not.eq{field=/region,value=apac}",
		"and.eq{field=/status,value=open},or.eq{field=/owner,value=\"alice\"}",
		"and.eq{field=/status,value=open",
		"in{field=/env,any=}",
		"contains{field=/msg,value=timeout,any=error}",
		"prefix{field=/service,any=auth|api}",
		"contains{field=/msg,value=timeout,ignoreCase=maybe}",
		"or.0.eq{field=/status,value=ok},or.0.eq{field=/status,value=done}",
		"and.0.eq{field=/status,value=ok},and.0.eq{field=/status,value=done}",
	}
	seed = append(seed, paritySelectorExpressions()...)
	for _, s := range seed {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, expr string) {
		_, _ = ParseSelectorString(expr)
	})
}

func FuzzParseSelectorShorthand(f *testing.F) {
	seeds := []struct {
		field string
		value string
		op    uint8
	}{
		{"status", "open", 0},
		{"progress/count", "42", 2},
		{"flags/urgent", "true", 1},
		{"battery_mv", "3600", 5},
		{"meta/etag", "\"etag-1\"", 0},
		{"items/0/sku", "B", 0},
		{"weird key", "value,with,comma", 0},
		{"tabs\nstate", "on", 0},
	}
	for _, seed := range seeds {
		f.Add(seed.field, seed.value, seed.op)
	}
	ops := []string{"=", "!=", ">", ">=", "<", "<="}
	f.Fuzz(func(t *testing.T, field, value string, opIdx uint8) {
		field = "/" + sanitizeFuzzField(field)
		if field == "/" {
			field = "/fuzz"
		}
		op := ops[int(opIdx)%len(ops)]
		literal := buildFuzzLiteral(op, value, opIdx)
		expr := field + op + literal
		_, _ = ParseSelectorString(expr)
	})
}

func sanitizeFuzzField(in string) string {
	trimmed := strings.TrimSpace(in)
	if trimmed == "" {
		return "field"
	}
	replacer := func(r rune) rune {
		switch r {
		case '\n', '\r', '\t', '=', '>', '<', '!', '{', '}', ',', '\\':
			return '_'
		default:
			return r
		}
	}
	return strings.Map(replacer, trimmed)
}

func buildFuzzLiteral(op string, value string, opIdx uint8) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		trimmed = "seed"
	}
	switch op {
	case ">", ">=", "<", "<=":
		if opIdx%2 == 0 {
			// numeric range literal path
			size := len(trimmed)
			if size <= 0 {
				size = 1
			}
			return strings.TrimSpace(strings.Repeat("1", size%5+1))
		}
		// datetime range literal path
		dates := []string{
			"2026-03-05",
			"2026-03-05T10:28:21Z",
			"2026-03-05T11:28:21+01:00",
			"2026-03-05T11:29:41.265+01:00",
		}
		date := dates[int(opIdx)%len(dates)]
		if opIdx%3 == 0 {
			return fmt.Sprintf("\"%s\"", date)
		}
		return date
	default:
		lower := strings.ToLower(trimmed)
		if lower == "true" || lower == "false" {
			return lower
		}
		if isNumeric(trimmed) {
			return trimmed
		}
		bytes, err := json.Marshal(trimmed)
		if err != nil {
			return "\"seed\""
		}
		return string(bytes)
	}
}

func isNumeric(in string) bool {
	if in == "" {
		return false
	}
	for _, r := range in {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
