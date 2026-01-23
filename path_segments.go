package lql

import (
	"strings"

	"pkt.systems/lql/jsonpointer"
)

func selectorPathSegments(field string) ([]string, error) {
	segments, err := jsonpointer.Split(field)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, nil
	}
	return expandBracketSugar(segments), nil
}

func expandBracketSugar(segments []string) []string {
	out := make([]string, 0, len(segments))
	for _, segment := range segments {
		if segment == "" || segment == "[]" || segment == "*" || segment == "**" || segment == "..." || !strings.HasSuffix(segment, "[]") {
			out = append(out, segment)
			continue
		}
		base := segment
		count := 0
		for strings.HasSuffix(base, "[]") && base != "[]" {
			base = strings.TrimSuffix(base, "[]")
			count++
		}
		if base == "" {
			out = append(out, segment)
			continue
		}
		out = append(out, base)
		for i := 0; i < count; i++ {
			out = append(out, "[]")
		}
	}
	return out
}
