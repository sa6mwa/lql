package lql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"testing"
)

type streamParityRecord struct {
	matched bool
	value   any
}

func assertQueryStreamParity(t *testing.T, input []byte, selector Selector, includeJSON bool) {
	t.Helper()

	got, gotErr := runQueryStreamParity(input, selector, includeJSON)
	want, wantErr := runReferenceQueryParity(input, selector, includeJSON)

	if len(got) != len(want) {
		t.Fatalf("candidate count mismatch: got %d want %d (gotErr=%v wantErr=%v)", len(got), len(want), gotErr, wantErr)
	}

	for i := range got {
		if got[i].matched != want[i].matched {
			t.Fatalf("candidate %d matched mismatch: got %v want %v", i, got[i].matched, want[i].matched)
		}
		if includeJSON && !reflect.DeepEqual(got[i].value, want[i].value) {
			t.Fatalf("candidate %d value mismatch:\n got: %#v\nwant: %#v", i, got[i].value, want[i].value)
		}
	}

	gotHasErr := gotErr != nil
	wantHasErr := wantErr != nil
	if gotHasErr != wantHasErr {
		t.Fatalf("error parity mismatch: gotErr=%v wantErr=%v", gotErr, wantErr)
	}
}

func runQueryStreamParity(input []byte, selector Selector, includeJSON bool) ([]streamParityRecord, error) {
	records := make([]streamParityRecord, 0, 16)
	err := QueryStream(QueryStreamRequest{
		Reader:      bytes.NewReader(input),
		Selector:    selector,
		IncludeJSON: includeJSON,
		OnValue: func(value QueryStreamValue) error {
			record := streamParityRecord{matched: value.Matched}
			if includeJSON {
				payload := value.JSON
				if payload == nil && value.OpenJSON != nil {
					rc, err := value.OpenJSON()
					if err != nil {
						return fmt.Errorf("open streamed json: %w", err)
					}
					defer rc.Close()
					payload, err = io.ReadAll(rc)
					if err != nil {
						return fmt.Errorf("read streamed json: %w", err)
					}
				}
				decoded, err := decodeJSONAny(payload)
				if err != nil {
					return fmt.Errorf("decode streamed json: %w", err)
				}
				record.value = decoded
			}
			records = append(records, record)
			return nil
		},
	})
	return records, err
}

func runReferenceQueryParity(input []byte, selector Selector, includeJSON bool) ([]streamParityRecord, error) {
	dec := json.NewDecoder(bytes.NewReader(input))
	dec.UseNumber()

	empty := selector.IsEmpty()
	records := make([]streamParityRecord, 0, 16)
	for {
		token, err := dec.Token()
		if err == io.EOF {
			return records, nil
		}
		if err != nil {
			return records, err
		}
		if delim, ok := token.(json.Delim); ok && delim == '[' {
			if err := consumeReferenceTopArray(dec, selector, includeJSON, empty, &records); err != nil {
				return records, err
			}
			continue
		}
		value, err := readReferenceValue(dec, token)
		if err != nil {
			return records, err
		}
		record := streamParityRecord{matched: empty || MatchesValue(selector, value)}
		if includeJSON {
			record.value = value
		}
		records = append(records, record)
	}
}

func consumeReferenceTopArray(dec *json.Decoder, selector Selector, includeJSON, empty bool, records *[]streamParityRecord) error {
	for {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		if delim, ok := token.(json.Delim); ok {
			if delim == ']' {
				return nil
			}
			if delim == '[' {
				if err := consumeReferenceTopArray(dec, selector, includeJSON, empty, records); err != nil {
					return err
				}
				continue
			}
		}
		value, err := readReferenceValue(dec, token)
		if err != nil {
			return err
		}
		record := streamParityRecord{matched: empty || MatchesValue(selector, value)}
		if includeJSON {
			record.value = value
		}
		*records = append(*records, record)
	}
}

func readReferenceValue(dec *json.Decoder, token any) (any, error) {
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return token, nil
	}
	switch delim {
	case '{':
		object := make(map[string]any)
		for {
			keyToken, err := dec.Token()
			if err != nil {
				return nil, err
			}
			if end, ok := keyToken.(json.Delim); ok {
				if end == '}' {
					return object, nil
				}
				return nil, fmt.Errorf("unexpected token %v in object", end)
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, fmt.Errorf("object key token has type %T", keyToken)
			}
			valueToken, err := dec.Token()
			if err != nil {
				return nil, err
			}
			value, err := readReferenceValue(dec, valueToken)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
	case '[':
		array := make([]any, 0, 4)
		for {
			valueToken, err := dec.Token()
			if err != nil {
				return nil, err
			}
			if end, ok := valueToken.(json.Delim); ok && end == ']' {
				return array, nil
			}
			value, err := readReferenceValue(dec, valueToken)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
	default:
		return nil, fmt.Errorf("unexpected delimiter token %v", delim)
	}
}

func decodeJSONAny(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	if err := consumeOnlyWhitespace(dec); err != nil {
		return nil, err
	}
	return value, nil
}

func consumeOnlyWhitespace(dec *json.Decoder) error {
	var trailing any
	err := dec.Decode(&trailing)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("unexpected trailing json token")
}

func TestQueryStreamStdlibParityValidStreams(t *testing.T) {
	invalidUTF8 := []byte{'"', 'a', 0xff, 'b', '"', '\n', '{', '"', 's', '"', ':', '"', 'x', 0xfe, 'y', '"', '}'}

	cases := []struct {
		name  string
		input []byte
	}{
		{
			name: "ndjson-and-top-level-array",
			input: []byte(`{"id":"a","n":1}
{"id":"b","n":-2.5e+3}
[{"id":"c","n":3},[{"id":"d","n":4}],true,null,"x"]`),
		},
		{
			name:  "unicode-escapes-and-surrogates",
			input: []byte(`{"id":"a","s":"line\n\t\u0001\u2028\u2029\ud800\udc00\ud800x"}`),
		},
		{
			name:  "raw-invalid-utf8-bytes",
			input: invalidUTF8,
		},
		{
			name:  "mixed-numbers",
			input: []byte(`0 -0 1e-9 -1.2E+3 42`),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertQueryStreamParity(t, tc.input, Selector{}, true)
		})
	}
}

func TestQueryStreamStdlibParityInvalidStreams(t *testing.T) {
	control := []byte{'"', 'a', '\n', 'b', '"'}

	cases := []struct {
		name  string
		input []byte
	}{
		{name: "unterminated-object", input: []byte(`{"id":1`)},
		{name: "invalid-unicode-escape", input: []byte(`"\uZZZZ"`)},
		{name: "invalid-surrogate-followup-escape", input: []byte(`{"a":"\uD800\uZZZZ"}`)},
		{name: "leading-zero-number", input: []byte(`01`)},
		{name: "trailing-comma-array", input: []byte(`[1,2,]`)},
		{name: "control-character-in-string", input: control},
		{name: "invalid-literal", input: []byte(`tru`)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertQueryStreamParity(t, tc.input, Selector{}, true)
		})
	}
}

func TestQueryStreamStdlibParityMultiFieldSelectors(t *testing.T) {
	input := []byte(`[
  {
    "id": "a",
    "status": "open",
    "region": "eu",
    "msg": "Timeout while reading",
    "service": "Auth-Service",
    "progress": 12,
    "latency": 180,
    "env": "prod",
    "meta": {"etag": "x", "trace": 1},
    "items": [{"sku": "A", "price": 10}, {"sku": "B", "price": 25}],
    "groups": [{"items": [{"sku": "A"}, {"sku": "B"}]}]
  },
  [
    {
      "id": "b",
      "status": "closed",
      "region": "us",
      "msg": "done",
      "service": "billing",
      "progress": 5,
      "latency": 90,
      "env": "stage",
      "meta": {"trace": 2},
      "items": [{"sku": "C", "price": 5}],
      "groups": [{"items": [{"sku": "C"}]}]
    }
  ],
  {
    "id": "c",
    "status": "ok",
    "region": "us",
    "msg": "Complete",
    "service": "auth-api",
    "progress": 15,
    "latency": 205,
    "env": "dev",
    "meta": {"etag": null, "trace": 3},
    "items": [{"sku": "B", "price": 30}],
    "groups": [{"items": [{"sku": "B"}]}]
  },
  7
]
{"id":"d","status":"processing","region":"eu","msg":"queued","service":"api-gateway","progress":1,"latency":40,"env":"prod","meta":{"etag":"y"},"items":[{"sku":"D","price":11}]}`)

	selectors := paritySelectorExpressions()
	for i, expr := range selectors {
		sel := mustParseSelector(t, expr)
		t.Run(fmt.Sprintf("selector-%02d", i), func(t *testing.T) {
			assertQueryStreamParity(t, input, sel, true)
		})
	}
}

func paritySelectorExpressions() []string {
	return []string{
		`/status="open"`,
		`/status="open",/region="eu"`,
		`and.eq{field=/status,value=open},and.range{field=/progress,gte=10}`,
		`and.eq{field=/status,value=open},and.range{field=/latency,gte=150},/region="eu"`,
		`or.eq{field=/status,value=open},or.eq{field=/status,value=closed}`,
		`or.0.eq{field=/status,value=ok},or.0.range{field=/progress,gte=15}`,
		`and.0.eq{field=/status,value=ok},and.0.range{field=/progress,gte=15}`,
		`not.eq{field=/status,value=closed},/region="us"`,
		`contains{field=/msg,value=Timeout}`,
		`icontains{field=/msg,value=complete}`,
		`contains{f=/msg,a=Timeout|queue}`,
		`icontains{f=/msg,a=timeout|queue}`,
		`icontains{f=/,v=""}`,
		`prefix{field=/service,value=Auth}`,
		`iprefix{field=/service,value=auth}`,
		`in{field=/env,any=prod|stage}`,
		`exists{/meta/etag}`,
		`/items[]/sku="B"`,
		`/items[]/price>=20,/region="eu"`,
		`/groups/.../sku="B"`,
		`/items/**/sku="B",exists{/meta/etag}`,
		`/items/*/sku="B"`,
		`/voucher/lines/10/amount>=1`,
		`exists{/voucher/lines/10/status}`,
		`in{field=/voucher/lines/10/status,any=open|closed|ok|processing}`,
		`contains{field=/voucher/lines/10/msg,value=line}`,
		`/voucher/.../10/amount>=1`,
		`/timestamp="2026-03-05"`,
		`/timestamp>=2026-03-05T10:28:21Z`,
		`range{field=/timestamp,gte=2026-03-05T10:28:21Z,lt=2026-03-05T10:30:00Z}`,
		`date{field=/timestamp,after=2026-03-05T10:28:21Z,before=2026-03-05T10:30:00Z}`,
		`/status="open",or.eq{field=/msg,value="Timeout while reading"},or.eq{field=/msg,value=fail}`,
		`or.eq{field=/status,value=open},or.eq{field=/status,value=closed},not.eq{field=/region,value=apac},range{field=/latency,gte=50},in{field=/env,any=prod|stage},exists{/meta}`,
	}
}

func paritySeedByte(seed []byte, idx int) byte {
	if len(seed) == 0 {
		return byte((idx*73 + 19) & 0xff)
	}
	return seed[idx%len(seed)]
}

func synthesizeParityDoc(seed []byte, idx int) map[string]any {
	statuses := []string{"open", "closed", "ok", "processing"}
	regions := []string{"eu", "us", "apac"}
	msgs := []string{"Timeout while reading", "done", "Complete", "queued"}
	services := []string{"Auth-Service", "billing", "auth-api", "api-gateway"}
	envs := []string{"prod", "stage", "dev"}
	owners := []string{"alice", "bob", "carol"}
	skus := []string{"A", "B", "C", "D"}
	timestamps := []string{
		"2026-03-05T10:28:21Z",
		"2026-03-05T11:28:21+01:00",
		"2026-03-05T10:29:41.265Z",
		"2026-03-05",
		"not-a-date",
	}

	etagMode := paritySeedByte(seed, idx*11+1) % 3
	var etag any
	switch etagMode {
	case 0:
		etag = fmt.Sprintf("etag-%d", paritySeedByte(seed, idx*11+2))
	case 1:
		etag = nil
	default:
		etag = "x"
	}

	priceA := int(paritySeedByte(seed, idx*11+3)%50) + 1
	priceB := int(paritySeedByte(seed, idx*11+4)%80) + 1
	lineAmount := int(paritySeedByte(seed, idx*11+20)) + 1
	lineStatus := statuses[int(paritySeedByte(seed, idx*11+21))%len(statuses)]
	lineMsg := fmt.Sprintf("line-msg-%d", paritySeedByte(seed, idx*11+22))
	lineCode := fmt.Sprintf("AUTH-10-%d", paritySeedByte(seed, idx*11+23))
	linesArray := make([]any, 11)
	for i := range linesArray {
		linesArray[i] = map[string]any{
			"amount": i + 1,
		}
	}
	linesArray[10] = map[string]any{
		"amount": lineAmount,
		"status": lineStatus,
		"msg":    lineMsg,
		"code":   lineCode,
	}
	var lines any = map[string]any{
		"10": map[string]any{
			"amount": lineAmount,
			"status": lineStatus,
			"msg":    lineMsg,
			"code":   lineCode,
		},
	}
	if paritySeedByte(seed, idx*11+24)%2 == 1 {
		lines = linesArray
	}

	return map[string]any{
		"id":        fmt.Sprintf("doc-%d-%d", idx, paritySeedByte(seed, idx*11+5)),
		"status":    statuses[int(paritySeedByte(seed, idx*11+6))%len(statuses)],
		"region":    regions[int(paritySeedByte(seed, idx*11+7))%len(regions)],
		"msg":       msgs[int(paritySeedByte(seed, idx*11+8))%len(msgs)],
		"service":   services[int(paritySeedByte(seed, idx*11+9))%len(services)],
		"progress":  int(paritySeedByte(seed, idx*11+10) % 30),
		"latency":   int(paritySeedByte(seed, idx*11+11)%250) + 10,
		"timestamp": timestamps[int(paritySeedByte(seed, idx*11+25))%len(timestamps)],
		"env":       envs[int(paritySeedByte(seed, idx*11+12))%len(envs)],
		"meta": map[string]any{
			"etag":  etag,
			"trace": int(paritySeedByte(seed, idx*11+13) % 8),
			"labels": map[string]any{
				"owner": owners[int(paritySeedByte(seed, idx*11+14))%len(owners)],
				"env":   envs[int(paritySeedByte(seed, idx*11+15))%len(envs)],
			},
		},
		"items": []any{
			map[string]any{"sku": skus[int(paritySeedByte(seed, idx*11+16))%len(skus)], "price": priceA},
			map[string]any{"sku": skus[int(paritySeedByte(seed, idx*11+17))%len(skus)], "price": priceB},
		},
		"groups": []any{
			map[string]any{
				"items": []any{
					map[string]any{"sku": skus[int(paritySeedByte(seed, idx*11+18))%len(skus)]},
					map[string]any{"sku": skus[int(paritySeedByte(seed, idx*11+19))%len(skus)]},
				},
			},
		},
		"voucher": map[string]any{
			"lines": lines,
		},
	}
}

func synthesizeParityStream(seed []byte, topArray bool) []byte {
	docCount := 2 + int(paritySeedByte(seed, 0)%4)
	values := make([]any, 0, docCount*2)
	for i := 0; i < docCount; i++ {
		values = append(values, synthesizeParityDoc(seed, i))
		switch paritySeedByte(seed, i+21) % 4 {
		case 0:
			values = append(values, int(paritySeedByte(seed, i+31)))
		case 1:
			values = append(values, paritySeedByte(seed, i+37)%2 == 0)
		case 2:
			values = append(values, []any{map[string]any{"sku": "B", "price": int(paritySeedByte(seed, i+41)) + 1}})
		}
	}

	if topArray {
		payload := make([]any, 0, len(values))
		for i, value := range values {
			if i%3 == 1 && paritySeedByte(seed, i+53)%2 == 0 {
				payload = append(payload, []any{value})
				continue
			}
			payload = append(payload, value)
		}
		raw, _ := json.Marshal(payload)
		if paritySeedByte(seed, 67)%2 == 0 {
			tail, _ := json.Marshal(synthesizeParityDoc(seed, docCount+1))
			raw = append(raw, '\n')
			raw = append(raw, tail...)
		}
		return raw
	}

	out := make([]byte, 0, len(values)*96)
	for i, value := range values {
		raw, _ := json.Marshal(value)
		out = append(out, raw...)
		if i != len(values)-1 {
			out = append(out, '\n')
		}
	}
	return out
}
