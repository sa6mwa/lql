package lql

import "testing"

func TestQueryStreamNumericSegmentParityObjectAndArray(t *testing.T) {
	makeLineArray := func(amount int, status, msg, code string) []any {
		lines := make([]any, 11)
		for i := range lines {
			lines[i] = map[string]any{"amount": i}
		}
		lines[10] = map[string]any{
			"amount": amount,
			"status": status,
			"msg":    msg,
			"code":   code,
		}
		return lines
	}

	docs := []any{
		map[string]any{
			"voucher": map[string]any{
				"lines": map[string]any{
					"10": map[string]any{
						"amount": 3500,
						"status": "open",
						"msg":    "hello object line",
						"code":   "AUTH-10-OBJECT",
					},
				},
			},
		},
		map[string]any{
			"voucher": map[string]any{
				"lines": makeLineArray(3600, "closed", "hello array line", "AUTH-10-ARRAY"),
			},
		},
		map[string]any{
			"batches": []any{
				map[string]any{
					"lines": map[string]any{
						"10": map[string]any{
							"amount": 4100,
							"status": "open",
						},
					},
				},
				map[string]any{
					"lines": makeLineArray(4200, "closed", "nested array line", "AUTH-10-NESTED"),
				},
			},
		},
		map[string]any{
			"voucher": map[string]any{
				"lines": map[string]any{
					"10": map[string]any{
						"amount": 1200,
						"status": "processing",
						"msg":    "low amount",
						"code":   "AUTH-10-LOW",
					},
				},
			},
		},
	}
	input, err := encodeValuesAsNDJSON(docs)
	if err != nil {
		t.Fatalf("encode input: %v", err)
	}

	selectors := []string{
		`/voucher/lines/10/amount>=3000`,
		`exists{/voucher/lines/10/amount}`,
		`in{field=/voucher/lines/10/status,any=open|closed}`,
		`contains{field=/voucher/lines/10/msg,value=hello}`,
		`iprefix{field=/voucher/lines/10/code,value=auth-10}`,
		`/voucher/.../10/amount>=3000`,
		`/batches[]/lines/10/amount>=4000`,
		`/batches[]/.../10/status="open"`,
	}

	for _, expr := range selectors {
		selector := mustParseSelector(t, expr)
		t.Run(expr+"/decision_only", func(t *testing.T) {
			assertQueryStreamParity(t, input, selector, false)
		})
		t.Run(expr+"/plus_value", func(t *testing.T) {
			assertQueryStreamParity(t, input, selector, true)
		})
	}
}
