package lql

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type queryBenchmarkDataset struct {
	name          string
	payload       []byte
	selectorExpr  string
	spoolExpected bool
}

var (
	queryBenchOnce sync.Once
	queryBenchData []queryBenchmarkDataset
	queryBenchErr  error
)

func BenchmarkQueryStreamSynthetic(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping synthetic query benchmark in short mode")
	}

	datasets, err := loadQueryBenchmarkDatasets()
	if err != nil {
		b.Fatalf("load query benchmark datasets: %v", err)
	}

	for _, ds := range datasets {
		ds := ds
		selector, err := ParseSelectorString(ds.selectorExpr)
		if err != nil {
			b.Fatalf("parse selector for %s: %v", ds.name, err)
		}
		plan, err := NewQueryStreamPlan(selector)
		if err != nil {
			b.Fatalf("compile query plan for %s: %v", ds.name, err)
		}
		b.Run(ds.name, func(b *testing.B) {
			b.SetBytes(int64(len(ds.payload)))
			strategies := []struct {
				name    string
				usePlan bool
			}{
				{name: "selector", usePlan: false},
				{name: "plan", usePlan: true},
			}

			for _, strategy := range strategies {
				strategy := strategy
				b.Run("decision_only_"+strategy.name, func(b *testing.B) {
					var src bytes.Reader
					reader := bufio.NewReaderSize(&src, 64*1024)
					var seen, matched int
					onValue := func(value QueryStreamValue) error {
						seen++
						if value.Matched {
							matched++
						}
						return nil
					}
					runBenchmarkModes(b, func() error {
						seen = 0
						matched = 0
						src.Reset(ds.payload)
						reader.Reset(&src)
						req := QueryStreamRequest{
							Reader:  reader,
							Mode:    QueryDecisionOnly,
							OnValue: onValue,
						}
						if strategy.usePlan {
							req.Plan = plan
						} else {
							req.Selector = selector
						}
						err := QueryStream(req)
						if err != nil {
							return err
						}
						if seen == 0 || matched == 0 {
							return fmt.Errorf("unexpected empty stream result: seen=%d matched=%d", seen, matched)
						}
						return nil
					})
				})
			}

			for _, strategy := range strategies {
				strategy := strategy
				b.Run("plus_value_"+strategy.name, func(b *testing.B) {
					var src bytes.Reader
					reader := bufio.NewReaderSize(&src, 64*1024)
					var seen, matched int
					onValue := func(value QueryStreamValue) error {
						seen++
						if value.Matched {
							matched++
						}
						if value.JSON == nil && value.OpenJSON == nil {
							return fmt.Errorf("expected query payload")
						}
						return nil
					}
					runBenchmarkModes(b, func() error {
						seen = 0
						matched = 0
						src.Reset(ds.payload)
						reader.Reset(&src)
						req := QueryStreamRequest{
							Reader:  reader,
							Mode:    QueryDecisionPlusValue,
							OnValue: onValue,
						}
						if strategy.usePlan {
							req.Plan = plan
						} else {
							req.Selector = selector
						}
						err := QueryStream(req)
						if err != nil {
							return err
						}
						if seen == 0 || matched == 0 {
							return fmt.Errorf("unexpected empty stream result: seen=%d matched=%d", seen, matched)
						}
						return nil
					})
				})
			}

			if ds.spoolExpected {
				for _, strategy := range strategies {
					strategy := strategy
					b.Run("plus_value_openjson_"+strategy.name, func(b *testing.B) {
						var src bytes.Reader
						reader := bufio.NewReaderSize(&src, 64*1024)
						var seen, matched int
						onValue := func(value QueryStreamValue) error {
							seen++
							if value.Matched {
								matched++
							}
							if value.OpenJSON == nil {
								return fmt.Errorf("expected OpenJSON for spooled payload")
							}
							rc, err := value.OpenJSON()
							if err != nil {
								return err
							}
							_, copyErr := io.Copy(io.Discard, rc)
							closeErr := rc.Close()
							if copyErr != nil {
								return copyErr
							}
							return closeErr
						}
						runBenchmarkModes(b, func() error {
							seen = 0
							matched = 0
							src.Reset(ds.payload)
							reader.Reset(&src)
							req := QueryStreamRequest{
								Reader:           reader,
								Mode:             QueryDecisionPlusValue,
								SpoolMemoryBytes: 8,
								OnValue:          onValue,
							}
							if strategy.usePlan {
								req.Plan = plan
							} else {
								req.Selector = selector
							}
							err := QueryStream(req)
							if err != nil {
								return err
							}
							if seen == 0 || matched == 0 {
								return fmt.Errorf("unexpected empty stream result: seen=%d matched=%d", seen, matched)
							}
							return nil
						})
					})
				}
			}
		})
	}
}

func loadQueryBenchmarkDatasets() ([]queryBenchmarkDataset, error) {
	queryBenchOnce.Do(func() {
		ndjsonCount := queryBenchmarkCountFromEnv("LQL_BENCH_QUERY_NDJSON_COUNT", 20000)
		arrayCount := queryBenchmarkCountFromEnv("LQL_BENCH_QUERY_ARRAY_COUNT", 20000)
		singleCount := queryBenchmarkCountFromEnv("LQL_BENCH_QUERY_SINGLE_COUNT", 12000)

		ndjson, err := buildQueryNDJSONPayload(ndjsonCount, 128)
		if err != nil {
			queryBenchErr = err
			return
		}
		arrayJSON, err := buildQueryArrayPayload(arrayCount, 128)
		if err != nil {
			queryBenchErr = err
			return
		}
		singleJSON, err := buildQuerySingleObjectPayload(singleCount, 512)
		if err != nil {
			queryBenchErr = err
			return
		}

		queryBenchData = []queryBenchmarkDataset{
			{
				name:          "large_ndjson",
				payload:       ndjson,
				selectorExpr:  `/status="open"`,
				spoolExpected: false,
			},
			{
				name:          "large_ndjson_contains_any",
				payload:       ndjson,
				selectorExpr:  `contains{field=/blob,any=xxxx|nomatch}`,
				spoolExpected: false,
			},
			{
				name:          "large_ndjson_contains",
				payload:       ndjson,
				selectorExpr:  `contains{field=/blob,value=xxxx}`,
				spoolExpected: false,
			},
			{
				name:          "large_ndjson_icontains",
				payload:       ndjson,
				selectorExpr:  `icontains{field=/blob,value=XXXX}`,
				spoolExpected: false,
			},
			{
				name:          "large_ndjson_datetime_range",
				payload:       ndjson,
				selectorExpr:  `/timestamp>=2026-03-05T10:28:21Z`,
				spoolExpected: false,
			},
			{
				name:          "large_ndjson_date_selector",
				payload:       ndjson,
				selectorExpr:  `date{field=/timestamp,after=2026-03-05T10:28:21Z,before=2026-03-05T10:29:50Z}`,
				spoolExpected: false,
			},
			{
				name:          "large_array",
				payload:       arrayJSON,
				selectorExpr:  `/status="open"`,
				spoolExpected: false,
			},
			{
				name:          "large_single_json",
				payload:       singleJSON,
				selectorExpr:  `/records[]/status="open"`,
				spoolExpected: true,
			},
		}
	})

	if queryBenchErr != nil {
		return nil, queryBenchErr
	}
	return queryBenchData, nil
}

func queryBenchmarkCountFromEnv(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return fallback
	}
	return n
}

func buildQueryNDJSONPayload(count, blobSize int) ([]byte, error) {
	blob := strings.Repeat("x", blobSize)
	statuses := []string{"new", "open", "closed", "queued"}
	var buf bytes.Buffer
	for i := 0; i < count; i++ {
		line := buildQueryRecordJSON(i, statuses[i%len(statuses)], blob)
		if _, err := buf.WriteString(line); err != nil {
			return nil, fmt.Errorf("write ndjson record %d: %w", i, err)
		}
		if err := buf.WriteByte('\n'); err != nil {
			return nil, fmt.Errorf("write ndjson newline %d: %w", i, err)
		}
	}
	return buf.Bytes(), nil
}

func buildQueryArrayPayload(count, blobSize int) ([]byte, error) {
	blob := strings.Repeat("x", blobSize)
	statuses := []string{"new", "open", "closed", "queued"}
	var buf bytes.Buffer
	if err := buf.WriteByte('['); err != nil {
		return nil, err
	}
	for i := 0; i < count; i++ {
		if i > 0 {
			if err := buf.WriteByte(','); err != nil {
				return nil, fmt.Errorf("write array separator %d: %w", i, err)
			}
		}
		if _, err := buf.WriteString(buildQueryRecordJSON(i, statuses[i%len(statuses)], blob)); err != nil {
			return nil, fmt.Errorf("write array record %d: %w", i, err)
		}
	}
	if err := buf.WriteByte(']'); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func buildQuerySingleObjectPayload(count, blobSize int) ([]byte, error) {
	blob := strings.Repeat("y", blobSize)
	statuses := []string{"new", "open", "closed", "queued"}
	var buf bytes.Buffer
	if _, err := buf.WriteString(`{"id":"synthetic-root","records":[`); err != nil {
		return nil, err
	}
	for i := 0; i < count; i++ {
		if i > 0 {
			if err := buf.WriteByte(','); err != nil {
				return nil, fmt.Errorf("write records separator %d: %w", i, err)
			}
		}
		if _, err := buf.WriteString(buildQueryRecordJSON(i, statuses[i%len(statuses)], blob)); err != nil {
			return nil, fmt.Errorf("write record %d: %w", i, err)
		}
	}
	if _, err := buf.WriteString(`]}`); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func buildQueryRecordJSON(i int, status, blob string) string {
	timestamp := "2026-03-05T11:28:21+01:00"
	if i%2 == 1 {
		timestamp = "2026-03-05T11:29:41.265+01:00"
	}
	return `{"id":"id-` + strconv.Itoa(i) +
		`","status":"` + status +
		`","metrics":{"retries":` + strconv.Itoa(i%9) +
		`,"qps":` + strconv.Itoa((i%1000)+1) +
		`},"timestamp":"` + timestamp +
		`","blob":"` + blob + `"}`
}
