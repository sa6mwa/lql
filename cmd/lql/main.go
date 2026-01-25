package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"pkt.systems/lql"
	"pkt.systems/lql/jsonpointer"
	"pkt.systems/prettyx"
	"pkt.systems/version"
)

type stringList []string

func (s *stringList) String() string {
	return strings.Join(*s, ",")
}

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func (s *stringList) Type() string {
	return "string"
}

type config struct {
	mutations stringList
	fields    stringList
	inline    bool
	compact   bool
	theme     string
	matchesOnly bool
}

func main() {
	if len(os.Args) == 1 {
		printUsage(os.Stderr)
		os.Exit(2)
	}

	var cfg config
	var showHelp bool
	var showVersion bool
	var orMode bool

	flags := pflag.NewFlagSet("lql", pflag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.VarP(&cfg.mutations, "mutate", "m", "mutation expression (repeatable)")
	flags.VarP(&cfg.fields, "field", "f", "field to include in output (JSON Pointer, repeatable)")
	flags.BoolVarP(&cfg.inline, "inline", "i", false, "edit input file inline")
	flags.BoolVarP(&cfg.inline, "write", "w", false, "alias of --inline")
	flags.BoolVarP(&cfg.compact, "compact", "c", false, "compact output (one JSON document per line)")
	flags.StringVarP(&cfg.theme, "theme", "t", "", "prettyx palette name")
	flags.BoolVarP(&showHelp, "help", "h", false, "show help")
	flags.BoolVarP(&showVersion, "version", "v", false, "show version")
	flags.BoolVarP(&orMode, "or", "O", false, "combine selector arguments with OR")
	flags.BoolVarP(&cfg.matchesOnly, "matches-only", "M", false, "output only selector matches (even with -m)")
	flags.Usage = func() {}

	if err := flags.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "lql: %v\n", err)
		printUsage(os.Stderr)
		os.Exit(2)
	}

	if showHelp {
		printUsage(os.Stdout)
		return
	}
	if showVersion {
		version.SetDefaultModule("pkt.systems/lql")
		fmt.Printf("%s %s\n", version.Module(), version.Current())
		return
	}

	if err := validateTheme(cfg.theme); err != nil {
		fmt.Fprintf(os.Stderr, "lql: %v\n", err)
		os.Exit(2)
	}

	var selectors []string
	var inputPath string
	var inputPaths []string
	var err error
	if len(cfg.mutations) > 0 {
		selectors, inputPaths, err = splitMutationArgs(flags.Args(), cfg.inline)
	} else {
		selectors, inputPath, err = splitArgs(flags.Args(), false)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "lql: %v\n", err)
		os.Exit(2)
	}

	fieldPaths, err := parseFieldPaths(cfg.fields)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lql: %v\n", err)
		os.Exit(2)
	}

	var selector lql.Selector
	if len(selectors) > 0 {
		selector, err = buildSelector(selectors, orMode)
		if err != nil {
			fmt.Fprintf(os.Stderr, "lql: %v\n", err)
			os.Exit(2)
		}
	}

	if len(cfg.mutations) > 0 {
		if err := runMutations(cfg, selector, fieldPaths, inputPaths); err != nil {
			fmt.Fprintf(os.Stderr, "lql: %v\n", err)
			os.Exit(2)
		}
		return
	}

	if err := runSelections(cfg, selector, fieldPaths, inputPath); err != nil {
		fmt.Fprintf(os.Stderr, "lql: %v\n", err)
		os.Exit(2)
	}
}

func runSelections(cfg config, selector lql.Selector, fields []fieldPath, inputPath string) error {
	reader, err := openInput(inputPath)
	if err != nil {
		return err
	}
	defer closeInput(reader)

	enc := newOutputEncoder(os.Stdout, cfg)
	_, err = processStream(reader, enc, selectionHandler(selector, fields))
	if err != nil {
		return err
	}
	return nil
}

func runMutations(cfg config, selector lql.Selector, fields []fieldPath, inputPaths []string) error {
	muts, err := lql.ParseMutations(cfg.mutations, time.Now())
	if err != nil {
		return err
	}

	handler := func(value any) (any, bool, error) {
		matches := selector.IsEmpty() || lql.MatchesValue(selector, value)
		if cfg.matchesOnly && !matches {
			return nil, false, nil
		}

		var out any = value
		if len(fields) > 0 {
			doc, ok := value.(map[string]any)
			if !ok {
				return nil, false, fmt.Errorf("field selection requires JSON objects")
			}
			filtered, ok, err := selectFields(doc, fields)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, nil
			}
			out = filtered
		}

		if matches {
			if doc, ok := out.(map[string]any); ok {
				if err := lql.ApplyMutations(doc, muts); err != nil {
					return nil, false, err
				}
			}
		}

		return out, true, nil
	}

	if cfg.inline {
		if len(inputPaths) != 1 || inputPaths[0] == "" || inputPaths[0] == "-" {
			return fmt.Errorf("inline mode requires a single JSON file")
		}
		return writeInlineStream(inputPaths[0], cfg, fields, handler)
	}

	enc := newOutputEncoder(os.Stdout, cfg)
	stats := streamStats{}
	for _, path := range mutationInputs(inputPaths) {
		reader, err := openInput(path)
		if err != nil {
			return err
		}
		seg, err := processStream(reader, enc, handler)
		closeInput(reader)
		stats.inputs += seg.inputs
		stats.outputs += seg.outputs
		if err != nil {
			return err
		}
	}
	if stats.inputs == 0 {
		return fmt.Errorf("no JSON input")
	}
	return nil
}

type streamStats struct {
	inputs  int
	outputs int
}

func selectionHandler(selector lql.Selector, fields []fieldPath) func(any) (any, bool, error) {
	return func(value any) (any, bool, error) {
		switch node := value.(type) {
		case map[string]any:
			if !selector.IsEmpty() && !lql.Matches(selector, node) {
				return nil, false, nil
			}
			if len(fields) == 0 {
				return node, true, nil
			}
			filtered, ok, err := selectFields(node, fields)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, nil
			}
			return filtered, true, nil
		default:
			if !selector.IsEmpty() {
				return nil, false, nil
			}
			if len(fields) > 0 {
				return nil, false, fmt.Errorf("field selection requires JSON objects")
			}
			return node, true, nil
		}
	}
}

func processStream(r io.Reader, enc *outputEncoder, handler func(any) (any, bool, error)) (streamStats, error) {
	dec := json.NewDecoder(r)
	dec.UseNumber()
	stats := streamStats{}
	for {
		var value any
		if err := dec.Decode(&value); err != nil {
			if errors.Is(err, io.EOF) {
				return stats, nil
			}
			return stats, err
		}
		next, err := processRecord(value, enc, handler, stats)
		if err != nil {
			return next, err
		}
		stats = next
	}
}

func processRecord(value any, enc *outputEncoder, handler func(any) (any, bool, error), stats streamStats) (streamStats, error) {
	if arr, ok := value.([]any); ok {
		for _, item := range arr {
			next, err := processRecord(item, enc, handler, stats)
			if err != nil {
				return next, err
			}
			stats = next
		}
		return stats, nil
	}
	stats.inputs++
	out, ok, err := handler(value)
	if err != nil || !ok {
		return stats, err
	}
	if err := enc.WriteValue(out); err != nil {
		return stats, err
	}
	stats.outputs++
	return stats, nil
}

func emitSelectionStream(enc *outputEncoder, selector lql.Selector, fields []fieldPath, value any) error {
	if arr, ok := value.([]any); ok {
		for _, item := range arr {
			if err := emitSelectionValue(enc, selector, fields, item); err != nil {
				return err
			}
		}
		return nil
	}
	return emitSelectionValue(enc, selector, fields, value)
}

func emitSelectionValue(enc *outputEncoder, selector lql.Selector, fields []fieldPath, value any) error {
	switch node := value.(type) {
	case map[string]any:
		if !selector.IsEmpty() && !lql.Matches(selector, node) {
			return nil
		}
		if len(fields) == 0 {
			return enc.WriteValue(node)
		}
		filtered, ok, err := selectFields(node, fields)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		return enc.WriteValue(filtered)
	default:
		if !selector.IsEmpty() {
			return nil
		}
		if len(fields) > 0 {
			return fmt.Errorf("field selection requires JSON objects")
		}
		return enc.WriteValue(node)
	}
}

type outputEncoder struct {
	writer  io.Writer
	opts    prettyx.Options
	compact bool
}

func newOutputEncoder(w io.Writer, cfg config) *outputEncoder {
	opts := *prettyx.DefaultOptions
	if cfg.theme != "" {
		opts.Palette = cfg.theme
	}
	return &outputEncoder{
		writer:  w,
		opts:    opts,
		compact: cfg.compact,
	}
}

func (o *outputEncoder) WriteValue(value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	reader := bytes.NewReader(payload)
	if o.compact {
		return prettyx.CompactTo(o.writer, reader, &o.opts)
	}
	return prettyx.PrettyStream(o.writer, reader, &o.opts)
}

func openInput(path string) (io.Reader, error) {
	if path == "" || path == "-" {
		return os.Stdin, nil
	}
	return os.Open(path)
}

func closeInput(r io.Reader) {
	if f, ok := r.(*os.File); ok && f != os.Stdin {
		_ = f.Close()
	}
}

func writeInlineStream(path string, cfg config, fields []fieldPath, handler func(any) (any, bool, error)) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "lql-*")
	if err != nil {
		return err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	reader, err := openInput(path)
	if err != nil {
		return err
	}
	defer closeInput(reader)

	enc := newOutputEncoder(tmp, cfg)
	stats, err := processStream(reader, enc, handler)
	if err != nil {
		return err
	}
	if stats.inputs == 0 {
		return fmt.Errorf("no JSON input")
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	info, err := os.Stat(path)
	if err == nil {
		_ = os.Chmod(tmp.Name(), info.Mode())
	}
	return os.Rename(tmp.Name(), path)
}

type fieldPath struct {
	raw      string
	segments []string
}

func parseFieldPaths(paths []string) ([]fieldPath, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	out := make([]fieldPath, 0, len(paths))
	for _, raw := range paths {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		segments, err := jsonpointer.Split(trimmed)
		if err != nil {
			return nil, err
		}
		if len(segments) == 0 {
			return nil, fmt.Errorf("field path %q refers to document root", raw)
		}
		out = append(out, fieldPath{raw: trimmed, segments: segments})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no valid field paths provided")
	}
	return out, nil
}

func selectFields(doc map[string]any, fields []fieldPath) (map[string]any, bool, error) {
	if doc == nil {
		return nil, false, fmt.Errorf("field selection requires JSON objects")
	}
	if len(fields) == 0 {
		return doc, true, nil
	}
	var (
		root  any = map[string]any{}
		found bool
	)
	for _, field := range fields {
		value, ok := extractValue(doc, field.segments)
		if !ok {
			continue
		}
		found = true
		if len(field.segments) == 0 {
			continue
		}
		if _, err := strconv.Atoi(field.segments[0]); err == nil {
			return nil, false, fmt.Errorf("field path %q must not start with an array index", field.raw)
		}
		next, err := assignField(root, field.segments, value)
		if err != nil {
			return nil, false, err
		}
		root = next
	}
	if !found {
		return nil, false, nil
	}
	out, ok := root.(map[string]any)
	if !ok {
		return nil, false, fmt.Errorf("field selection failed")
	}
	return out, true, nil
}

func extractValue(root any, segments []string) (any, bool) {
	current := root
	for _, segment := range segments {
		switch node := current.(type) {
		case map[string]any:
			value, ok := node[segment]
			if !ok {
				return nil, false
			}
			current = value
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

const maxFieldIndex = 1 << 20

func assignField(node any, path []string, value any) (any, error) {
	if len(path) == 0 {
		return value, nil
	}
	token := path[0]
	last := len(path) == 1
	if idx, err := strconv.Atoi(token); err == nil {
		if idx < 0 || idx > maxFieldIndex {
			return nil, fmt.Errorf("field index %d invalid", idx)
		}
		var arr []any
		switch v := node.(type) {
		case nil:
			arr = make([]any, idx+1)
		case []any:
			arr = v
			if idx >= len(arr) {
				grow := idx - len(arr) + 1
				if grow > maxFieldIndex {
					return nil, fmt.Errorf("field index %d invalid", idx)
				}
				arr = append(arr, make([]any, grow)...)
			}
		default:
			return nil, fmt.Errorf("field path conflict at %s", token)
		}
		if last {
			arr[idx] = value
			return arr, nil
		}
		next, err := assignField(arr[idx], path[1:], value)
		if err != nil {
			return nil, err
		}
		arr[idx] = next
		return arr, nil
	}
	var obj map[string]any
	switch v := node.(type) {
	case nil:
		obj = make(map[string]any)
	case map[string]any:
		obj = v
	default:
		return nil, fmt.Errorf("field path conflict at %s", token)
	}
	if last {
		obj[token] = value
		return obj, nil
	}
	next, err := assignField(obj[token], path[1:], value)
	if err != nil {
		return nil, err
	}
	obj[token] = next
	return obj, nil
}

func validateTheme(theme string) error {
	if theme == "" {
		return nil
	}
	for _, name := range prettyx.PaletteNames() {
		if name == theme {
			return nil
		}
	}
	return fmt.Errorf("unknown theme %q", theme)
}

func splitArgs(args []string, mutating bool) ([]string, string, error) {
	if len(args) == 0 {
		return nil, "", nil
	}
	var fileCount int
	for _, arg := range args {
		if arg == "-" {
			continue
		}
		if info, err := os.Stat(arg); err == nil && !info.IsDir() {
			fileCount++
		}
	}
	if mutating && fileCount > 1 {
		return nil, "", fmt.Errorf("mutation input accepts a single JSON file")
	}

	last := args[len(args)-1]
	if last == "-" {
		return args[:len(args)-1], "-", nil
	}
	if info, err := os.Stat(last); err == nil && !info.IsDir() {
		return args[:len(args)-1], last, nil
	}
	return args, "", nil
}

func splitMutationArgs(args []string, inline bool) ([]string, []string, error) {
	if len(args) == 0 {
		return nil, nil, nil
	}
	var selectors []string
	var inputs []string
	for _, arg := range args {
		if arg == "-" {
			inputs = append(inputs, arg)
			continue
		}
		if info, err := os.Stat(arg); err == nil && !info.IsDir() {
			inputs = append(inputs, arg)
			continue
		}
		selectors = append(selectors, arg)
	}
	if inline {
		if len(inputs) == 0 {
			return nil, nil, fmt.Errorf("inline mode requires a file path")
		}
		if len(inputs) != 1 || inputs[0] == "-" {
			return nil, nil, fmt.Errorf("inline mode requires a single JSON file")
		}
	}
	return selectors, inputs, nil
}

func mutationInputs(inputs []string) []string {
	if len(inputs) == 0 {
		return []string{""}
	}
	return inputs
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: lql [-m mutator...] [-f field...] selector... [data.json]")
	fmt.Fprintln(w, "   or: lql selector... < data.json")
	fmt.Fprintln(w, "   or: cat data.json | lql selector...")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Selectors:")
	fmt.Fprintln(w, "  LQL selector expressions (comma/newline separated).")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Mutations:")
	fmt.Fprintln(w, "  -m, --mutate expr    Apply mutations to each JSON object in the input stream.")
	fmt.Fprintln(w, "  -i, --inline         Write mutation output inline to a single input file.")
	fmt.Fprintln(w, "  -w, --write          Alias of --inline.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Output:")
	fmt.Fprintln(w, "  -f, --field /path    Output only the selected JSON Pointer fields (repeatable).")
	fmt.Fprintln(w, "  -c, --compact        Compact output (one JSON document per line).")
	fmt.Fprintln(w, "  -t, --theme theme    Prettyx theme name (use with color terminals).")
	fmt.Fprintln(w, "  -h, --help           Show help.")
	fmt.Fprintln(w, "  -v, --version        Show version.")
	fmt.Fprintln(w, "  -O, --or             Combine selector arguments with OR.")
	fmt.Fprintln(w, "  -M, --matches-only   Output only selector matches (even with -m).")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Themes:")
	writeWrappedList(w, "  ", prettyx.PaletteNames(), 80)
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Selector examples (shorthand):")
	fmt.Fprintln(w, "  /status=\"open\"")
	fmt.Fprintln(w, "  /status!=closed")
	fmt.Fprintln(w, "  /progress>=50")
	fmt.Fprintln(w, "  /devices/0/status=\"online\"")
	fmt.Fprintln(w, "  /labels/*=\"production\"")
	fmt.Fprintln(w, "  /items[]/sku=\"ABC-123\"")
	fmt.Fprintln(w, "  /items/**/sku=\"ABC-123\"")
	fmt.Fprintln(w, "  /items/.../sku=\"ABC-123\"")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Selector examples (full LQL):")
	fmt.Fprintln(w, "  eq{field=/status,value=open}")
	fmt.Fprintln(w, "  and.eq{field=/status,value=open},and.range{field=/progress,gte=50}")
	fmt.Fprintln(w, "  or.eq{field=/region,value=us},or.eq{field=/region,value=eu}")
	fmt.Fprintln(w, "  not.eq{field=/state,value=disabled}")
	fmt.Fprintln(w, "  exists{/metadata/etag}")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Selector OR example:")
	fmt.Fprintln(w, "  lql -O '/status=\"open\"' '/status=\"queued\"' data.json")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Note: with -m, selectors only control which objects are mutated unless -M")
	fmt.Fprintln(w, "      is used to output only selector matches.")
	fmt.Fprintln(w, "Note: mutations apply to a JSON object root, but paths may traverse arrays.")
}

func buildSelector(args []string, orMode bool) (lql.Selector, error) {
	if len(args) == 0 {
		return lql.Selector{}, nil
	}
	if !orMode {
		return lql.ParseSelectorString(strings.Join(args, "\n"))
	}
	clauses := make([]lql.Selector, 0, len(args))
	for _, raw := range args {
		expr := strings.TrimSpace(raw)
		if expr == "" {
			continue
		}
		sel, err := lql.ParseSelectorString(expr)
		if err != nil {
			return lql.Selector{}, err
		}
		if sel.IsEmpty() {
			continue
		}
		clauses = append(clauses, sel)
	}
	switch len(clauses) {
	case 0:
		return lql.Selector{}, nil
	case 1:
		return clauses[0], nil
	default:
		return lql.Selector{Or: clauses}, nil
	}
}

func writeWrappedList(w io.Writer, prefix string, items []string, width int) {
	if len(items) == 0 {
		return
	}
	lineLen := 0
	for i, item := range items {
		sep := ""
		if i > 0 {
			sep = " "
		}
		addLen := len(sep) + len(item)
		if lineLen == 0 {
			fmt.Fprint(w, prefix)
			lineLen = len(prefix)
		}
		if lineLen+addLen > width {
			fmt.Fprint(w, "\n", prefix)
			lineLen = len(prefix)
			sep = ""
			addLen = len(item)
		}
		fmt.Fprint(w, sep, item)
		lineLen += addLen
	}
	fmt.Fprintln(w)
}
