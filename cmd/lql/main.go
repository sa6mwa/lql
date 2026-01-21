package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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

type config struct {
	mutations stringList
	fields    stringList
	inline    bool
	compact   bool
	theme     string
}

func main() {
	if len(os.Args) == 1 {
		printUsage(os.Stderr)
		os.Exit(2)
	}

	var cfg config
	var showHelp bool
	var showVersion bool

	flags := flag.NewFlagSet("lql", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.Var(&cfg.mutations, "m", "mutation expression (repeatable)")
	flags.Var(&cfg.fields, "f", "field to include in output (JSON Pointer, repeatable)")
	flags.BoolVar(&cfg.inline, "i", false, "edit input file inline")
	flags.BoolVar(&cfg.inline, "w", false, "alias of -i")
	flags.BoolVar(&cfg.compact, "c", false, "compact output (one JSON document per line)")
	flags.StringVar(&cfg.theme, "t", "", "prettyx palette name")
	flags.BoolVar(&showHelp, "h", false, "show help")
	flags.BoolVar(&showVersion, "v", false, "show version")
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

	selectors, inputPath, err := splitArgs(flags.Args(), len(cfg.mutations) > 0)
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
		selector, err = lql.ParseSelectorString(strings.Join(selectors, "\n"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "lql: %v\n", err)
			os.Exit(2)
		}
	}

	if len(cfg.mutations) > 0 {
		if err := runMutations(cfg, selector, fieldPaths, inputPath); err != nil {
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
	dec := json.NewDecoder(reader)
	dec.UseNumber()

	for {
		var value any
		if err := dec.Decode(&value); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := emitSelectionStream(enc, selector, fields, value); err != nil {
			return err
		}
	}
}

func runMutations(cfg config, selector lql.Selector, fields []fieldPath, inputPath string) error {
	if cfg.inline && inputPath == "" {
		return fmt.Errorf("inline mode requires a file path")
	}

	reader, err := openInput(inputPath)
	if err != nil {
		return err
	}
	defer closeInput(reader)

	doc, err := readSingleObject(reader)
	if err != nil {
		return err
	}

	muts, err := lql.ParseMutations(cfg.mutations, time.Now())
	if err != nil {
		return err
	}
	apply := selector.IsEmpty() || lql.Matches(selector, doc)
	if apply {
		if err := lql.ApplyMutations(doc, muts); err != nil {
			return err
		}
	}

	if len(fields) > 0 {
		filtered, err := selectFields(doc, fields)
		if err != nil {
			return err
		}
		doc = filtered
	}

	if cfg.inline {
		return writeInline(inputPath, cfg, doc)
	}

	enc := newOutputEncoder(os.Stdout, cfg)
	return enc.WriteValue(doc)
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
		filtered, err := selectFields(node, fields)
		if err != nil {
			return err
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

func readSingleObject(r io.Reader) (map[string]any, error) {
	dec := json.NewDecoder(r)
	dec.UseNumber()

	var value any
	if err := dec.Decode(&value); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("no JSON input")
		}
		return nil, err
	}
	doc, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("mutation input must be a JSON object")
	}
	var extra any
	if err := dec.Decode(&extra); err == nil {
		return nil, fmt.Errorf("mutation input must contain a single JSON object")
	} else if !errors.Is(err, io.EOF) {
		return nil, err
	}
	return doc, nil
}

func writeInline(path string, cfg config, doc map[string]any) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "lql-*")
	if err != nil {
		return err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	enc := newOutputEncoder(tmp, cfg)
	if err := enc.WriteValue(doc); err != nil {
		return err
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

func selectFields(doc map[string]any, fields []fieldPath) (map[string]any, error) {
	if doc == nil {
		return nil, fmt.Errorf("field selection requires JSON objects")
	}
	if len(fields) == 0 {
		return doc, nil
	}
	var root any = map[string]any{}
	for _, field := range fields {
		value, ok := extractValue(doc, field.segments)
		if !ok {
			continue
		}
		if len(field.segments) == 0 {
			continue
		}
		if _, err := strconv.Atoi(field.segments[0]); err == nil {
			return nil, fmt.Errorf("field path %q must not start with an array index", field.raw)
		}
		next, err := assignField(root, field.segments, value)
		if err != nil {
			return nil, err
		}
		root = next
	}
	out, ok := root.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("field selection failed")
	}
	return out, nil
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

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: lql [-m mutator...] [-f field...] selector... [data.json]")
	fmt.Fprintln(w, "   or: lql selector... < data.json")
	fmt.Fprintln(w, "   or: cat data.json | lql selector...")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Selectors:")
	fmt.Fprintln(w, "  LQL selector expressions (comma/newline separated).")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Mutations:")
	fmt.Fprintln(w, "  -m expression    Apply mutations to a single JSON object.")
	fmt.Fprintln(w, "  -i, -w           Write mutation output inline to the input file.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Output:")
	fmt.Fprintln(w, "  -f /path         Output only the selected JSON Pointer fields (repeatable).")
	fmt.Fprintln(w, "  -c               Compact output (one JSON document per line).")
	fmt.Fprintln(w, "  -t theme         Prettyx theme name (use with color terminals).")
	fmt.Fprintln(w, "  -h               Show help.")
	fmt.Fprintln(w, "  -v               Show version.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Themes:")
	writeWrappedList(w, "  ", prettyx.PaletteNames(), 80)
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Selector examples (shorthand):")
	fmt.Fprintln(w, "  /status=\"open\"")
	fmt.Fprintln(w, "  /status!=closed")
	fmt.Fprintln(w, "  /progress>=50")
	fmt.Fprintln(w, "  /devices/0/status=\"online\"")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Selector examples (full LQL):")
	fmt.Fprintln(w, "  eq{field=/status,value=open}")
	fmt.Fprintln(w, "  and.eq{field=/status,value=open},and.range{field=/progress,gte=50}")
	fmt.Fprintln(w, "  or.eq{field=/region,value=us},or.eq{field=/region,value=eu}")
	fmt.Fprintln(w, "  not.eq{field=/state,value=disabled}")
	fmt.Fprintln(w, "  exists{/metadata/etag}")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Note: mutations only work on JSON objects and do not support array traversal.")
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
