package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"pkt.systems/lql"
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
	mutations   stringList
	fields      stringList
	inline      bool
	compact     bool
	theme       string
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

	var projectionPlan *lql.ProjectionPlan
	if len(fields) > 0 {
		projectionPlan, err = lql.NewProjectionPlan(fields)
		if err != nil {
			return err
		}
	}

	enc := newOutputEncoder(os.Stdout, cfg)
	return lql.QueryStream(lql.QueryStreamRequest{
		Reader:      reader,
		Selector:    selector,
		IncludeJSON: true,
		OnValue: func(value lql.QueryStreamValue) error {
			if !value.Matched {
				return nil
			}
			if len(fields) == 0 {
				return writeQueryStreamValue(enc, value)
			}
			projected, found, err := projectQueryStreamValue(value, fields, projectionPlan)
			if err != nil {
				return err
			}
			if !found {
				return nil
			}
			return enc.WriteRawJSON(projected)
		},
	})
}

func runMutations(cfg config, selector lql.Selector, fields []fieldPath, inputPaths []string) error {
	muts, err := lql.ParseMutations(cfg.mutations, time.Now())
	if err != nil {
		return err
	}

	if cfg.inline {
		if len(inputPaths) != 1 || inputPaths[0] == "" || inputPaths[0] == "-" {
			return fmt.Errorf("inline mode requires a single JSON file")
		}
		return writeInlineMutations(inputPaths[0], cfg, selector, fields, muts)
	}

	enc := newOutputEncoder(os.Stdout, cfg)
	stats := streamStats{}
	for _, path := range mutationInputs(inputPaths) {
		reader, err := openInput(path)
		if err != nil {
			return err
		}
		seg, err := processMutationStream(reader, enc, selector, fields, muts, cfg.matchesOnly)
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

func processMutationStream(r io.Reader, enc *outputEncoder, selector lql.Selector, fields []fieldPath, muts []lql.Mutation, matchesOnly bool) (streamStats, error) {
	stats := streamStats{}
	if selector.IsEmpty() && len(fields) == 0 && !matchesOnly {
		return processMutationStreamUngated(r, enc, muts)
	}
	var err error
	var projectionPlan *lql.ProjectionPlan
	if len(fields) > 0 {
		projectionPlan, err = lql.NewProjectionPlan(fields)
		if err != nil {
			return stats, err
		}
	}
	err = lql.QueryStream(lql.QueryStreamRequest{
		Reader:      r,
		Selector:    selector,
		IncludeJSON: true,
		OnValue: func(value lql.QueryStreamValue) error {
			stats.inputs++
			if matchesOnly && !value.Matched {
				return nil
			}

			if value.Matched && len(muts) > 0 && len(fields) == 0 {
				if err := mutateAndWriteQueryStreamValue(enc, value, muts); err != nil {
					return err
				}
				stats.outputs++
				return nil
			}

			if len(fields) == 0 {
				if err := writeQueryStreamValue(enc, value); err != nil {
					return err
				}
				stats.outputs++
				return nil
			}

			projected, found, err := projectQueryStreamValue(value, fields, projectionPlan)
			if err != nil {
				return err
			}
			if !found {
				return nil
			}
			if value.Matched {
				if len(muts) > 0 {
					if err := mutateAndWriteQueryStreamValue(enc, lql.QueryStreamValue{JSON: projected}, muts); err != nil {
						return err
					}
					stats.outputs++
					return nil
				}
			}
			if err := enc.WriteRawJSON(projected); err != nil {
				return err
			}
			stats.outputs++
			return nil
		},
	})
	if err != nil {
		return stats, err
	}
	return stats, nil
}

func projectQueryStreamValue(value lql.QueryStreamValue, fields []fieldPath, plan *lql.ProjectionPlan) ([]byte, bool, error) {
	reader, err := openQueryStreamValue(value)
	if err != nil {
		return nil, false, err
	}
	defer reader.Close()

	var out bytes.Buffer
	result, err := lql.ProjectFields(lql.ProjectFieldsRequest{
		Reader: reader,
		Writer: &out,
		Paths:  fields,
		Plan:   plan,
	})
	if err != nil {
		return nil, false, err
	}
	if !result.Found {
		return nil, false, nil
	}
	return out.Bytes(), true, nil
}

func mutateAndWriteQueryStreamValue(enc *outputEncoder, value lql.QueryStreamValue, muts []lql.Mutation) error {
	reader, err := openQueryStreamValue(value)
	if err != nil {
		return err
	}
	defer reader.Close()

	pipeReader, pipeWriter := io.Pipe()
	mutateErrCh := make(chan error, 1)
	go func() {
		err := lql.MutateStream(lql.MutateStreamRequest{
			Reader:    reader,
			Writer:    pipeWriter,
			Mode:      lql.MutateSingleValueOnly,
			Mutations: muts,
		})
		if err != nil {
			_ = pipeWriter.CloseWithError(err)
			mutateErrCh <- err
			return
		}
		if err := pipeWriter.Close(); err != nil {
			mutateErrCh <- err
			return
		}
		mutateErrCh <- nil
	}()

	writeErr := enc.WriteRawJSONReader(pipeReader)
	if writeErr != nil {
		_ = pipeReader.CloseWithError(writeErr)
	} else {
		_ = pipeReader.Close()
	}
	mutateErr := <-mutateErrCh
	if writeErr != nil {
		return writeErr
	}
	return mutateErr
}

func processMutationStreamUngated(r io.Reader, enc *outputEncoder, muts []lql.Mutation) (streamStats, error) {
	stats := streamStats{}
	writer := &mutationStreamOutputWriter{
		enc:   enc,
		stats: &stats,
		buf:   make([]byte, 0, 4096),
	}
	err := lql.MutateStream(lql.MutateStreamRequest{
		Reader:    r,
		Writer:    writer,
		Mutations: muts,
	})
	if err != nil {
		return stats, err
	}
	if err := writer.Flush(); err != nil {
		return stats, err
	}
	return stats, nil
}

type mutationStreamOutputWriter struct {
	enc   *outputEncoder
	stats *streamStats
	buf   []byte
}

func (w *mutationStreamOutputWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		idx := bytes.IndexByte(w.buf, '\n')
		if idx < 0 {
			break
		}
		line := w.buf[:idx]
		if len(line) > 0 {
			if err := w.enc.WriteRawJSON(line); err != nil {
				return 0, err
			}
			w.stats.inputs++
			w.stats.outputs++
		}
		w.buf = w.buf[idx+1:]
	}
	return len(p), nil
}

func (w *mutationStreamOutputWriter) Flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	if err := w.enc.WriteRawJSON(w.buf); err != nil {
		return err
	}
	w.stats.inputs++
	w.stats.outputs++
	w.buf = w.buf[:0]
	return nil
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
	return o.writePayload(payload)
}

func (o *outputEncoder) WriteRawJSON(payload []byte) error {
	return o.writePayload(payload)
}

func (o *outputEncoder) WriteRawJSONReader(r io.Reader) error {
	if o.compact {
		return prettyx.CompactTo(o.writer, r, &o.opts)
	}
	return prettyx.PrettyStream(o.writer, r, &o.opts)
}

func (o *outputEncoder) writePayload(payload []byte) error {
	reader := bytes.NewReader(payload)
	if o.compact {
		return prettyx.CompactTo(o.writer, reader, &o.opts)
	}
	return prettyx.PrettyStream(o.writer, reader, &o.opts)
}

func openQueryStreamValue(value lql.QueryStreamValue) (io.ReadCloser, error) {
	if len(value.JSON) > 0 {
		return io.NopCloser(bytes.NewReader(value.JSON)), nil
	}
	if value.OpenJSON == nil {
		return nil, fmt.Errorf("query stream candidate payload unavailable")
	}
	return value.OpenJSON()
}

func writeQueryStreamValue(enc *outputEncoder, value lql.QueryStreamValue) error {
	if len(value.JSON) > 0 {
		return enc.WriteRawJSON(value.JSON)
	}
	reader, err := openQueryStreamValue(value)
	if err != nil {
		return err
	}
	defer reader.Close()
	return enc.WriteRawJSONReader(reader)
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

func writeInlineMutations(path string, cfg config, selector lql.Selector, fields []fieldPath, muts []lql.Mutation) error {
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
	stats, err := processMutationStream(reader, enc, selector, fields, muts, cfg.matchesOnly)
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

type fieldPath = lql.ProjectionPath

func parseFieldPaths(paths []string) ([]fieldPath, error) {
	return lql.ParseProjectionPaths(paths)
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
	fmt.Fprintln(w, "  contains{field=/msg,value=timeout,ic=t}")
	fmt.Fprintln(w, "  icontains{field=/msg,value=timeout}")
	fmt.Fprintln(w, "  iprefix{field=/service,value=auth}")
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
