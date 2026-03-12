package lql

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// MutationKind identifies the operation applied to a JSON path.
type MutationKind int

const (
	// MutationSet assigns a value to the target path.
	MutationSet MutationKind = iota
	// MutationIncrement adds/subtracts a numeric delta to the target path.
	MutationIncrement
	// MutationRemove deletes the target path.
	MutationRemove
)

// Mutation describes a parsed mutation expression.
type Mutation struct {
	Path  []string
	Kind  MutationKind
	Value any
	Delta float64

	fileValue *mutationFileValue
}

type mutationFileValueMode uint8

const (
	mutationFileValueModeAuto mutationFileValueMode = iota
	mutationFileValueModeText
	mutationFileValueModeBase64
)

type mutationFileValue struct {
	path     string
	mode     mutationFileValueMode
	resolver MutationFileValueResolver
}

func (v *mutationFileValue) open() (io.ReadCloser, error) {
	if v == nil {
		return nil, fmt.Errorf("file-backed mutation value is nil")
	}
	if v.resolver != nil {
		return v.resolver.Open(v.path)
	}
	return os.Open(v.path)
}

func (m Mutation) hasFileValue() bool {
	return m.fileValue != nil
}

// MutationFileValueResolver opens file-backed mutation sources.
//
// Implementations must return a new reader positioned at offset 0 on each Open
// call. Auto mode may open the same path more than once per mutation
// application.
type MutationFileValueResolver interface {
	Open(path string) (io.ReadCloser, error)
}

// ParseMutationsOptions configures ParseMutationsWithOptions behavior.
type ParseMutationsOptions struct {
	EnableFileValues  bool
	FileValueBaseDir  string
	FileValueResolver MutationFileValueResolver
}

// ParseMutations parses CLI-style mutation expressions into Mutation structs.
// Paths follow JSON Pointer semantics (`/foo/bar`), so literal dots or spaces in
// keys require no extra quoting. Brace shorthand (`/foo{/bar=1,/baz=2}`),
// rm:/time: prefixes, and ++/--/+=/-= increment forms are supported.
func ParseMutations(exprs []string, now time.Time) ([]Mutation, error) {
	return ParseMutationsWithOptions(exprs, now, ParseMutationsOptions{})
}

// ParseMutationsWithOptions parses CLI-style mutation expressions into Mutation
// structs using the supplied parser options.
func ParseMutationsWithOptions(exprs []string, now time.Time, opts ParseMutationsOptions) ([]Mutation, error) {
	if len(exprs) == 0 {
		return nil, fmt.Errorf("no field mutations provided")
	}
	var out []Mutation
	for _, raw := range exprs {
		expr := strings.TrimSpace(raw)
		if expr == "" {
			continue
		}
		muts, err := parseMutationExpr(expr, now, opts)
		if err != nil {
			return nil, err
		}
		out = append(out, muts...)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no valid field mutations parsed")
	}
	return out, nil
}

// ParseMutationsString parses a comma/newline separated LQL mutation string.
func ParseMutationsString(expr string, now time.Time) ([]Mutation, error) {
	chunks, err := splitExpressions(expr)
	if err != nil {
		return nil, err
	}
	return ParseMutations(chunks, now)
}

// Mutations parses variadic mutation expressions (each of which may contain
// comma/newline separated clauses) using the provided timestamp for time:
// operands.
func Mutations(now time.Time, exprs ...string) ([]Mutation, error) {
	var chunks []string
	for _, raw := range exprs {
		expr := strings.TrimSpace(raw)
		if expr == "" {
			continue
		}
		parts, err := splitExpressions(expr)
		if err != nil {
			return nil, err
		}
		if len(parts) == 0 {
			chunks = append(chunks, expr)
		} else {
			chunks = append(chunks, parts...)
		}
	}
	return ParseMutations(chunks, now)
}

// Mutate applies the provided mutation expressions to doc using time.Now().
func Mutate(doc map[string]any, exprs ...string) error {
	return MutateWithTime(doc, time.Now(), exprs...)
}

// MutateWithTime applies mutation expressions using the supplied time.
func MutateWithTime(doc map[string]any, now time.Time, exprs ...string) error {
	muts, err := Mutations(now, exprs...)
	if err != nil {
		return err
	}
	return ApplyMutations(doc, muts)
}

func parseMutationExpr(expr string, now time.Time, opts ParseMutationsOptions) ([]Mutation, error) {
	removeMode := false
	fileMode := mutationFileValueModeAuto
	fileModeSet := false
	switch {
	case strings.HasPrefix(expr, "file:"):
		fileModeSet = true
		expr = strings.TrimPrefix(expr, "file:")
	case strings.HasPrefix(expr, "textfile:"):
		fileModeSet = true
		fileMode = mutationFileValueModeText
		expr = strings.TrimPrefix(expr, "textfile:")
	case strings.HasPrefix(expr, "base64file:"):
		fileModeSet = true
		fileMode = mutationFileValueModeBase64
		expr = strings.TrimPrefix(expr, "base64file:")
	}
	switch {
	case strings.HasPrefix(expr, "rm:"):
		removeMode = true
		expr = strings.TrimPrefix(expr, "rm:")
	case strings.HasPrefix(expr, "remove:"):
		removeMode = true
		expr = strings.TrimPrefix(expr, "remove:")
	case strings.HasPrefix(expr, "delete:"):
		removeMode = true
		expr = strings.TrimPrefix(expr, "delete:")
	case strings.HasPrefix(expr, "del:"):
		removeMode = true
		expr = strings.TrimPrefix(expr, "del:")
	}
	timeMode := false
	if strings.HasPrefix(expr, "time:") {
		if fileModeSet {
			return nil, fmt.Errorf("file-backed mutation cannot be combined with time: (%s)", expr)
		}
		if removeMode {
			return nil, fmt.Errorf("time-prefixed mutation cannot be combined with delete/remove (%s)", expr)
		}
		timeMode = true
		expr = strings.TrimPrefix(expr, "time:")
	}
	if removeMode {
		if fileModeSet {
			return nil, fmt.Errorf("file-backed mutation cannot be combined with delete/remove (%s)", expr)
		}
		path := strings.TrimSpace(expr)
		if path == "" {
			return nil, fmt.Errorf("remove mutation missing key path")
		}
		pathParts, err := splitPath(path)
		if err != nil {
			return nil, err
		}
		return []Mutation{{Path: pathParts, Kind: MutationRemove}}, nil
	}
	if strings.HasSuffix(expr, "++") {
		if fileModeSet {
			return nil, fmt.Errorf("file-backed mutation cannot be combined with ++ (%s)", expr)
		}
		if timeMode {
			return nil, fmt.Errorf("time-prefixed mutation does not support ++ (%s)", expr)
		}
		path := strings.TrimSpace(strings.TrimSuffix(expr, "++"))
		mut, err := buildIncrementMutation(path, 1)
		if err != nil {
			return nil, err
		}
		return []Mutation{mut}, nil
	}
	if strings.HasSuffix(expr, "--") {
		if fileModeSet {
			return nil, fmt.Errorf("file-backed mutation cannot be combined with -- (%s)", expr)
		}
		if timeMode {
			return nil, fmt.Errorf("time-prefixed mutation does not support -- (%s)", expr)
		}
		path := strings.TrimSpace(strings.TrimSuffix(expr, "--"))
		mut, err := buildIncrementMutation(path, -1)
		if err != nil {
			return nil, err
		}
		return []Mutation{mut}, nil
	}
	// Brace shorthand (foo.bar{...}) with nested expressions.
	if strings.HasSuffix(expr, "}") {
		if idx := strings.Index(expr, "{"); idx > 0 {
			pathExpr := strings.TrimSpace(expr[:idx])
			if pathExpr != "" && !strings.Contains(pathExpr, "=") {
				content := expr[idx+1 : len(expr)-1]
				subExprs, err := splitExpressions(content)
				if err != nil {
					return nil, err
				}
				if len(subExprs) == 0 {
					return nil, fmt.Errorf("brace mutation %q empty", expr)
				}
				prefix, err := splitPath(pathExpr)
				if err != nil {
					return nil, err
				}
				var muts []Mutation
				for _, sub := range subExprs {
					sub = strings.TrimSpace(sub)
					if sub == "" {
						continue
					}
					subMuts, err := parseMutationExpr(sub, now, opts)
					if err != nil {
						return nil, err
					}
					for _, m := range subMuts {
						m.Path = append(prefix, m.Path...)
						muts = append(muts, m)
					}
				}
				if len(muts) == 0 {
					return nil, fmt.Errorf("brace mutation %q produced no expressions", expr)
				}
				return muts, nil
			}
		}
	}
	parts := strings.SplitN(expr, "=", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid mutation %q (expected key=value)", expr)
	}
	path := strings.TrimSpace(parts[0])
	if path == "" {
		return nil, fmt.Errorf("mutation %q missing key path", expr)
	}
	value := strings.TrimSpace(parts[1])
	if !timeMode {
		if fileModeSet {
			if !opts.EnableFileValues {
				return nil, fmt.Errorf("file-backed mutations are disabled")
			}
			mut, err := buildFileSetMutation(path, value, fileMode, opts)
			if err != nil {
				return nil, err
			}
			return []Mutation{mut}, nil
		}
		if len(value) > 0 && (value[0] == '+' || value[0] == '-') {
			if delta, err := strconv.ParseFloat(value, 64); err == nil {
				mut, err := buildIncrementMutation(path, delta)
				if err != nil {
					return nil, err
				}
				return []Mutation{mut}, nil
			}
		}
	}
	mut, err := buildSetMutation(path, value, timeMode, now)
	if err != nil {
		return nil, err
	}
	return []Mutation{mut}, nil
}

func buildFileSetMutation(path string, filePath string, mode mutationFileValueMode, opts ParseMutationsOptions) (Mutation, error) {
	pathSegments, err := splitPath(path)
	if err != nil {
		return Mutation{}, err
	}
	resolvedPath, err := resolveMutationFilePath(filePath, opts.FileValueBaseDir)
	if err != nil {
		return Mutation{}, err
	}
	return Mutation{
		Path: pathSegments,
		Kind: MutationSet,
		fileValue: &mutationFileValue{
			path:     resolvedPath,
			mode:     mode,
			resolver: opts.FileValueResolver,
		},
	}, nil
}

func resolveMutationFilePath(path string, baseDir string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("file-backed mutation missing file path")
	}
	if strings.HasPrefix(path, `"`) && strings.HasSuffix(path, `"`) && len(path) >= 2 {
		path = path[1 : len(path)-1]
	} else if strings.HasPrefix(path, `'`) && strings.HasSuffix(path, `'`) && len(path) >= 2 {
		path = path[1 : len(path)-1]
	}
	var err error
	path, err = expandMutationFileHomePath(path)
	if err != nil {
		return "", err
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path), nil
	}
	if baseDir == "" {
		return "", fmt.Errorf("relative file-backed mutation path %q requires file value base dir", path)
	}
	return filepath.Clean(filepath.Join(baseDir, path)), nil
}

func expandMutationFileHomePath(path string) (string, error) {
	if path == "~" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home dir for file-backed mutation: %w", err)
		}
		return homeDir, nil
	}
	if !strings.HasPrefix(path, "~/") {
		return path, nil
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir for file-backed mutation: %w", err)
	}
	return filepath.Join(homeDir, path[2:]), nil
}

func buildSetMutation(path string, literal string, timeMode bool, now time.Time) (Mutation, error) {
	pathSegments, err := splitPath(path)
	if err != nil {
		return Mutation{}, err
	}
	val, err := parseValue(literal, timeMode, now)
	if err != nil {
		return Mutation{}, err
	}
	return Mutation{Path: pathSegments, Kind: MutationSet, Value: val}, nil
}

func buildIncrementMutation(path string, delta float64) (Mutation, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return Mutation{}, fmt.Errorf("increment mutation missing key path")
	}
	if delta == 0 {
		return Mutation{}, fmt.Errorf("increment mutation requires non-zero delta")
	}
	pathSegments, err := splitPath(path)
	if err != nil {
		return Mutation{}, err
	}
	return Mutation{Path: pathSegments, Kind: MutationIncrement, Delta: delta}, nil
}

func splitPath(path string) ([]string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("path empty")
	}
	if !strings.HasPrefix(path, "/") {
		return nil, fmt.Errorf("mutation path %q must start with '/'", path)
	}
	segments, err := selectorPathSegments(path)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, fmt.Errorf("path %q refers to document root", path)
	}
	return segments, nil
}

func parseValue(lit string, timeMode bool, now time.Time) (any, error) {
	unquoted := strings.TrimSpace(lit)
	if len(unquoted) >= 2 && ((unquoted[0] == '"' && unquoted[len(unquoted)-1] == '"') || (unquoted[0] == '\'' && unquoted[len(unquoted)-1] == '\'')) {
		unquoted = unquoted[1 : len(unquoted)-1]
	}
	if timeMode {
		if strings.EqualFold(unquoted, "NOW") {
			return now.UTC().Format(time.RFC3339Nano), nil
		}
		if t, err := time.Parse(time.RFC3339Nano, unquoted); err == nil {
			return t.UTC().Format(time.RFC3339Nano), nil
		}
		if t, err := time.Parse(time.RFC3339, unquoted); err == nil {
			return t.UTC().Format(time.RFC3339Nano), nil
		}
		return nil, fmt.Errorf("invalid time literal %q", lit)
	}
	if strings.EqualFold(unquoted, "true") {
		return true, nil
	}
	if strings.EqualFold(unquoted, "false") {
		return false, nil
	}
	if strings.EqualFold(unquoted, "null") {
		return nil, nil
	}
	if i, err := strconv.ParseInt(unquoted, 10, 64); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(unquoted, 64); err == nil {
		return f, nil
	}
	return unquoted, nil
}

// ApplyMutations mutates the provided JSON object in-place according to muts.
func ApplyMutations(doc map[string]any, muts []Mutation) error {
	if doc == nil {
		return fmt.Errorf("document must be an object")
	}
	for _, mut := range muts {
		if mut.hasFileValue() {
			return fmt.Errorf("file-backed mutation at /%s requires MutateStream", strings.Join(mut.Path, "/"))
		}
		if err := applyMutation(doc, mut); err != nil {
			return err
		}
	}
	return nil
}

func applyMutation(doc map[string]any, mut Mutation) error {
	if len(mut.Path) == 0 {
		return fmt.Errorf("mutation has empty path")
	}
	if pathHasWildcard(mut.Path) {
		return applyMutationWildcard(doc, mut)
	}
	create := mut.Kind != MutationRemove
	parent, key, err := navigate(doc, mut.Path, create)
	if err != nil {
		if mut.Kind == MutationRemove {
			return nil
		}
		return err
	}
	switch mut.Kind {
	case MutationSet:
		parent[key] = mut.Value
	case MutationIncrement:
		existing, ok := parent[key]
		if !ok {
			parent[key] = normalizeNumber(mut.Delta)
			return nil
		}
		num, ok := toFloat(existing)
		if !ok {
			return fmt.Errorf("value at %s is not numeric", strings.Join(mut.Path, "."))
		}
		parent[key] = normalizeNumber(num + mut.Delta)
	case MutationRemove:
		if parent != nil {
			delete(parent, key)
		}
	default:
		return fmt.Errorf("unknown mutation kind")
	}
	return nil
}

func navigate(root map[string]any, path []string, create bool) (map[string]any, string, error) {
	if len(path) == 0 {
		return nil, "", fmt.Errorf("empty path")
	}
	var current any = root
	for i := 0; i < len(path)-1; i++ {
		segment := path[i]
		switch node := current.(type) {
		case map[string]any:
			next, ok := node[segment]
			if !ok {
				if !create {
					return nil, "", fmt.Errorf("path %s does not exist", strings.Join(path[:i+1], "."))
				}
				child := make(map[string]any)
				node[segment] = child
				current = child
				continue
			}
			child, ok := next.(map[string]any)
			if !ok {
				if !create {
					return nil, "", fmt.Errorf("path %s is not an object", strings.Join(path[:i+1], "."))
				}
				child = make(map[string]any)
				node[segment] = child
			}
			current = child
		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(node) {
				return nil, "", fmt.Errorf("path %s does not exist", strings.Join(path[:i+1], "."))
			}
			current = node[index]
		default:
			return nil, "", fmt.Errorf("path %s is not an object", strings.Join(path[:i+1], "."))
		}
	}
	obj, ok := current.(map[string]any)
	if !ok {
		return nil, "", fmt.Errorf("path %s is not an object", strings.Join(path[:len(path)-1], "."))
	}
	return obj, path[len(path)-1], nil
}

func pathHasWildcard(path []string) bool {
	for _, segment := range path {
		switch segment {
		case "*", "[]", "**", "...":
			return true
		}
	}
	return false
}

func applyMutationWildcard(root map[string]any, mut Mutation) error {
	return applyMutationAtPath(root, mut.Path, mut)
}

func applyMutationAtPath(node any, path []string, mut Mutation) error {
	if len(path) == 0 {
		return nil
	}
	segment := path[0]
	last := len(path) == 1
	switch segment {
	case "*":
		obj, ok := node.(map[string]any)
		if !ok {
			return nil
		}
		if last {
			for key := range obj {
				if err := applyMutationToMapKey(obj, key, mut, true); err != nil {
					return err
				}
			}
			return nil
		}
		for _, child := range obj {
			if err := applyMutationAtPath(child, path[1:], mut); err != nil {
				return err
			}
		}
		return nil
	case "[]":
		arr, ok := node.([]any)
		if !ok {
			return nil
		}
		if last {
			for i := range arr {
				if err := applyMutationToArray(arr, i, mut, true); err != nil {
					return err
				}
			}
			return nil
		}
		for _, child := range arr {
			if err := applyMutationAtPath(child, path[1:], mut); err != nil {
				return err
			}
		}
		return nil
	case "**":
		switch v := node.(type) {
		case map[string]any:
			if last {
				for key := range v {
					if err := applyMutationToMapKey(v, key, mut, true); err != nil {
						return err
					}
				}
				return nil
			}
			for _, child := range v {
				if err := applyMutationAtPath(child, path[1:], mut); err != nil {
					return err
				}
			}
		case []any:
			if last {
				for i := range v {
					if err := applyMutationToArray(v, i, mut, true); err != nil {
						return err
					}
				}
				return nil
			}
			for _, child := range v {
				if err := applyMutationAtPath(child, path[1:], mut); err != nil {
					return err
				}
			}
		}
		return nil
	case "...":
		if last {
			return applyMutationRecursiveDesc(node, mut)
		}
		return applyMutationRecursivePath(node, path[1:], mut)
	default:
		if last {
			switch v := node.(type) {
			case map[string]any:
				return applyMutationToMapKey(v, segment, mut, true)
			case []any:
				index, err := strconv.Atoi(segment)
				if err != nil || index < 0 || index >= len(v) {
					return nil
				}
				return applyMutationToArray(v, index, mut, true)
			default:
				return nil
			}
		}
		switch v := node.(type) {
		case map[string]any:
			child, ok := v[segment]
			if !ok {
				return nil
			}
			return applyMutationAtPath(child, path[1:], mut)
		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(v) {
				return nil
			}
			return applyMutationAtPath(v[index], path[1:], mut)
		default:
			return nil
		}
	}
}

func applyMutationRecursiveDesc(node any, mut Mutation) error {
	switch v := node.(type) {
	case map[string]any:
		for key := range v {
			if err := applyMutationToMapKey(v, key, mut, true); err != nil {
				return err
			}
		}
		for _, child := range v {
			if err := applyMutationRecursiveDesc(child, mut); err != nil {
				return err
			}
		}
	case []any:
		for i := range v {
			if err := applyMutationToArray(v, i, mut, true); err != nil {
				return err
			}
		}
		for _, child := range v {
			if err := applyMutationRecursiveDesc(child, mut); err != nil {
				return err
			}
		}
	}
	return nil
}

func applyMutationRecursivePath(node any, path []string, mut Mutation) error {
	if err := applyMutationAtPath(node, path, mut); err != nil {
		return err
	}
	switch v := node.(type) {
	case map[string]any:
		for _, child := range v {
			if err := applyMutationRecursivePath(child, path, mut); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range v {
			if err := applyMutationRecursivePath(child, path, mut); err != nil {
				return err
			}
		}
	}
	return nil
}

func applyMutationToMapKey(obj map[string]any, key string, mut Mutation, skipMissing bool) error {
	existing, ok := obj[key]
	if !ok && skipMissing {
		return nil
	}
	switch mut.Kind {
	case MutationSet:
		if ok || !skipMissing {
			obj[key] = mut.Value
		}
	case MutationIncrement:
		if !ok {
			if skipMissing {
				return nil
			}
			obj[key] = normalizeNumber(mut.Delta)
			return nil
		}
		num, ok := toFloat(existing)
		if !ok {
			return fmt.Errorf("value at %s is not numeric", key)
		}
		obj[key] = normalizeNumber(num + mut.Delta)
	case MutationRemove:
		if ok {
			delete(obj, key)
		}
	default:
		return fmt.Errorf("unknown mutation kind")
	}
	return nil
}

func applyMutationToArray(arr []any, index int, mut Mutation, skipMissing bool) error {
	if index < 0 || index >= len(arr) {
		return nil
	}
	existing := arr[index]
	switch mut.Kind {
	case MutationSet:
		if index < len(arr) || !skipMissing {
			arr[index] = mut.Value
		}
	case MutationIncrement:
		num, ok := toFloat(existing)
		if !ok {
			return fmt.Errorf("value at index %d is not numeric", index)
		}
		arr[index] = normalizeNumber(num + mut.Delta)
	case MutationRemove:
		arr[index] = nil
	default:
		return fmt.Errorf("unknown mutation kind")
	}
	return nil
}

func toFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case json.Number:
		f, err := val.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	default:
		return 0, false
	}
}

func normalizeNumber(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return f
	}
	if math.Abs(f-math.Round(f)) < 1e-9 {
		return int64(math.Round(f))
	}
	return f
}
