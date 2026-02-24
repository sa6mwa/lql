package lql

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"

	"pkt.systems/lql/jsonpointer"
)

const maxProjectionFieldIndex = 1 << 20

var projectFieldsReaderPool = sync.Pool{
	New: func() any {
		return bufio.NewReaderSize(bytes.NewReader(nil), 64*1024)
	},
}

type projectFieldsEmptyReader struct{}

func (projectFieldsEmptyReader) Read([]byte) (int, error) { return 0, io.EOF }

var projectFieldsZeroReader io.Reader = projectFieldsEmptyReader{}

var projectFieldsStatePool = sync.Pool{
	New: func() any {
		return &projectFieldsState{
			rootObject:      make(map[string]any, 8),
			keyScratch:      make([]byte, 0, 256),
			keyOrderScratch: make([]string, 0, 16),
			valueHint:       256,
		}
	},
}

// ProjectionPath is a compiled JSON Pointer path used for field projection.
type ProjectionPath struct {
	Raw      string
	Segments []string
}

// ProjectionPlan stores compiled projection paths for repeated invocations.
type ProjectionPlan struct {
	paths []ProjectionPath
	trie  *projectionTrieNode
}

// NewProjectionPlan compiles projection paths for repeated ProjectFields calls.
func NewProjectionPlan(paths []ProjectionPath) (*ProjectionPlan, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("projection paths required")
	}
	trie, err := buildProjectionTrie(paths)
	if err != nil {
		return nil, err
	}
	cloned := make([]ProjectionPath, len(paths))
	copy(cloned, paths)
	return &ProjectionPlan{paths: cloned, trie: trie}, nil
}

// Paths returns a copy of compiled projection paths.
func (p *ProjectionPlan) Paths() []ProjectionPath {
	if p == nil || len(p.paths) == 0 {
		return nil
	}
	out := make([]ProjectionPath, len(p.paths))
	copy(out, p.paths)
	return out
}

// ParseProjectionPaths parses repeated JSON Pointer fields used by projection.
func ParseProjectionPaths(paths []string) ([]ProjectionPath, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	out := make([]ProjectionPath, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, raw := range paths {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		segments, err := jsonpointer.Split(trimmed)
		if err != nil {
			return nil, err
		}
		if len(segments) == 0 {
			return nil, fmt.Errorf("field path %q refers to document root", raw)
		}
		if idx, err := strconv.Atoi(segments[0]); err == nil && idx >= 0 {
			return nil, fmt.Errorf("field path %q must not start with an array index", raw)
		}
		out = append(out, ProjectionPath{Raw: trimmed, Segments: segments})
		seen[trimmed] = struct{}{}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no valid field paths provided")
	}
	return out, nil
}

// ProjectFieldsRequest configures streaming projection for one JSON value.
//
// Reader must provide exactly one JSON value. The root value must be an object.
// Paths are matched as JSON Pointer segments and only matched fields are emitted.
// Selected field payloads are spooled in memory and spill to disk by spool config.
type ProjectFieldsRequest struct {
	Ctx    context.Context
	Reader io.Reader
	Writer io.Writer
	Paths  []ProjectionPath
	Plan   *ProjectionPlan

	MaxOutputBytes int64

	SpoolMemoryBytes int64
	SpoolTempDir     string
	SpoolFilePattern string
}

// ProjectFieldsResult describes one projection invocation.
type ProjectFieldsResult struct {
	Found bool
	Size  int64
}

// ProjectFields projects selected fields from one JSON object without
// materializing the full input document.
func ProjectFields(req ProjectFieldsRequest) (result ProjectFieldsResult, err error) {
	if req.Reader == nil {
		return ProjectFieldsResult{}, &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: "projection reader required",
			Offset: -1,
		}
	}
	if req.Writer == nil {
		return ProjectFieldsResult{}, &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: "projection writer required",
			Offset: -1,
		}
	}
	if req.Plan == nil && len(req.Paths) == 0 {
		return ProjectFieldsResult{}, &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: "projection paths required",
			Offset: -1,
		}
	}

	var trie *projectionTrieNode
	if req.Plan != nil {
		trie = req.Plan.trie
		if trie == nil {
			return ProjectFieldsResult{}, &StreamError{
				Code:   StreamErrorInvalidBody,
				Detail: "projection plan is not initialized",
				Offset: -1,
			}
		}
	} else {
		trie, err = buildProjectionTrie(req.Paths)
		if err != nil {
			return ProjectFieldsResult{}, &StreamError{
				Code:   StreamErrorInvalidBody,
				Detail: "invalid projection paths",
				Offset: -1,
				Err:    err,
			}
		}
	}

	ctx := normalizeStreamContext(req.Ctx)
	state := acquireProjectFieldsState()
	state.reset(ctx, trie, normalizeStreamSpoolConfig(req.SpoolMemoryBytes, req.SpoolTempDir, req.SpoolFilePattern))

	bufReader, pooled := acquireProjectFieldsReader(req.Reader)
	state.reader.Reset(ctx, bufReader)
	state.resetParser(ctx, &state.reader)

	start, readErr := readNonSpaceByte(&state.reader)
	if readErr != nil {
		if readErr == io.EOF {
			return finalizeProjectFields(state, bufReader, pooled, ProjectFieldsResult{}, &StreamError{
				Code:   StreamErrorInvalidBody,
				Detail: "projection input is empty",
				Offset: -1,
			})
		}
		return finalizeProjectFields(state, bufReader, pooled, ProjectFieldsResult{}, state.wrapError(readErr))
	}
	if start != '{' {
		return finalizeProjectFields(state, bufReader, pooled, ProjectFieldsResult{}, &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: "field selection requires JSON objects",
			Offset: state.reader.Offset(),
		})
	}
	if err := state.projectObject(trie); err != nil {
		return finalizeProjectFields(state, bufReader, pooled, ProjectFieldsResult{}, state.wrapError(err))
	}
	if trailing, trailingErr := readNonSpaceByte(&state.reader); trailingErr == nil {
		return finalizeProjectFields(state, bufReader, pooled, ProjectFieldsResult{}, &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: fmt.Sprintf("unexpected trailing token %q", trailing),
			Offset: state.reader.Offset(),
		})
	} else if trailingErr != io.EOF {
		return finalizeProjectFields(state, bufReader, pooled, ProjectFieldsResult{}, state.wrapError(trailingErr))
	}
	if !state.found {
		return finalizeProjectFields(state, bufReader, pooled, ProjectFieldsResult{Found: false, Size: 0}, nil)
	}

	state.countWriter.writer = req.Writer
	state.countWriter.size = 0
	state.countWriter.max = req.MaxOutputBytes
	if err := writeProjectedValue(&state.countWriter, state.outputRoot, &state.keyScratch, &state.keyOrderScratch); err != nil {
		return finalizeProjectFields(state, bufReader, pooled, ProjectFieldsResult{}, state.wrapError(err))
	}
	return finalizeProjectFields(state, bufReader, pooled, ProjectFieldsResult{Found: true, Size: state.countWriter.size}, nil)
}

func acquireProjectFieldsReader(reader io.Reader) (*bufio.Reader, bool) {
	if buffered, ok := reader.(*bufio.Reader); ok {
		return buffered, false
	}
	buffered := projectFieldsReaderPool.Get().(*bufio.Reader)
	buffered.Reset(reader)
	return buffered, true
}

func releaseProjectFieldsReader(reader *bufio.Reader, pooled bool) {
	if !pooled || reader == nil {
		return
	}
	reader.Reset(projectFieldsZeroReader)
	projectFieldsReaderPool.Put(reader)
}

func finalizeProjectFields(
	state *projectFieldsState,
	reader *bufio.Reader,
	pooled bool,
	result ProjectFieldsResult,
	err error,
) (ProjectFieldsResult, error) {
	releaseProjectFieldsReader(reader, pooled)
	cleanupErr := state.cleanup()
	if cleanupErr != nil && err == nil {
		err = &StreamError{
			Code:   StreamErrorInternal,
			Detail: "projection spool cleanup failed",
			Offset: state.offset(),
			Err:    cleanupErr,
		}
	}
	releaseProjectFieldsState(state)
	return result, err
}

type projectionTrieNode struct {
	path          *ProjectionPath
	objectChild   map[string]*projectionTrieNode
	arrayChild    map[int]*projectionTrieNode
	childrenOrder []string
}

func buildProjectionTrie(paths []ProjectionPath) (*projectionTrieNode, error) {
	root := &projectionTrieNode{
		objectChild: make(map[string]*projectionTrieNode),
	}
	for idx := range paths {
		path := paths[idx]
		if len(path.Segments) == 0 {
			return nil, fmt.Errorf("field path %q refers to document root", path.Raw)
		}
		node := root
		for segIdx, segment := range path.Segments {
			if node.path != nil && segIdx < len(path.Segments) {
				return nil, fmt.Errorf("field path conflict between %q and %q", node.path.Raw, path.Raw)
			}
			next, exists := node.objectChild[segment]
			if !exists {
				next = &projectionTrieNode{
					objectChild: make(map[string]*projectionTrieNode),
				}
				node.objectChild[segment] = next
				node.childrenOrder = append(node.childrenOrder, segment)
			}
			node = next
		}
		if node.path != nil {
			continue
		}
		if len(node.objectChild) > 0 {
			first := node.childrenOrder[0]
			conflictPath := path.Raw + "/" + first
			return nil, fmt.Errorf("field path conflict between %q and %q", path.Raw, conflictPath)
		}
		node.path = &paths[idx]
	}

	if err := finalizeProjectionTrie(root); err != nil {
		return nil, err
	}
	return root, nil
}

func finalizeProjectionTrie(node *projectionTrieNode) error {
	if node == nil {
		return nil
	}
	if len(node.objectChild) > 0 {
		node.arrayChild = make(map[int]*projectionTrieNode)
	}
	for key, child := range node.objectChild {
		if err := finalizeProjectionTrie(child); err != nil {
			return err
		}
		idx, err := strconv.Atoi(key)
		if err != nil || idx < 0 || idx > maxProjectionFieldIndex {
			continue
		}
		if existing, exists := node.arrayChild[idx]; exists && existing != child {
			return fmt.Errorf("field path conflict at array index %d", idx)
		}
		node.arrayChild[idx] = child
	}
	return nil
}

type projectFieldsState struct {
	ctx context.Context

	parser mutateStreamState
	trie   *projectionTrieNode
	reader streamByteReader

	spoolCfg        streamSpoolConfig
	spools          []*streamCandidateSpool
	nextSpool       int
	leaves          []projectionLeaf
	nextLeaf        int
	rootObject      map[string]any
	outputRoot      any
	keyScratch      []byte
	keyOrderScratch []string
	countWriter     projectionCountWriter
	valueHint       int
	found           bool
}

func acquireProjectFieldsState() *projectFieldsState {
	return projectFieldsStatePool.Get().(*projectFieldsState)
}

func releaseProjectFieldsState(state *projectFieldsState) {
	if state == nil {
		return
	}
	state.ctx = nil
	state.trie = nil
	state.spoolCfg = streamSpoolConfig{}
	state.nextSpool = 0
	state.nextLeaf = 0
	state.outputRoot = state.rootObject
	state.keyScratch = state.keyScratch[:0]
	state.keyOrderScratch = state.keyOrderScratch[:0]
	state.countWriter.writer = nil
	state.countWriter.size = 0
	state.countWriter.max = 0
	state.found = false
	state.parser.ctx = nil
	state.parser.reader = nil
	state.parser.stringBuf = state.parser.stringBuf[:0]
	state.parser.numberBuf = state.parser.numberBuf[:0]
	state.reader = streamByteReader{}
	projectFieldsStatePool.Put(state)
}

func (s *projectFieldsState) reset(ctx context.Context, trie *projectionTrieNode, cfg streamSpoolConfig) {
	s.ctx = ctx
	s.trie = trie
	s.spoolCfg = cfg
	s.nextSpool = 0
	s.nextLeaf = 0
	s.outputRoot = s.rootObject
	for key := range s.rootObject {
		delete(s.rootObject, key)
	}
	s.keyScratch = s.keyScratch[:0]
	s.keyOrderScratch = s.keyOrderScratch[:0]
	s.countWriter.writer = nil
	s.countWriter.size = 0
	s.countWriter.max = 0
	s.found = false
}

func (s *projectFieldsState) resetParser(ctx context.Context, reader *streamByteReader) {
	s.parser.ctx = ctx
	s.parser.reader = reader
	s.parser.stringBuf = s.parser.stringBuf[:0]
	s.parser.numberBuf = s.parser.numberBuf[:0]
}

func (s *projectFieldsState) offset() int64 {
	if s.parser.reader == nil {
		return -1
	}
	return s.parser.reader.Offset()
}

func (s *projectFieldsState) cleanup() error {
	var firstErr error
	for i := 0; i < s.nextSpool; i++ {
		candidate := s.spools[i]
		if candidate == nil {
			continue
		}
		if err := candidate.cleanup(true); err != nil && firstErr == nil {
			firstErr = err
		}
		releaseStreamCandidateSpool(candidate)
		s.spools[i] = nil
	}
	for i := 0; i < s.nextLeaf; i++ {
		s.leaves[i].candidate = nil
	}
	s.nextSpool = 0
	s.nextLeaf = 0
	return firstErr
}

func (s *projectFieldsState) acquireCandidateSpool() *streamCandidateSpool {
	idx := s.nextSpool
	s.nextSpool++
	if idx >= len(s.spools) {
		s.spools = append(s.spools, nil)
	}
	candidate := s.spools[idx]
	if candidate == nil {
		candidate = acquireStreamCandidateSpool(s.spoolCfg, s.valueHint)
		s.spools[idx] = candidate
		return candidate
	}
	candidate.resetForCandidate(s.spoolCfg, s.valueHint)
	return candidate
}

func (s *projectFieldsState) acquireLeaf(candidate *streamCandidateSpool) *projectionLeaf {
	idx := s.nextLeaf
	s.nextLeaf++
	if idx >= len(s.leaves) {
		s.leaves = append(s.leaves, projectionLeaf{})
	}
	s.leaves[idx].candidate = candidate
	return &s.leaves[idx]
}

func (s *projectFieldsState) wrapError(err error) error {
	if err == nil {
		return nil
	}
	var streamErr *StreamError
	if errors.As(err, &streamErr) {
		return streamErr
	}
	var tooLarge *projectionOutputTooLargeError
	if errors.As(err, &tooLarge) {
		return &StreamError{
			Code:   StreamErrorDocumentTooLarge,
			Detail: fmt.Sprintf("projection exceeds max output bytes (%d > %d)", tooLarge.size, tooLarge.max),
			Offset: s.offset(),
			Err:    err,
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &StreamError{
			Code:   StreamErrorContextCanceled,
			Detail: "context canceled",
			Offset: s.offset(),
			Err:    err,
		}
	}
	return &StreamError{
		Code:   StreamErrorInvalidBody,
		Detail: "invalid json stream",
		Offset: s.offset(),
		Err:    err,
	}
}

func (s *projectFieldsState) projectValue(start byte, node *projectionTrieNode) error {
	if node == nil {
		return s.skipValue(start)
	}
	if node.path != nil {
		candidate := s.acquireCandidateSpool()
		if err := s.copyValue(start, candidate); err != nil {
			_ = candidate.cleanup(false)
			return err
		}
		s.valueHint = candidate.SizeHint()

		leaf := s.acquireLeaf(candidate)
		next, err := assignProjectedField(s.outputRoot, node.path.Segments, leaf)
		if err != nil {
			return err
		}
		s.outputRoot = next
		s.found = true
		return nil
	}
	switch start {
	case '{':
		return s.projectObject(node)
	case '[':
		return s.projectArray(node)
	default:
		return s.skipValue(start)
	}
}

func (s *projectFieldsState) projectObject(node *projectionTrieNode) error {
	next, err := readNonSpaceByte(s.parser.reader)
	if err != nil {
		return err
	}
	if next == '}' {
		return nil
	}

	for {
		if next != '"' {
			return fmt.Errorf("expected string object key")
		}
		keyBytes, err := s.parser.readString(s.parser.reader)
		if err != nil {
			return err
		}
		key := bytesToStringUnsafe(keyBytes)

		colon, err := readNonSpaceByte(s.parser.reader)
		if err != nil {
			return err
		}
		if colon != ':' {
			return fmt.Errorf("expected ':' after object key")
		}

		valueStart, err := readNonSpaceByte(s.parser.reader)
		if err != nil {
			return err
		}
		if err := s.projectValue(valueStart, node.objectChild[key]); err != nil {
			return err
		}

		next, err = readNonSpaceByte(s.parser.reader)
		if err != nil {
			return err
		}
		switch next {
		case ',':
			next, err = readNonSpaceByte(s.parser.reader)
			if err != nil {
				return err
			}
		case '}':
			return nil
		default:
			return fmt.Errorf("expected ',' or '}' in object, got %q", next)
		}
	}
}

func (s *projectFieldsState) projectArray(node *projectionTrieNode) error {
	next, err := readNonSpaceByte(s.parser.reader)
	if err != nil {
		return err
	}
	if next == ']' {
		return nil
	}

	index := 0
	for {
		var child *projectionTrieNode
		if node.arrayChild != nil {
			child = node.arrayChild[index]
		}
		if err := s.projectValue(next, child); err != nil {
			return err
		}
		index++

		next, err = readNonSpaceByte(s.parser.reader)
		if err != nil {
			return err
		}
		switch next {
		case ',':
			next, err = readNonSpaceByte(s.parser.reader)
			if err != nil {
				return err
			}
		case ']':
			return nil
		default:
			return fmt.Errorf("expected ',' or ']' in array, got %q", next)
		}
	}
}

func (s *projectFieldsState) skipValue(start byte) error {
	switch start {
	case '{':
		return s.skipObject()
	case '[':
		return s.skipArray()
	case '"':
		return s.skipString()
	case 't':
		return expectLiteral(s.parser.reader, "rue")
	case 'f':
		return expectLiteral(s.parser.reader, "alse")
	case 'n':
		return expectLiteral(s.parser.reader, "ull")
	default:
		if !isNumberStart(start) {
			return fmt.Errorf("unexpected value start %q", start)
		}
		_, err := s.parser.readNumber(s.parser.reader, start)
		return err
	}
}

func (s *projectFieldsState) copyValue(start byte, out mutateByteWriter) error {
	switch start {
	case '{':
		if err := out.WriteByte('{'); err != nil {
			return err
		}
		return s.copyObject(out)
	case '[':
		if err := out.WriteByte('['); err != nil {
			return err
		}
		return s.copyArray(out)
	case '"':
		return s.copyString(out)
	case 't':
		if err := expectLiteral(s.parser.reader, "rue"); err != nil {
			return err
		}
		_, err := out.Write(jsonTrueLiteral)
		return err
	case 'f':
		if err := expectLiteral(s.parser.reader, "alse"); err != nil {
			return err
		}
		_, err := out.Write(jsonFalseLiteral)
		return err
	case 'n':
		if err := expectLiteral(s.parser.reader, "ull"); err != nil {
			return err
		}
		_, err := out.Write(jsonNullLiteral)
		return err
	default:
		if !isNumberStart(start) {
			return fmt.Errorf("unexpected value start %q", start)
		}
		number, err := s.parser.readNumber(s.parser.reader, start)
		if err != nil {
			return err
		}
		_, err = out.Write(number)
		return err
	}
}

func (s *projectFieldsState) skipObject() error {
	next, err := readNonSpaceByte(s.parser.reader)
	if err != nil {
		return err
	}
	if next == '}' {
		return nil
	}
	for {
		if next != '"' {
			return fmt.Errorf("expected string object key")
		}
		if err := s.skipString(); err != nil {
			return err
		}
		colon, err := readNonSpaceByte(s.parser.reader)
		if err != nil {
			return err
		}
		if colon != ':' {
			return fmt.Errorf("expected ':' after object key")
		}
		valueStart, err := readNonSpaceByte(s.parser.reader)
		if err != nil {
			return err
		}
		if err := s.skipValue(valueStart); err != nil {
			return err
		}
		next, err = readNonSpaceByte(s.parser.reader)
		if err != nil {
			return err
		}
		switch next {
		case ',':
			next, err = readNonSpaceByte(s.parser.reader)
			if err != nil {
				return err
			}
		case '}':
			return nil
		default:
			return fmt.Errorf("expected ',' or '}' in object, got %q", next)
		}
	}
}

func (s *projectFieldsState) skipArray() error {
	next, err := readNonSpaceByte(s.parser.reader)
	if err != nil {
		return err
	}
	if next == ']' {
		return nil
	}
	for {
		if err := s.skipValue(next); err != nil {
			return err
		}
		next, err = readNonSpaceByte(s.parser.reader)
		if err != nil {
			return err
		}
		switch next {
		case ',':
			next, err = readNonSpaceByte(s.parser.reader)
			if err != nil {
				return err
			}
		case ']':
			return nil
		default:
			return fmt.Errorf("expected ',' or ']' in array, got %q", next)
		}
	}
}

func (s *projectFieldsState) copyObject(out mutateByteWriter) error {
	next, err := readNonSpaceByte(s.parser.reader)
	if err != nil {
		return err
	}
	if next == '}' {
		return out.WriteByte('}')
	}
	first := true
	for {
		if next != '"' {
			return fmt.Errorf("expected string object key")
		}
		if !first {
			if err := out.WriteByte(','); err != nil {
				return err
			}
		}
		if err := s.copyString(out); err != nil {
			return err
		}
		colon, err := readNonSpaceByte(s.parser.reader)
		if err != nil {
			return err
		}
		if colon != ':' {
			return fmt.Errorf("expected ':' after object key")
		}
		if err := out.WriteByte(':'); err != nil {
			return err
		}
		valueStart, err := readNonSpaceByte(s.parser.reader)
		if err != nil {
			return err
		}
		if err := s.copyValue(valueStart, out); err != nil {
			return err
		}
		next, err = readNonSpaceByte(s.parser.reader)
		if err != nil {
			return err
		}
		switch next {
		case ',':
			next, err = readNonSpaceByte(s.parser.reader)
			if err != nil {
				return err
			}
			first = false
		case '}':
			return out.WriteByte('}')
		default:
			return fmt.Errorf("expected ',' or '}' in object, got %q", next)
		}
	}
}

func (s *projectFieldsState) copyArray(out mutateByteWriter) error {
	next, err := readNonSpaceByte(s.parser.reader)
	if err != nil {
		return err
	}
	if next == ']' {
		return out.WriteByte(']')
	}
	first := true
	for {
		if !first {
			if err := out.WriteByte(','); err != nil {
				return err
			}
		}
		if err := s.copyValue(next, out); err != nil {
			return err
		}
		first = false
		next, err = readNonSpaceByte(s.parser.reader)
		if err != nil {
			return err
		}
		switch next {
		case ',':
			next, err = readNonSpaceByte(s.parser.reader)
			if err != nil {
				return err
			}
		case ']':
			return out.WriteByte(']')
		default:
			return fmt.Errorf("expected ',' or ']' in array, got %q", next)
		}
	}
}

func (s *projectFieldsState) copyString(out mutateByteWriter) error {
	if err := out.WriteByte('"'); err != nil {
		return err
	}
	for {
		ch, err := s.parser.reader.ReadByte()
		if err != nil {
			return err
		}
		switch ch {
		case '"':
			return out.WriteByte('"')
		case '\\':
			if err := out.WriteByte('\\'); err != nil {
				return err
			}
			escaped, err := s.parser.reader.ReadByte()
			if err != nil {
				return err
			}
			switch escaped {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				if err := out.WriteByte(escaped); err != nil {
					return err
				}
			case 'u':
				if err := out.WriteByte('u'); err != nil {
					return err
				}
				for i := 0; i < 4; i++ {
					hex, err := s.parser.reader.ReadByte()
					if err != nil {
						return err
					}
					if !isHexDigit(hex) {
						return fmt.Errorf("invalid unicode escape")
					}
					if err := out.WriteByte(hex); err != nil {
						return err
					}
				}
			default:
				return fmt.Errorf("invalid string escape \\%c", escaped)
			}
		default:
			if ch < 0x20 {
				return fmt.Errorf("invalid control character in string")
			}
			if err := out.WriteByte(ch); err != nil {
				return err
			}
		}
	}
}

func (s *projectFieldsState) skipString() error {
	for {
		if buffered := s.parser.reader.Buffered(); buffered > 0 {
			chunk, err := s.parser.reader.Peek(buffered)
			if err != nil && err != io.EOF {
				return err
			}
			prefix := leadingProjectionJSONStringBytes(chunk)
			if prefix > 0 {
				if _, err := s.parser.reader.Discard(prefix); err != nil {
					return err
				}
				continue
			}
		}

		ch, err := s.parser.reader.ReadByte()
		if err != nil {
			return err
		}
		switch ch {
		case '"':
			return nil
		case '\\':
			escaped, err := s.parser.reader.ReadByte()
			if err != nil {
				return err
			}
			switch escaped {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			case 'u':
				for i := 0; i < 4; i++ {
					hex, err := s.parser.reader.ReadByte()
					if err != nil {
						return err
					}
					if !isHexDigit(hex) {
						return fmt.Errorf("invalid unicode escape")
					}
				}
			default:
				return fmt.Errorf("invalid string escape \\%c", escaped)
			}
		default:
			if ch < 0x20 {
				return fmt.Errorf("invalid control character in string")
			}
		}
	}
}

func leadingProjectionJSONStringBytes(buf []byte) int {
	for i, ch := range buf {
		if ch == '"' || ch == '\\' || ch < 0x20 {
			return i
		}
	}
	return len(buf)
}

type projectionLeaf struct {
	candidate *streamCandidateSpool
}

func assignProjectedField(node any, path []string, value *projectionLeaf) (any, error) {
	if len(path) == 0 {
		return value, nil
	}
	token := path[0]
	last := len(path) == 1
	if idx, ok := parseProjectionArrayIndexSegment(token); ok {
		if idx < 0 || idx > maxProjectionFieldIndex {
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
				if grow > maxProjectionFieldIndex {
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
		next, err := assignProjectedField(arr[idx], path[1:], value)
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
	next, err := assignProjectedField(obj[token], path[1:], value)
	if err != nil {
		return nil, err
	}
	obj[token] = next
	return obj, nil
}

func parseProjectionArrayIndexSegment(token string) (int, bool) {
	if token == "" {
		return 0, false
	}
	for i := 0; i < len(token); i++ {
		ch := token[i]
		if ch < '0' || ch > '9' {
			return 0, false
		}
	}
	idx, err := strconv.Atoi(token)
	if err != nil {
		return 0, false
	}
	return idx, true
}

func writeProjectedValue(w *projectionCountWriter, node any, keyScratch *[]byte, keyOrderScratch *[]string) error {
	switch v := node.(type) {
	case map[string]any:
		if err := w.WriteByte('{'); err != nil {
			return err
		}
		if len(v) == 1 {
			for key, child := range v {
				*keyScratch = appendJSONStringString((*keyScratch)[:0], key)
				if _, err := w.Write(*keyScratch); err != nil {
					return err
				}
				if err := w.WriteByte(':'); err != nil {
					return err
				}
				if err := writeProjectedValue(w, child, keyScratch, keyOrderScratch); err != nil {
					return err
				}
			}
			if err := w.WriteByte('}'); err != nil {
				return err
			}
			return nil
		}
		keys := (*keyOrderScratch)[:0]
		for key := range v {
			keys = append(keys, key)
		}
		*keyOrderScratch = keys
		sort.Strings(keys)
		for i, key := range keys {
			if i > 0 {
				if err := w.WriteByte(','); err != nil {
					return err
				}
			}
			*keyScratch = appendJSONStringString((*keyScratch)[:0], key)
			if _, err := w.Write(*keyScratch); err != nil {
				return err
			}
			if err := w.WriteByte(':'); err != nil {
				return err
			}
			if err := writeProjectedValue(w, v[key], keyScratch, keyOrderScratch); err != nil {
				return err
			}
		}
		if err := w.WriteByte('}'); err != nil {
			return err
		}
		return nil
	case []any:
		if err := w.WriteByte('['); err != nil {
			return err
		}
		for i := range v {
			if i > 0 {
				if err := w.WriteByte(','); err != nil {
					return err
				}
			}
			if v[i] == nil {
				if _, err := w.Write(jsonNullLiteral); err != nil {
					return err
				}
				continue
			}
			if err := writeProjectedValue(w, v[i], keyScratch, keyOrderScratch); err != nil {
				return err
			}
		}
		if err := w.WriteByte(']'); err != nil {
			return err
		}
		return nil
	case *projectionLeaf:
		if v == nil || v.candidate == nil {
			_, err := w.Write(jsonNullLiteral)
			return err
		}
		if payload := v.candidate.PayloadBytes(); payload != nil {
			_, err := w.Write(payload)
			return err
		}
		reader, err := v.candidate.Open()
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(w, reader)
		closeErr := reader.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	default:
		_, err := w.Write(jsonNullLiteral)
		return err
	}
}

type projectionCountWriter struct {
	writer io.Writer
	size   int64
	max    int64
	one    [1]byte
}

func (w *projectionCountWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	next := w.size + int64(len(p))
	if w.max > 0 && next > w.max {
		return 0, &projectionOutputTooLargeError{size: next, max: w.max}
	}
	n, err := w.writer.Write(p)
	w.size += int64(n)
	if err != nil {
		return n, err
	}
	if n != len(p) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

func (w *projectionCountWriter) WriteByte(b byte) error {
	w.one[0] = b
	_, err := w.Write(w.one[:])
	return err
}

type projectionOutputTooLargeError struct {
	size int64
	max  int64
}

func (e *projectionOutputTooLargeError) Error() string {
	if e == nil {
		return "projection output too large"
	}
	return fmt.Sprintf("projection exceeds max output bytes (%d > %d)", e.size, e.max)
}

func isHexDigit(ch byte) bool {
	return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}
