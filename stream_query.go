package lql

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"unicode/utf16"
	"unicode/utf8"
	"unsafe"
)

var queryStreamPayloadHint = newStreamAdaptiveHint(256, 64*1024, 8*1024*1024)

// QueryStreamRequest configures token-stream query evaluation.
type QueryStreamRequest struct {
	Ctx      context.Context
	Reader   io.Reader
	Selector Selector
	// Plan reuses compiled selector state across query invocations.
	// When set, Selector must be empty.
	Plan        QueryStreamPlan
	Mode        QueryStreamMode
	IncludeJSON bool
	// MatchedOnly invokes OnValue only for matched candidates.
	// Default false preserves callback-per-candidate behavior.
	MatchedOnly bool
	// SpoolMemoryBytes sets the in-memory payload threshold for IncludeJSON mode.
	// Values <= 0 default to 3 MiB.
	SpoolMemoryBytes int64
	// SpoolTempDir sets the temp directory used when payloads spill to disk.
	// Empty defaults to /tmp.
	SpoolTempDir string
	// SpoolFilePattern controls os.CreateTemp naming for spilled payload files.
	// Empty defaults to "lql-spool-*.json".
	SpoolFilePattern string
	// DisableInternalSpool requires caller-managed payload sink in IncludeJSON mode.
	DisableInternalSpool bool
	// PayloadSinkFactory creates a candidate payload sink for caller-managed spooling.
	// When set, QueryStream uses this sink instead of internal spool.
	PayloadSinkFactory QueryStreamPayloadSinkFactory
	// MaxCandidateBytes is measured as canonical candidate bytes from the first
	// non-whitespace byte of each candidate to its closing JSON token, excluding
	// surrounding top-level separators/whitespace.
	MaxCandidateBytes int64
	OnValue           func(QueryStreamValue) error
}

// QueryStreamPayloadSink receives candidate payload bytes in IncludeJSON mode.
// Implementations may keep bytes in memory, spill to disk, or forward elsewhere.
type QueryStreamPayloadSink interface {
	io.Writer
	// Finalize is called once after candidate parse before callback observation.
	Finalize() error
	// Open returns a readable payload view from offset 0.
	Open() (io.ReadCloser, error)
	// Bytes returns in-memory payload bytes when available; may return nil.
	Bytes() []byte
	// SizeHint allows reuse-aware callers to tune next allocation hint.
	SizeHint() int
	// Cleanup releases all resources; always called exactly once after callback,
	// including on errors.
	Cleanup() error
}

// QueryStreamPayloadSinkRequest describes one candidate sink allocation.
type QueryStreamPayloadSinkRequest struct {
	Offset int64
}

// QueryStreamPayloadSinkFactory creates per-candidate payload sinks.
type QueryStreamPayloadSinkFactory func(QueryStreamPayloadSinkRequest) (QueryStreamPayloadSink, error)

// QueryStreamValue describes one candidate JSON value from the stream.
//
// JSON and OpenJSON are valid only during the callback invocation.
// JSON may be nil when payloads are spooled to disk; use OpenJSON to read bytes.
type QueryStreamValue struct {
	OpenJSON func() (io.ReadCloser, error)
	JSON     []byte
	Size     int64
	Matched  bool
}

// QueryStreamPlan reuses compiled selector state for QueryStream.
//
// A zero-value plan is treated as unset.
type QueryStreamPlan struct {
	template *streamSelectorEngine
}

// IsZero reports whether the plan is unset.
func (p QueryStreamPlan) IsZero() bool {
	return p.template == nil
}

// NewQueryStreamPlan compiles selector state for reuse across QueryStream calls.
func NewQueryStreamPlan(selector Selector) (QueryStreamPlan, error) {
	engine, err := newStreamSelectorEngine(selector)
	if err != nil {
		return QueryStreamPlan{}, err
	}
	return QueryStreamPlan{template: engine}, nil
}

func (p QueryStreamPlan) newEngineInto(dst *streamSelectorEngine, hits []bool) ([]bool, error) {
	if p.template == nil {
		return nil, fmt.Errorf("query stream plan is unset")
	}
	*dst = *p.template
	n := len(p.template.hits)
	if n == 0 {
		dst.hits = nil
		return hits[:0], nil
	}
	if cap(hits) < n {
		hits = make([]bool, n)
	} else {
		hits = hits[:n]
		clear(hits)
	}
	dst.hits = hits
	return hits, nil
}

// QueryStream evaluates selector matches against a JSON stream without
// materializing full documents.
//
// Top-level arrays are treated as streams of candidate values.
func QueryStream(req QueryStreamRequest) error {
	if req.Reader == nil {
		return &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: "query stream reader required",
			Offset: -1,
		}
	}
	if req.OnValue == nil {
		return &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: "query stream callback required",
			Offset: -1,
		}
	}
	ctx := normalizeStreamContext(req.Ctx)
	includeJSON := req.IncludeJSON
	switch req.Mode {
	case QueryModeAuto:
	case QueryDecisionOnly:
		includeJSON = false
	case QueryDecisionPlusValue:
		includeJSON = true
	default:
		return &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: fmt.Sprintf("unknown query stream mode %d", req.Mode),
			Offset: -1,
		}
	}
	if includeJSON && req.DisableInternalSpool && req.PayloadSinkFactory == nil {
		return &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: "query stream caller spool requested but payload sink factory is nil",
			Offset: -1,
		}
	}

	state := acquireStreamScanState()
	defer releaseStreamScanState(state)
	state.reset(ctx, includeJSON, req)

	if !req.Plan.IsZero() {
		if !req.Selector.IsEmpty() {
			return &StreamError{
				Code:   StreamErrorInvalidBody,
				Detail: "query stream request must set either selector or plan, not both",
				Offset: -1,
			}
		}
		hits, err := req.Plan.newEngineInto(&state.engineStorage, state.hitsScratch)
		if err != nil {
			return &StreamError{
				Code:   StreamErrorInvalidBody,
				Detail: "query stream plan is invalid",
				Offset: -1,
				Err:    err,
			}
		}
		state.hitsScratch = hits
		state.engine = &state.engineStorage
	} else {
		compiled, err := newStreamSelectorEngine(req.Selector)
		if err != nil {
			return &StreamError{
				Code:   StreamErrorInvalidSelector,
				Detail: "invalid selector",
				Offset: -1,
				Err:    err,
			}
		}
		state.engine = compiled
	}

	bufReader, ok := req.Reader.(*bufio.Reader)
	if !ok {
		bufReader = bufio.NewReaderSize(req.Reader, 64*1024)
	}
	state.readerStorage.Reset(ctx, bufReader)
	reader := &state.readerStorage
	for {
		start, err := readNonSpaceByte(reader)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return state.wrapStreamError(err, reader.Offset())
		}
		if err := state.consumeCandidate(reader, start); err != nil {
			return err
		}
	}
}

var streamScanStatePool = sync.Pool{
	New: func() any {
		state := &streamScanState{
			path:        make([]streamPathSegment, 0, 32),
			keyBytes:    make([]byte, 0, 256),
			jsonScratch: make([]byte, 0, 256),
			stringBuf:   make([]byte, 0, 256),
			numberBuf:   make([]byte, 0, 64),
		}
		state.openJSON = state.openCurrentPayload
		return state
	},
}

func acquireStreamScanState() *streamScanState {
	return streamScanStatePool.Get().(*streamScanState)
}

func releaseStreamScanState(state *streamScanState) {
	if state == nil {
		return
	}
	state.releaseInternalPayload()
	state.ctx = nil
	state.engine = nil
	state.includeJSON = false
	state.matchedOnly = false
	state.onValue = nil
	state.payloadSinkMaker = nil
	state.disableSpool = false
	state.maxCandidateBytes = 0
	state.path = state.path[:0]
	state.keyBytes = state.keyBytes[:0]
	state.payload = nil
	state.payloadBytes = nil
	state.usingInternal = false
	state.jsonScratch = state.jsonScratch[:0]
	state.candidateStart = 0
	state.readerStorage.r = nil
	state.readerStorage.ctx = nil
	state.readerStorage.checkCtx = false
	state.readerStorage.offset = 0
	state.stringBuf = state.stringBuf[:0]
	state.numberBuf = state.numberBuf[:0]
	streamScanStatePool.Put(state)
}

type streamScanState struct {
	ctx           context.Context
	engine        *streamSelectorEngine
	engineStorage streamSelectorEngine
	hitsScratch   []bool
	includeJSON   bool
	matchedOnly   bool
	onValue       func(QueryStreamValue) error

	payloadSinkMaker  QueryStreamPayloadSinkFactory
	disableSpool      bool
	maxCandidateBytes int64

	path           []streamPathSegment
	keyBytes       []byte
	spoolCfg       streamSpoolConfig
	payload        QueryStreamPayloadSink
	payloadBytes   interface{ WriteByte(byte) error }
	internalSink   internalQueryPayloadSink
	readerStorage  streamByteReader
	usingInternal  bool
	openJSON       func() (io.ReadCloser, error)
	jsonScratch    []byte
	lastValue      int
	candidateStart int64

	stringBuf []byte
	numberBuf []byte
	oneByte   [1]byte
}

func (s *streamScanState) reset(ctx context.Context, includeJSON bool, req QueryStreamRequest) {
	s.ctx = ctx
	s.engine = nil
	s.includeJSON = includeJSON
	s.matchedOnly = req.MatchedOnly
	s.onValue = req.OnValue
	s.payloadSinkMaker = req.PayloadSinkFactory
	s.disableSpool = req.DisableInternalSpool
	s.maxCandidateBytes = req.MaxCandidateBytes
	s.path = s.path[:0]
	s.keyBytes = s.keyBytes[:0]
	s.spoolCfg = normalizeStreamSpoolConfig(req.SpoolMemoryBytes, req.SpoolTempDir, req.SpoolFilePattern)
	s.payload = nil
	s.payloadBytes = nil
	s.usingInternal = false
	s.jsonScratch = s.jsonScratch[:0]
	s.stringBuf = s.stringBuf[:0]
	s.numberBuf = s.numberBuf[:0]
	s.lastValue = queryStreamPayloadHint.Load()
}

func (s *streamScanState) newPayloadSink() (QueryStreamPayloadSink, error) {
	if s.payloadSinkMaker != nil {
		payload, err := s.payloadSinkMaker(QueryStreamPayloadSinkRequest{Offset: s.candidateStart})
		if err != nil {
			return nil, err
		}
		if payload == nil {
			return nil, fmt.Errorf("query payload sink factory returned nil")
		}
		return payload, nil
	}
	if s.disableSpool {
		return nil, fmt.Errorf("query payload sink required when internal spool is disabled")
	}
	s.internalSink.prepare(s.spoolCfg, s.lastValue)
	s.usingInternal = true
	return &s.internalSink, nil
}

func (s *streamScanState) releaseInternalPayload() {
	if !s.usingInternal || !s.internalSink.initialized {
		return
	}
	_ = s.internalSink.spool.cleanup(false)
	s.usingInternal = false
}

func (s *streamScanState) openCurrentPayload() (io.ReadCloser, error) {
	if s.payload == nil {
		return nil, fmt.Errorf("query stream candidate payload unavailable")
	}
	return s.payload.Open()
}

type internalQueryPayloadSink struct {
	spool       streamCandidateSpool
	initialized bool
}

func (s *internalQueryPayloadSink) prepare(cfg streamSpoolConfig, hint int) {
	if !s.initialized {
		s.spool = streamCandidateSpool{
			cfg: cfg,
			mem: acquireSpoolMem(cfg, hint),
		}
		s.initialized = true
		return
	}
	s.spool.resetForCandidate(cfg, hint)
}

func (s *internalQueryPayloadSink) Write(p []byte) (int, error) {
	return s.spool.Write(p)
}

func (s *internalQueryPayloadSink) WriteByte(b byte) error {
	return s.spool.WriteByte(b)
}

func (s *internalQueryPayloadSink) Finalize() error {
	return s.spool.Finalize()
}

func (s *internalQueryPayloadSink) Open() (io.ReadCloser, error) {
	return s.spool.Open()
}

func (s *internalQueryPayloadSink) Bytes() []byte {
	return s.spool.PayloadBytes()
}

func (s *internalQueryPayloadSink) SizeHint() int {
	return s.spool.SizeHint()
}

func (s *internalQueryPayloadSink) Cleanup() error {
	return s.spool.cleanup(false)
}

func (s *streamScanState) consumeCandidate(reader *streamByteReader, start byte) error {
	s.candidateStart = reader.Offset() - 1
	if start == '[' {
		if err := s.consumeTopArray(reader); err != nil {
			return s.wrapStreamError(err, reader.Offset())
		}
		return nil
	}

	s.engine.reset()
	s.path = s.path[:0]
	s.keyBytes = s.keyBytes[:0]

	if s.includeJSON {
		payload, err := s.newPayloadSink()
		if err != nil {
			return s.wrapStreamError(err, reader.Offset())
		}
		s.payload = payload
		if bw, ok := payload.(interface{ WriteByte(byte) error }); ok {
			s.payloadBytes = bw
		} else {
			s.payloadBytes = nil
		}
	}

	kind, err := s.scanValue(reader, start)
	if err != nil {
		if s.includeJSON {
			_ = s.payload.Cleanup()
			s.payload = nil
			s.payloadBytes = nil
		}
		return s.wrapStreamError(err, reader.Offset())
	}
	size := reader.Offset() - s.candidateStart
	if s.maxCandidateBytes > 0 && size > s.maxCandidateBytes {
		if s.includeJSON {
			_ = s.payload.Cleanup()
			s.payload = nil
			s.payloadBytes = nil
		}
		return &StreamError{
			Code:   StreamErrorDocumentTooLarge,
			Detail: fmt.Sprintf("candidate exceeds max bytes (%d > %d)", size, s.maxCandidateBytes),
			Offset: reader.Offset(),
			Path:   s.currentPointer(),
		}
	}

	matched := s.engine.match(kind == streamValueObject)
	if s.matchedOnly && !matched {
		if s.includeJSON {
			s.lastValue = s.payload.SizeHint()
			if s.usingInternal {
				queryStreamPayloadHint.Observe(s.lastValue)
			}
			if cleanupErr := s.payload.Cleanup(); cleanupErr != nil {
				return &StreamError{
					Code:   StreamErrorInternal,
					Detail: "query payload cleanup failed",
					Offset: reader.Offset(),
					Path:   s.currentPointer(),
					Err:    cleanupErr,
				}
			}
			s.payload = nil
			s.payloadBytes = nil
		}
		return nil
	}
	out := QueryStreamValue{Matched: matched, Size: size}
	if s.includeJSON {
		if err := s.payload.Finalize(); err != nil {
			return s.wrapStreamError(err, reader.Offset())
		}
		out.JSON = s.payload.Bytes()
		out.OpenJSON = s.openJSON
	}
	err = s.onValue(out)
	if s.includeJSON {
		s.lastValue = s.payload.SizeHint()
		if s.usingInternal {
			queryStreamPayloadHint.Observe(s.lastValue)
		}
		if cleanupErr := s.payload.Cleanup(); cleanupErr != nil {
			return &StreamError{
				Code:   StreamErrorInternal,
				Detail: "query payload cleanup failed",
				Offset: reader.Offset(),
				Path:   s.currentPointer(),
				Err:    cleanupErr,
			}
		}
		s.payload = nil
		s.payloadBytes = nil
	}
	if err != nil {
		return s.wrapStreamError(err, reader.Offset())
	}
	return nil
}

func (s *streamScanState) wrapStreamError(err error, offset int64) error {
	if err == nil {
		return nil
	}
	var streamErr *StreamError
	if errors.As(err, &streamErr) {
		return streamErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &StreamError{
			Code:   StreamErrorContextCanceled,
			Detail: "context canceled",
			Offset: offset,
			Path:   s.currentPointer(),
			Err:    err,
		}
	}
	return &StreamError{
		Code:   StreamErrorInvalidBody,
		Detail: "invalid json stream",
		Offset: offset,
		Path:   s.currentPointer(),
		Err:    err,
	}
}

func (s *streamScanState) currentPointer() string {
	if len(s.path) == 0 {
		return ""
	}
	var b strings.Builder
	for _, segment := range s.path {
		switch segment.kind {
		case streamPathObjectKey:
			b.WriteByte('/')
			for _, r := range s.keyBytes[segment.keyStart:segment.keyEnd] {
				switch r {
				case '~':
					b.WriteString("~0")
				case '/':
					b.WriteString("~1")
				default:
					b.WriteRune(rune(r))
				}
			}
		case streamPathArrayIndex:
			b.WriteByte('/')
			b.WriteString(strconv.Itoa(segment.index))
		}
	}
	return b.String()
}

func (s *streamScanState) consumeTopArray(reader *streamByteReader) error {
	start, err := readByteOrSkipSpace(reader)
	if err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	if start == ']' {
		return nil
	}

	for {
		if err := s.consumeCandidate(reader, start); err != nil {
			return err
		}
		next, err := readNonSpaceByte(reader)
		if err != nil {
			return err
		}
		switch next {
		case ',':
			start, err = readByteOrSkipSpace(reader)
			if err != nil {
				return err
			}
		case ']':
			return nil
		default:
			return fmt.Errorf("expected ',' or ']' in top-level array, got %q", next)
		}
	}
}

func (s *streamScanState) scanValue(reader *streamByteReader, start byte) (streamValueKind, error) {
	switch start {
	case '{':
		if len(s.path) == 0 && s.engine != nil && s.engine.fastTopLevelEq != nil {
			return s.scanObjectFastTopLevelEq(reader, s.engine.fastTopLevelEq)
		}
		if len(s.path) == 0 && s.engine != nil && s.engine.fastTopLevel != nil {
			return s.scanObjectFastTopLevel(reader, s.engine.fastTopLevel)
		}
		s.engine.observe(s.path, s.keyBytes, streamValueObject, nil)
		if s.includeJSON {
			if err := s.payloadWriteByte('{'); err != nil {
				return streamValueInvalid, err
			}
		}
		return s.scanObject(reader)
	case '[':
		s.engine.observe(s.path, s.keyBytes, streamValueArray, nil)
		if s.includeJSON {
			if err := s.payloadWriteByte('['); err != nil {
				return streamValueInvalid, err
			}
		}
		return s.scanArray(reader)
	case '"':
		needsValue := s.engine.stringValueNeeded(s.path, s.keyBytes)
		if !needsValue {
			if s.includeJSON {
				if err := s.copyRawJSONString(reader); err != nil {
					return streamValueInvalid, err
				}
			} else if err := s.skipString(reader); err != nil {
				return streamValueInvalid, err
			}
			s.engine.observe(s.path, s.keyBytes, streamValueString, nil)
			return streamValueString, nil
		}
		value, err := s.readString(reader)
		if err != nil {
			return streamValueInvalid, err
		}
		if needsValue {
			s.engine.observe(s.path, s.keyBytes, streamValueString, bytesToStringUnsafe(value))
		} else {
			s.engine.observe(s.path, s.keyBytes, streamValueString, nil)
		}
		if s.includeJSON {
			if err := s.payloadWriteJSONString(value); err != nil {
				return streamValueInvalid, err
			}
		}
		return streamValueString, nil
	case 't':
		if err := expectLiteral(reader, "rue"); err != nil {
			return streamValueInvalid, err
		}
		s.engine.observe(s.path, s.keyBytes, streamValueBool, true)
		if s.includeJSON {
			if _, err := s.payload.Write(jsonTrueLiteral); err != nil {
				return streamValueInvalid, err
			}
		}
		return streamValueBool, nil
	case 'f':
		if err := expectLiteral(reader, "alse"); err != nil {
			return streamValueInvalid, err
		}
		s.engine.observe(s.path, s.keyBytes, streamValueBool, false)
		if s.includeJSON {
			if _, err := s.payload.Write(jsonFalseLiteral); err != nil {
				return streamValueInvalid, err
			}
		}
		return streamValueBool, nil
	case 'n':
		if err := expectLiteral(reader, "ull"); err != nil {
			return streamValueInvalid, err
		}
		s.engine.observe(s.path, s.keyBytes, streamValueNull, nil)
		if s.includeJSON {
			if _, err := s.payload.Write(jsonNullLiteral); err != nil {
				return streamValueInvalid, err
			}
		}
		return streamValueNull, nil
	default:
		if !isNumberStart(start) {
			return streamValueInvalid, fmt.Errorf("unexpected value start %q", start)
		}
		number, err := s.readNumber(reader, start)
		if err != nil {
			return streamValueInvalid, err
		}
		s.engine.observe(s.path, s.keyBytes, streamValueNumber, json.Number(bytesToStringUnsafe(number)))
		if s.includeJSON {
			if _, err := s.payload.Write(number); err != nil {
				return streamValueInvalid, err
			}
		}
		return streamValueNumber, nil
	}
}

func (s *streamScanState) scanObjectFastTopLevel(reader *streamByteReader, program *streamFastTopLevelProgram) (streamValueKind, error) {
	next, err := readNonSpaceByte(reader)
	if err != nil {
		return streamValueInvalid, err
	}
	if s.includeJSON {
		if err := s.payloadWriteByte('{'); err != nil {
			return streamValueInvalid, err
		}
	}
	if next == '}' {
		if s.includeJSON {
			if err := s.payloadWriteByte('}'); err != nil {
				return streamValueInvalid, err
			}
		}
		return streamValueObject, nil
	}

	first := true
	for {
		if next != '"' {
			return streamValueInvalid, fmt.Errorf("expected string object key")
		}
		key, err := s.readString(reader)
		if err != nil {
			return streamValueInvalid, err
		}
		if s.includeJSON {
			if !first {
				if err := s.payloadWriteByte(','); err != nil {
					return streamValueInvalid, err
				}
			}
			if err := s.payloadWriteJSONString(key); err != nil {
				return streamValueInvalid, err
			}
			if err := s.payloadWriteByte(':'); err != nil {
				return streamValueInvalid, err
			}
		}

		colon, err := readNonSpaceByte(reader)
		if err != nil {
			return streamValueInvalid, err
		}
		if colon != ':' {
			return streamValueInvalid, fmt.Errorf("expected ':' after object key")
		}

		valueStart, err := readNonSpaceByte(reader)
		if err != nil {
			return streamValueInvalid, err
		}
		field := program.fieldForKey(key)
		if field != nil && s.fastTopLevelFieldHasPending(field) {
			if err := s.fastTopLevelObserveValue(reader, valueStart, field); err != nil {
				return streamValueInvalid, err
			}
		} else {
			if err := s.copyOrSkipValue(reader, valueStart); err != nil {
				return streamValueInvalid, err
			}
		}

		next, err = readNonSpaceByte(reader)
		if err != nil {
			return streamValueInvalid, err
		}
		switch next {
		case ',':
			next, err = readNonSpaceByte(reader)
			if err != nil {
				return streamValueInvalid, err
			}
		case '}':
			if s.includeJSON {
				if err := s.payloadWriteByte('}'); err != nil {
					return streamValueInvalid, err
				}
			}
			return streamValueObject, nil
		default:
			return streamValueInvalid, fmt.Errorf("expected ',' or '}' in object, got %q", next)
		}
		first = false
	}
}

func (s *streamScanState) scanObjectFastTopLevelEq(reader *streamByteReader, clause *streamFastTopLevelEqClause) (streamValueKind, error) {
	next, err := readNonSpaceByte(reader)
	if err != nil {
		return streamValueInvalid, err
	}
	if s.includeJSON {
		if err := s.payloadWriteByte('{'); err != nil {
			return streamValueInvalid, err
		}
	}
	if next == '}' {
		if s.includeJSON {
			if err := s.payloadWriteByte('}'); err != nil {
				return streamValueInvalid, err
			}
		}
		s.engine.hits[clause.id] = false
		return streamValueObject, nil
	}

	matched := false
	first := true
	for {
		if next != '"' {
			return streamValueInvalid, fmt.Errorf("expected string object key")
		}
		key, err := s.readString(reader)
		if err != nil {
			return streamValueInvalid, err
		}
		if s.includeJSON {
			if !first {
				if err := s.payloadWriteByte(','); err != nil {
					return streamValueInvalid, err
				}
			}
			if err := s.payloadWriteJSONString(key); err != nil {
				return streamValueInvalid, err
			}
			if err := s.payloadWriteByte(':'); err != nil {
				return streamValueInvalid, err
			}
		}

		colon, err := readNonSpaceByte(reader)
		if err != nil {
			return streamValueInvalid, err
		}
		if colon != ':' {
			return streamValueInvalid, fmt.Errorf("expected ':' after object key")
		}

		valueStart, err := readNonSpaceByte(reader)
		if err != nil {
			return streamValueInvalid, err
		}
		if !matched && bytes.Equal(key, clause.keyBytes) {
			matched, err = s.fastTopLevelEqValueMatches(reader, valueStart, clause)
			if err != nil {
				return streamValueInvalid, err
			}
		} else {
			if err := s.copyOrSkipValue(reader, valueStart); err != nil {
				return streamValueInvalid, err
			}
		}

		next, err = readNonSpaceByte(reader)
		if err != nil {
			return streamValueInvalid, err
		}
		switch next {
		case ',':
			next, err = readNonSpaceByte(reader)
			if err != nil {
				return streamValueInvalid, err
			}
		case '}':
			if s.includeJSON {
				if err := s.payloadWriteByte('}'); err != nil {
					return streamValueInvalid, err
				}
			}
			s.engine.hits[clause.id] = matched
			return streamValueObject, nil
		default:
			return streamValueInvalid, fmt.Errorf("expected ',' or '}' in object, got %q", next)
		}
		first = false
	}
}

func (s *streamScanState) fastTopLevelEqValueMatches(reader *streamByteReader, start byte, clause *streamFastTopLevelEqClause) (bool, error) {
	switch start {
	case '"':
		if !clause.ignoreCase {
			if matched, ok, err := s.readBufferedUnescapedStringEquals(reader, clause.needle, s.includeJSON); err != nil {
				return false, err
			} else if ok {
				return matched, nil
			}
		}
		value, err := s.readString(reader)
		if err != nil {
			return false, err
		}
		if s.includeJSON {
			if err := s.payloadWriteJSONString(value); err != nil {
				return false, err
			}
		}
		return streamEqStringMatch(bytesToStringUnsafe(value), clause), nil
	case 't':
		if err := expectLiteral(reader, "rue"); err != nil {
			return false, err
		}
		if s.includeJSON {
			if _, err := s.payload.Write(jsonTrueLiteral); err != nil {
				return false, err
			}
		}
		return streamEqStringMatch("true", clause), nil
	case 'f':
		if err := expectLiteral(reader, "alse"); err != nil {
			return false, err
		}
		if s.includeJSON {
			if _, err := s.payload.Write(jsonFalseLiteral); err != nil {
				return false, err
			}
		}
		return streamEqStringMatch("false", clause), nil
	case 'n':
		if err := expectLiteral(reader, "ull"); err != nil {
			return false, err
		}
		if s.includeJSON {
			if _, err := s.payload.Write(jsonNullLiteral); err != nil {
				return false, err
			}
		}
		return false, nil
	case '{', '[':
		if s.includeJSON {
			return false, s.copyValue(reader, start)
		}
		return false, s.skipValue(reader, start)
	default:
		if !isNumberStart(start) {
			return false, fmt.Errorf("unexpected value start %q", start)
		}
		number, err := s.readNumber(reader, start)
		if err != nil {
			return false, err
		}
		if s.includeJSON {
			if _, err := s.payload.Write(number); err != nil {
				return false, err
			}
		}
		return streamEqStringMatch(bytesToStringUnsafe(number), clause), nil
	}
}

func (s *streamScanState) fastTopLevelFieldHasPending(field *streamFastTopLevelField) bool {
	for _, idx := range field.termIdxs {
		if !s.engine.hits[s.engine.termClauses[idx].id] {
			return true
		}
	}
	for _, idx := range field.rangeIdxs {
		if !s.engine.hits[s.engine.rangeClauses[idx].id] {
			return true
		}
	}
	for _, idx := range field.inIdxs {
		if !s.engine.hits[s.engine.inClauses[idx].id] {
			return true
		}
	}
	for _, idx := range field.existsIdxs {
		if !s.engine.hits[s.engine.existsClauses[idx].id] {
			return true
		}
	}
	return false
}

func (s *streamScanState) fastTopLevelObserveValue(reader *streamByteReader, start byte, field *streamFastTopLevelField) error {
	needTerm := false
	for _, idx := range field.termIdxs {
		if !s.engine.hits[s.engine.termClauses[idx].id] {
			needTerm = true
			break
		}
	}
	needRange := false
	for _, idx := range field.rangeIdxs {
		if !s.engine.hits[s.engine.rangeClauses[idx].id] {
			needRange = true
			break
		}
	}
	needIn := false
	for _, idx := range field.inIdxs {
		if !s.engine.hits[s.engine.inClauses[idx].id] {
			needIn = true
			break
		}
	}
	needExists := false
	for _, idx := range field.existsIdxs {
		if !s.engine.hits[s.engine.existsClauses[idx].id] {
			needExists = true
			break
		}
	}

	var candidate string
	hasCandidate := false
	var number float64
	hasNumber := false

	switch start {
	case '"':
		if needTerm || needIn {
			value, err := s.readString(reader)
			if err != nil {
				return err
			}
			if s.includeJSON {
				if err := s.payloadWriteJSONString(value); err != nil {
					return err
				}
			}
			candidate = bytesToStringUnsafe(value)
			hasCandidate = true
		} else if s.includeJSON {
			if err := s.copyRawJSONString(reader); err != nil {
				return err
			}
		} else if err := s.skipString(reader); err != nil {
			return err
		}
		if needExists {
			s.fastTopLevelMarkExists(field)
		}
	case 't':
		if err := expectLiteral(reader, "rue"); err != nil {
			return err
		}
		if s.includeJSON {
			if _, err := s.payload.Write(jsonTrueLiteral); err != nil {
				return err
			}
		}
		if needTerm || needIn {
			candidate = "true"
			hasCandidate = true
		}
		if needExists {
			s.fastTopLevelMarkExists(field)
		}
	case 'f':
		if err := expectLiteral(reader, "alse"); err != nil {
			return err
		}
		if s.includeJSON {
			if _, err := s.payload.Write(jsonFalseLiteral); err != nil {
				return err
			}
		}
		if needTerm || needIn {
			candidate = "false"
			hasCandidate = true
		}
		if needExists {
			s.fastTopLevelMarkExists(field)
		}
	case 'n':
		if err := expectLiteral(reader, "ull"); err != nil {
			return err
		}
		if s.includeJSON {
			if _, err := s.payload.Write(jsonNullLiteral); err != nil {
				return err
			}
		}
		return nil
	case '{', '[':
		if needExists {
			s.fastTopLevelMarkExists(field)
		}
		return s.copyOrSkipValue(reader, start)
	default:
		if !isNumberStart(start) {
			return fmt.Errorf("unexpected value start %q", start)
		}
		numberRaw, err := s.readNumber(reader, start)
		if err != nil {
			return err
		}
		if s.includeJSON {
			if _, err := s.payload.Write(numberRaw); err != nil {
				return err
			}
		}
		if needTerm || needIn {
			candidate = bytesToStringUnsafe(numberRaw)
			hasCandidate = true
		}
		if needRange {
			parsed, parseErr := json.Number(bytesToStringUnsafe(numberRaw)).Float64()
			if parseErr == nil {
				number = parsed
				hasNumber = true
			}
		}
		if needExists {
			s.fastTopLevelMarkExists(field)
		}
	}

	if hasCandidate {
		s.fastTopLevelMatchTerms(field, candidate)
		s.fastTopLevelMatchIn(field, candidate)
	}
	if hasNumber {
		s.fastTopLevelMatchRange(field, number)
	}
	return nil
}

func (s *streamScanState) fastTopLevelMarkExists(field *streamFastTopLevelField) {
	for _, idx := range field.existsIdxs {
		clause := &s.engine.existsClauses[idx]
		s.engine.hits[clause.id] = true
	}
}

func (s *streamScanState) fastTopLevelMatchTerms(field *streamFastTopLevelField, candidate string) {
	lowerCandidate := ""
	haveLower := false
	for _, idx := range field.termIdxs {
		clause := &s.engine.termClauses[idx]
		if s.engine.hits[clause.id] {
			continue
		}
		value := candidate
		if clause.ignoreCase {
			if !haveLower {
				lowerCandidate = strings.ToLower(candidate)
				haveLower = true
			}
			value = lowerCandidate
		}
		switch clause.mode {
		case streamTermEq:
			s.engine.hits[clause.id] = value == clause.needle
		case streamTermContains:
			s.engine.hits[clause.id] = strings.Contains(value, clause.needle)
		case streamTermPrefix:
			s.engine.hits[clause.id] = strings.HasPrefix(value, clause.needle)
		}
	}
}

func (s *streamScanState) fastTopLevelMatchIn(field *streamFastTopLevelField, candidate string) {
	for _, idx := range field.inIdxs {
		clause := &s.engine.inClauses[idx]
		if s.engine.hits[clause.id] {
			continue
		}
		_, exists := clause.candidate[candidate]
		s.engine.hits[clause.id] = exists
	}
}

func (s *streamScanState) fastTopLevelMatchRange(field *streamFastTopLevelField, number float64) {
	for _, idx := range field.rangeIdxs {
		clause := &s.engine.rangeClauses[idx]
		if s.engine.hits[clause.id] {
			continue
		}
		term := clause.term
		if term.GTE != nil && number < *term.GTE {
			continue
		}
		if term.GT != nil && number <= *term.GT {
			continue
		}
		if term.LTE != nil && number > *term.LTE {
			continue
		}
		if term.LT != nil && number >= *term.LT {
			continue
		}
		s.engine.hits[clause.id] = true
	}
}

func (s *streamScanState) readBufferedUnescapedStringEquals(reader *streamByteReader, needle string, writeJSON bool) (matched bool, ok bool, err error) {
	buffered := reader.Buffered()
	if buffered <= 0 {
		return false, false, nil
	}
	chunk, err := reader.Peek(buffered)
	if err != nil && err != io.EOF {
		return false, false, err
	}
	end := -1
	for i, ch := range chunk {
		if ch == '"' {
			end = i
			break
		}
		if ch == '\\' || ch < 0x20 {
			return false, false, nil
		}
	}
	if end < 0 {
		return false, false, nil
	}
	candidate := chunk[:end]
	if _, err := reader.Discard(end + 1); err != nil {
		return false, false, err
	}
	if writeJSON {
		if err := s.payloadWriteByte('"'); err != nil {
			return false, false, err
		}
		if _, err := s.payload.Write(candidate); err != nil {
			return false, false, err
		}
		if err := s.payloadWriteByte('"'); err != nil {
			return false, false, err
		}
	}
	if len(candidate) != len(needle) {
		return false, true, nil
	}
	for i := range candidate {
		if candidate[i] != needle[i] {
			return false, true, nil
		}
	}
	return true, true, nil
}

func streamEqStringMatch(candidate string, clause *streamFastTopLevelEqClause) bool {
	if clause.ignoreCase {
		return strings.ToLower(candidate) == clause.needle
	}
	return candidate == clause.needle
}

func (s *streamScanState) copyOrSkipValue(reader *streamByteReader, start byte) error {
	if s.includeJSON {
		return s.copyValue(reader, start)
	}
	return s.skipValue(reader, start)
}

func (s *streamScanState) scanObject(reader *streamByteReader) (streamValueKind, error) {
	next, err := readByteOrSkipSpace(reader)
	if err != nil {
		return streamValueInvalid, err
	}
	if next == '}' {
		if s.includeJSON {
			if err := s.payloadWriteByte('}'); err != nil {
				return streamValueInvalid, err
			}
		}
		return streamValueObject, nil
	}

	first := true
	for {
		if next != '"' {
			return streamValueInvalid, fmt.Errorf("expected string object key")
		}
		key, err := s.readString(reader)
		if err != nil {
			return streamValueInvalid, err
		}
		if s.includeJSON {
			if !first {
				if err := s.payloadWriteByte(','); err != nil {
					return streamValueInvalid, err
				}
			}
			if err := s.payloadWriteJSONString(key); err != nil {
				return streamValueInvalid, err
			}
			if err := s.payloadWriteByte(':'); err != nil {
				return streamValueInvalid, err
			}
		}

		colon, err := reader.ReadByte()
		if err != nil {
			return streamValueInvalid, err
		}
		if colon != ':' && isWhitespace(colon) {
			colon, err = readNonSpaceByte(reader)
			if err != nil {
				return streamValueInvalid, err
			}
		}
		if colon != ':' {
			return streamValueInvalid, fmt.Errorf("expected ':' after object key")
		}

		keyOffset := len(s.keyBytes)
		s.keyBytes = append(s.keyBytes, key...)
		s.path = append(s.path, streamPathSegment{
			kind:     streamPathObjectKey,
			keyStart: keyOffset,
			keyEnd:   len(s.keyBytes),
		})

		valueStart, err := readByteOrSkipSpace(reader)
		if err != nil {
			return streamValueInvalid, err
		}
		if _, err := s.scanValue(reader, valueStart); err != nil {
			return streamValueInvalid, err
		}

		s.path = s.path[:len(s.path)-1]
		s.keyBytes = s.keyBytes[:keyOffset]

		next, err = readByteOrSkipSpace(reader)
		if err != nil {
			return streamValueInvalid, err
		}
		switch next {
		case ',':
			next, err = readByteOrSkipSpace(reader)
			if err != nil {
				return streamValueInvalid, err
			}
			first = false
		case '}':
			if s.includeJSON {
				if err := s.payloadWriteByte('}'); err != nil {
					return streamValueInvalid, err
				}
			}
			return streamValueObject, nil
		default:
			return streamValueInvalid, fmt.Errorf("expected ',' or '}' in object, got %q", next)
		}
	}
}

func (s *streamScanState) scanArray(reader *streamByteReader) (streamValueKind, error) {
	next, err := readByteOrSkipSpace(reader)
	if err != nil {
		return streamValueInvalid, err
	}
	if next == ']' {
		if s.includeJSON {
			if err := s.payloadWriteByte(']'); err != nil {
				return streamValueInvalid, err
			}
		}
		return streamValueArray, nil
	}

	first := true
	index := 0
	for {
		if s.includeJSON && !first {
			if err := s.payloadWriteByte(','); err != nil {
				return streamValueInvalid, err
			}
		}
		s.path = append(s.path, streamPathSegment{
			kind:  streamPathArrayIndex,
			index: index,
		})
		if _, err := s.scanValue(reader, next); err != nil {
			return streamValueInvalid, err
		}
		s.path = s.path[:len(s.path)-1]
		index++
		first = false

		next, err = readByteOrSkipSpace(reader)
		if err != nil {
			return streamValueInvalid, err
		}
		switch next {
		case ',':
			next, err = readByteOrSkipSpace(reader)
			if err != nil {
				return streamValueInvalid, err
			}
		case ']':
			if s.includeJSON {
				if err := s.payloadWriteByte(']'); err != nil {
					return streamValueInvalid, err
				}
			}
			return streamValueArray, nil
		default:
			return streamValueInvalid, fmt.Errorf("expected ',' or ']' in array, got %q", next)
		}
	}
}

func (s *streamScanState) payloadWriteJSONString(value []byte) error {
	s.jsonScratch = appendJSONString(s.jsonScratch[:0], value)
	_, err := s.payload.Write(s.jsonScratch)
	return err
}

func (s *streamScanState) payloadWriteByte(value byte) error {
	if s.payloadBytes != nil {
		return s.payloadBytes.WriteByte(value)
	}
	s.oneByte[0] = value
	_, err := s.payload.Write(s.oneByte[:])
	return err
}

func (s *streamScanState) readString(reader *streamByteReader) ([]byte, error) {
	s.stringBuf = s.stringBuf[:0]
	for {
		if buffered := reader.Buffered(); buffered > 0 {
			chunk, err := reader.Peek(buffered)
			if err != nil && err != io.EOF {
				return nil, err
			}
			prefix := leadingSimpleJSONStringBytes(chunk)
			if prefix > 0 {
				s.stringBuf = append(s.stringBuf, chunk[:prefix]...)
				if _, err := reader.Discard(prefix); err != nil {
					return nil, err
				}
				continue
			}
		}

		ch, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		switch ch {
		case '"':
			return s.stringBuf, nil
		case '\\':
			escaped, err := reader.ReadByte()
			if err != nil {
				return nil, err
			}
			switch escaped {
			case '"', '\\', '/':
				s.stringBuf = append(s.stringBuf, escaped)
			case 'b':
				s.stringBuf = append(s.stringBuf, '\b')
			case 'f':
				s.stringBuf = append(s.stringBuf, '\f')
			case 'n':
				s.stringBuf = append(s.stringBuf, '\n')
			case 'r':
				s.stringBuf = append(s.stringBuf, '\r')
			case 't':
				s.stringBuf = append(s.stringBuf, '\t')
			case 'u':
				r, err := readUnicodeEscape(reader)
				if err != nil {
					return nil, err
				}
				s.stringBuf = utf8.AppendRune(s.stringBuf, r)
			default:
				return nil, fmt.Errorf("invalid string escape \\%c", escaped)
			}
		default:
			if ch < 0x20 {
				return nil, fmt.Errorf("invalid control character in string")
			}
			if ch < utf8.RuneSelf {
				s.stringBuf = append(s.stringBuf, ch)
				continue
			}
			r, size := decodeUTF8RuneFromReader(ch, reader)
			if size > 1 {
				s.stringBuf = utf8.AppendRune(s.stringBuf, r)
				continue
			}
			s.stringBuf = utf8.AppendRune(s.stringBuf, utf8.RuneError)
		}
	}
}

func (s *streamScanState) skipString(reader *streamByteReader) error {
	for {
		if buffered := reader.Buffered(); buffered > 0 {
			chunk, err := reader.Peek(buffered)
			if err != nil && err != io.EOF {
				return err
			}
			prefix := leadingSimpleJSONStringBytes(chunk)
			if prefix > 0 {
				if _, err := reader.Discard(prefix); err != nil {
					return err
				}
				continue
			}
		}

		ch, err := reader.ReadByte()
		if err != nil {
			return err
		}
		switch ch {
		case '"':
			return nil
		case '\\':
			escaped, err := reader.ReadByte()
			if err != nil {
				return err
			}
			switch escaped {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			case 'u':
				if _, err := readUnicodeEscape(reader); err != nil {
					return err
				}
			default:
				return fmt.Errorf("invalid string escape \\%c", escaped)
			}
		default:
			if ch < 0x20 {
				return fmt.Errorf("invalid control character in string")
			}
			if ch < utf8.RuneSelf {
				continue
			}
			_, _ = decodeUTF8RuneFromReader(ch, reader)
		}
	}
}

func (s *streamScanState) copyValue(reader *streamByteReader, start byte) error {
	switch start {
	case '{':
		if err := s.payloadWriteByte('{'); err != nil {
			return err
		}
		return s.copyObject(reader)
	case '[':
		if err := s.payloadWriteByte('['); err != nil {
			return err
		}
		return s.copyArray(reader)
	case '"':
		return s.copyRawJSONString(reader)
	case 't':
		if err := expectLiteral(reader, "rue"); err != nil {
			return err
		}
		_, err := s.payload.Write(jsonTrueLiteral)
		return err
	case 'f':
		if err := expectLiteral(reader, "alse"); err != nil {
			return err
		}
		_, err := s.payload.Write(jsonFalseLiteral)
		return err
	case 'n':
		if err := expectLiteral(reader, "ull"); err != nil {
			return err
		}
		_, err := s.payload.Write(jsonNullLiteral)
		return err
	default:
		if !isNumberStart(start) {
			return fmt.Errorf("unexpected value start %q", start)
		}
		number, err := s.readNumber(reader, start)
		if err != nil {
			return err
		}
		_, err = s.payload.Write(number)
		return err
	}
}

func (s *streamScanState) copyObject(reader *streamByteReader) error {
	next, err := readNonSpaceByte(reader)
	if err != nil {
		return err
	}
	if next == '}' {
		return s.payloadWriteByte('}')
	}
	first := true
	for {
		if next != '"' {
			return fmt.Errorf("expected string object key")
		}
		if !first {
			if err := s.payloadWriteByte(','); err != nil {
				return err
			}
		}
		if err := s.copyRawJSONString(reader); err != nil {
			return err
		}
		if err := s.payloadWriteByte(':'); err != nil {
			return err
		}

		colon, err := readNonSpaceByte(reader)
		if err != nil {
			return err
		}
		if colon != ':' {
			return fmt.Errorf("expected ':' after object key")
		}
		valueStart, err := readNonSpaceByte(reader)
		if err != nil {
			return err
		}
		if err := s.copyValue(reader, valueStart); err != nil {
			return err
		}

		next, err = readNonSpaceByte(reader)
		if err != nil {
			return err
		}
		switch next {
		case ',':
			next, err = readNonSpaceByte(reader)
			if err != nil {
				return err
			}
			first = false
		case '}':
			return s.payloadWriteByte('}')
		default:
			return fmt.Errorf("expected ',' or '}' in object, got %q", next)
		}
	}
}

func (s *streamScanState) copyArray(reader *streamByteReader) error {
	next, err := readNonSpaceByte(reader)
	if err != nil {
		return err
	}
	if next == ']' {
		return s.payloadWriteByte(']')
	}
	first := true
	for {
		if !first {
			if err := s.payloadWriteByte(','); err != nil {
				return err
			}
		}
		if err := s.copyValue(reader, next); err != nil {
			return err
		}
		first = false
		next, err = readNonSpaceByte(reader)
		if err != nil {
			return err
		}
		switch next {
		case ',':
			next, err = readNonSpaceByte(reader)
			if err != nil {
				return err
			}
		case ']':
			return s.payloadWriteByte(']')
		default:
			return fmt.Errorf("expected ',' or ']' in array, got %q", next)
		}
	}
}

func (s *streamScanState) skipValue(reader *streamByteReader, start byte) error {
	switch start {
	case '{':
		return s.skipObject(reader)
	case '[':
		return s.skipArray(reader)
	case '"':
		return s.skipString(reader)
	case 't':
		return expectLiteral(reader, "rue")
	case 'f':
		return expectLiteral(reader, "alse")
	case 'n':
		return expectLiteral(reader, "ull")
	default:
		if !isNumberStart(start) {
			return fmt.Errorf("unexpected value start %q", start)
		}
		_, err := s.readNumber(reader, start)
		return err
	}
}

func (s *streamScanState) skipObject(reader *streamByteReader) error {
	next, err := readNonSpaceByte(reader)
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
		if err := s.skipString(reader); err != nil {
			return err
		}
		colon, err := readNonSpaceByte(reader)
		if err != nil {
			return err
		}
		if colon != ':' {
			return fmt.Errorf("expected ':' after object key")
		}
		valueStart, err := readNonSpaceByte(reader)
		if err != nil {
			return err
		}
		if err := s.skipValue(reader, valueStart); err != nil {
			return err
		}
		next, err = readNonSpaceByte(reader)
		if err != nil {
			return err
		}
		switch next {
		case ',':
			next, err = readNonSpaceByte(reader)
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

func (s *streamScanState) skipArray(reader *streamByteReader) error {
	next, err := readNonSpaceByte(reader)
	if err != nil {
		return err
	}
	if next == ']' {
		return nil
	}
	for {
		if err := s.skipValue(reader, next); err != nil {
			return err
		}
		next, err = readNonSpaceByte(reader)
		if err != nil {
			return err
		}
		switch next {
		case ',':
			next, err = readNonSpaceByte(reader)
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

func leadingSimpleJSONStringBytes(buf []byte) int {
	for i, ch := range buf {
		if ch == '"' || ch == '\\' || ch < 0x20 || ch >= utf8.RuneSelf {
			return i
		}
	}
	return len(buf)
}

func (s *streamScanState) copyRawJSONString(reader *streamByteReader) error {
	if err := s.payloadWriteByte('"'); err != nil {
		return err
	}
	for {
		if buffered := reader.Buffered(); buffered > 0 {
			chunk, err := reader.Peek(buffered)
			if err != nil && err != io.EOF {
				return err
			}
			prefix := leadingRawJSONStringBytes(chunk)
			if prefix > 0 {
				if _, err := s.payload.Write(chunk[:prefix]); err != nil {
					return err
				}
				if _, err := reader.Discard(prefix); err != nil {
					return err
				}
				continue
			}
		}

		ch, err := reader.ReadByte()
		if err != nil {
			return err
		}
		switch ch {
		case '"':
			return s.payloadWriteByte('"')
		case '\\':
			if err := s.payloadWriteByte('\\'); err != nil {
				return err
			}
			escaped, err := reader.ReadByte()
			if err != nil {
				return err
			}
			if err := s.payloadWriteByte(escaped); err != nil {
				return err
			}
			switch escaped {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			case 'u':
				for i := 0; i < 4; i++ {
					h, err := reader.ReadByte()
					if err != nil {
						return err
					}
					if !isHexDigitByte(h) {
						return fmt.Errorf("invalid hex digit %q in unicode escape", h)
					}
					if err := s.payloadWriteByte(h); err != nil {
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
			if err := s.payloadWriteByte(ch); err != nil {
				return err
			}
		}
	}
}

func leadingRawJSONStringBytes(buf []byte) int {
	for i, ch := range buf {
		if ch == '"' || ch == '\\' || ch < 0x20 {
			return i
		}
	}
	return len(buf)
}

func isHexDigitByte(ch byte) bool {
	return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}

func (s *streamScanState) readNumber(reader *streamByteReader, first byte) ([]byte, error) {
	s.numberBuf = s.numberBuf[:0]
	i := 0
	s.numberBuf = append(s.numberBuf, first)
	if first == '-' {
		ch, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if !isDigit(ch) {
			return nil, fmt.Errorf("invalid JSON number %q", s.numberBuf)
		}
		s.numberBuf = append(s.numberBuf, ch)
		i = 1
	}

	digit := s.numberBuf[i]
	if digit == '0' {
		ch, err := reader.ReadByte()
		if err == nil {
			if isDigit(ch) {
				if err := reader.UnreadByte(); err != nil {
					return nil, err
				}
				return s.numberBuf, nil
			}
			if ch == '.' {
				s.numberBuf = append(s.numberBuf, ch)
				if err := s.readFractionDigits(reader); err != nil {
					return nil, err
				}
			} else if ch == 'e' || ch == 'E' {
				s.numberBuf = append(s.numberBuf, ch)
				if err := s.readExponentDigits(reader); err != nil {
					return nil, err
				}
			} else {
				if err := reader.UnreadByte(); err != nil {
					return nil, err
				}
			}
		} else if err != io.EOF {
			return nil, err
		}
		return s.numberBuf, nil
	}

	for {
		ch, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return s.numberBuf, nil
			}
			return nil, err
		}
		if isDigit(ch) {
			s.numberBuf = append(s.numberBuf, ch)
			continue
		}
		if ch == '.' {
			s.numberBuf = append(s.numberBuf, ch)
			if err := s.readFractionDigits(reader); err != nil {
				return nil, err
			}
			return s.numberBuf, nil
		}
		if ch == 'e' || ch == 'E' {
			s.numberBuf = append(s.numberBuf, ch)
			if err := s.readExponentDigits(reader); err != nil {
				return nil, err
			}
			return s.numberBuf, nil
		}
		if err := reader.UnreadByte(); err != nil {
			return nil, err
		}
		return s.numberBuf, nil
	}
}

func (s *streamScanState) readFractionDigits(reader *streamByteReader) error {
	ch, err := reader.ReadByte()
	if err != nil {
		return err
	}
	if !isDigit(ch) {
		return fmt.Errorf("invalid JSON number %q", s.numberBuf)
	}
	s.numberBuf = append(s.numberBuf, ch)
	for {
		ch, err = reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if isDigit(ch) {
			s.numberBuf = append(s.numberBuf, ch)
			continue
		}
		if ch == 'e' || ch == 'E' {
			s.numberBuf = append(s.numberBuf, ch)
			return s.readExponentDigits(reader)
		}
		return reader.UnreadByte()
	}
}

func (s *streamScanState) readExponentDigits(reader *streamByteReader) error {
	ch, err := reader.ReadByte()
	if err != nil {
		return err
	}
	if ch == '+' || ch == '-' {
		s.numberBuf = append(s.numberBuf, ch)
		ch, err = reader.ReadByte()
		if err != nil {
			return err
		}
	}
	if !isDigit(ch) {
		return fmt.Errorf("invalid JSON number %q", s.numberBuf)
	}
	s.numberBuf = append(s.numberBuf, ch)
	for {
		ch, err = reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if isDigit(ch) {
			s.numberBuf = append(s.numberBuf, ch)
			continue
		}
		return reader.UnreadByte()
	}
}

func readUnicodeEscape(reader *streamByteReader) (rune, error) {
	hi, err := readHex4(reader)
	if err != nil {
		return 0, err
	}
	r := rune(hi)
	if !utf16.IsSurrogate(r) {
		return r, nil
	}
	if r < 0xD800 || r > 0xDBFF {
		return utf8.RuneError, nil
	}

	lookahead, err := reader.Peek(6)
	if err != nil && err != io.EOF {
		return 0, err
	}
	if len(lookahead) >= 6 && lookahead[0] == '\\' && lookahead[1] == 'u' {
		lo, ok := parseHex4Bytes(lookahead[2:6])
		if ok {
			loRune := rune(lo)
			if loRune >= 0xDC00 && loRune <= 0xDFFF {
				if _, err := reader.Discard(6); err != nil {
					return 0, err
				}
				return utf16.DecodeRune(r, loRune), nil
			}
		}
	}
	return utf8.RuneError, nil
}

func readHex4(reader *streamByteReader) (uint16, error) {
	var value uint16
	for i := 0; i < 4; i++ {
		ch, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		value <<= 4
		switch {
		case ch >= '0' && ch <= '9':
			value |= uint16(ch - '0')
		case ch >= 'a' && ch <= 'f':
			value |= uint16(ch-'a') + 10
		case ch >= 'A' && ch <= 'F':
			value |= uint16(ch-'A') + 10
		default:
			return 0, fmt.Errorf("invalid hex digit %q in unicode escape", ch)
		}
	}
	return value, nil
}

func parseHex4Bytes(buf []byte) (uint16, bool) {
	if len(buf) < 4 {
		return 0, false
	}
	var value uint16
	for i := 0; i < 4; i++ {
		ch := buf[i]
		value <<= 4
		switch {
		case ch >= '0' && ch <= '9':
			value |= uint16(ch - '0')
		case ch >= 'a' && ch <= 'f':
			value |= uint16(ch-'a') + 10
		case ch >= 'A' && ch <= 'F':
			value |= uint16(ch-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

func decodeUTF8RuneFromReader(first byte, reader *streamByteReader) (rune, int) {
	peek, err := reader.Peek(utf8.UTFMax - 1)
	if err != nil && err != io.EOF {
		return utf8.RuneError, 1
	}
	var buf [utf8.UTFMax]byte
	buf[0] = first
	n := 1
	copy(buf[1:], peek)
	n += len(peek)
	r, size := utf8.DecodeRune(buf[:n])
	if r == utf8.RuneError && size == 1 {
		return utf8.RuneError, 1
	}
	if size > 1 {
		if _, err := reader.Discard(size - 1); err != nil {
			return utf8.RuneError, 1
		}
	}
	return r, size
}

func expectLiteral(reader *streamByteReader, suffix string) error {
	for i := 0; i < len(suffix); i++ {
		ch, err := reader.ReadByte()
		if err != nil {
			return err
		}
		if ch != suffix[i] {
			return fmt.Errorf("invalid literal")
		}
	}
	return nil
}

func readNonSpaceByte(reader *streamByteReader) (byte, error) {
	for {
		ch, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		if !isWhitespace(ch) {
			return ch, nil
		}
	}
}

func readByteOrSkipSpace(reader *streamByteReader) (byte, error) {
	ch, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}
	if !isWhitespace(ch) {
		return ch, nil
	}
	return readNonSpaceByte(reader)
}

func isWhitespace(ch byte) bool {
	switch ch {
	case ' ', '\n', '\r', '\t':
		return true
	default:
		return false
	}
}

func isNumberStart(ch byte) bool {
	return ch == '-' || (ch >= '0' && ch <= '9')
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

type streamValueKind int

const (
	streamValueInvalid streamValueKind = iota
	streamValueObject
	streamValueArray
	streamValueString
	streamValueNumber
	streamValueBool
	streamValueNull
)

type streamPathSegmentKind int

const (
	streamPathObjectKey streamPathSegmentKind = iota + 1
	streamPathArrayIndex
)

type streamPathSegment struct {
	kind     streamPathSegmentKind
	keyStart int
	keyEnd   int
	index    int
}

func appendJSONString(dst []byte, value []byte) []byte {
	if isSimpleJSONStringBytes(value) {
		dst = append(dst, '"')
		dst = append(dst, value...)
		dst = append(dst, '"')
		return dst
	}
	dst = append(dst, '"')
	for len(value) > 0 {
		r, size := utf8.DecodeRune(value)
		if r == utf8.RuneError && size == 1 {
			dst = utf8.AppendRune(dst, utf8.RuneError)
			value = value[1:]
			continue
		}
		value = value[size:]
		switch r {
		case '"', '\\':
			dst = append(dst, '\\', byte(r))
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			if r < 0x20 || r == '\u2028' || r == '\u2029' {
				dst = appendUnicodeEscape(dst, r)
			} else {
				dst = utf8.AppendRune(dst, r)
			}
		}
	}
	dst = append(dst, '"')
	return dst
}

func appendJSONStringString(dst []byte, value string) []byte {
	if isSimpleJSONStringString(value) {
		dst = append(dst, '"')
		dst = append(dst, value...)
		dst = append(dst, '"')
		return dst
	}
	dst = append(dst, '"')
	for len(value) > 0 {
		r, size := utf8.DecodeRuneInString(value)
		if r == utf8.RuneError && size == 1 {
			dst = utf8.AppendRune(dst, utf8.RuneError)
			value = value[1:]
			continue
		}
		value = value[size:]
		switch r {
		case '"', '\\':
			dst = append(dst, '\\', byte(r))
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			if r < 0x20 || r == '\u2028' || r == '\u2029' {
				dst = appendUnicodeEscape(dst, r)
			} else {
				dst = utf8.AppendRune(dst, r)
			}
		}
	}
	dst = append(dst, '"')
	return dst
}

func appendUnicodeEscape(dst []byte, r rune) []byte {
	dst = append(dst, '\\', 'u')
	var buf [4]byte
	n := uint16(r)
	buf[0] = nibbleToHex(byte(n >> 12))
	buf[1] = nibbleToHex(byte((n >> 8) & 0x0f))
	buf[2] = nibbleToHex(byte((n >> 4) & 0x0f))
	buf[3] = nibbleToHex(byte(n & 0x0f))
	return append(dst, buf[:]...)
}

func isSimpleJSONStringBytes(value []byte) bool {
	for _, ch := range value {
		if ch == '"' || ch == '\\' || ch < 0x20 || ch >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

func isSimpleJSONStringString(value string) bool {
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch == '"' || ch == '\\' || ch < 0x20 || ch >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

func nibbleToHex(n byte) byte {
	if n < 10 {
		return '0' + n
	}
	return 'a' + (n - 10)
}

func bytesToStringUnsafe(buf []byte) string {
	if len(buf) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(buf), len(buf))
}

type streamSelectorEngine struct {
	selector      Selector
	emptySelector bool

	hits []bool

	termClauses   []streamTermClause
	rangeClauses  []streamRangeClause
	inClauses     []streamInClause
	existsClauses []streamExistsClause

	termDispatch   streamClauseDispatchIndex
	rangeDispatch  streamClauseDispatchIndex
	inDispatch     streamClauseDispatchIndex
	existsDispatch streamClauseDispatchIndex

	termIDs   map[*Term]int
	rangeIDs  map[*RangeTerm]int
	inIDs     map[*InTerm]int
	existsIDs map[string]int

	hasTermClauses   bool
	hasRangeClauses  bool
	hasInClauses     bool
	hasExistsClauses bool
	useDispatch      bool

	fastEq          *streamFastEqClause
	fastRecursiveEq *streamFastRecursiveEqClause

	fastTopLevelEq *streamFastTopLevelEqClause
	fastTopLevel   *streamFastTopLevelProgram
}

type streamFastEqClause struct {
	id         int
	path       []streamPathToken
	needle     string
	ignoreCase bool
}

type streamFastRecursiveEqClause struct {
	id         int
	key        string
	keyBytes   []byte
	needle     string
	ignoreCase bool
}

type streamFastTopLevelEqClause struct {
	id         int
	key        string
	keyBytes   []byte
	needle     string
	ignoreCase bool
}

type streamFastTopLevelProgram struct {
	fields map[string]int
	items  []streamFastTopLevelField
}

type streamFastTopLevelField struct {
	termIdxs   []int
	rangeIdxs  []int
	inIdxs     []int
	existsIdxs []int
}

type streamTermMode int

const (
	streamTermEq streamTermMode = iota
	streamTermContains
	streamTermPrefix
)

type streamPathTokenMode int

const (
	streamPathTokenLiteral streamPathTokenMode = iota
	streamPathTokenObjectWildcard
	streamPathTokenArrayWildcard
	streamPathTokenAnyChild
	streamPathTokenRecursive
)

type streamPathToken struct {
	mode      streamPathTokenMode
	raw       string
	rawBytes  []byte
	arrayOnly bool
	index     int
}

type streamTermClause struct {
	id              int
	path            []streamPathToken
	hasRecursive    bool
	singleRecursive bool
	recursiveIdx    int
	tailKind        streamTailFilterKind
	tailKey         string
	tailKeyBytes    []byte
	tailIndex       int
	mode            streamTermMode
	needle          string
	ignoreCase      bool
}

type streamRangeClause struct {
	id              int
	path            []streamPathToken
	hasRecursive    bool
	singleRecursive bool
	recursiveIdx    int
	tailKind        streamTailFilterKind
	tailKey         string
	tailKeyBytes    []byte
	tailIndex       int
	term            *RangeTerm
}

type streamInClause struct {
	id              int
	path            []streamPathToken
	hasRecursive    bool
	singleRecursive bool
	recursiveIdx    int
	tailKind        streamTailFilterKind
	tailKey         string
	tailKeyBytes    []byte
	tailIndex       int
	candidate       map[string]struct{}
}

type streamExistsClause struct {
	id              int
	path            []streamPathToken
	hasRecursive    bool
	singleRecursive bool
	recursiveIdx    int
	tailKind        streamTailFilterKind
	tailKey         string
	tailKeyBytes    []byte
	tailIndex       int
}

type streamTailFilterKind uint8

const (
	streamTailFilterNone streamTailFilterKind = iota
	streamTailFilterObjectKey
	streamTailFilterArrayIndex
)

type streamClauseDispatchIndex struct {
	any    []int
	object map[string][]int
	array  map[int][]int
}

func (d *streamClauseDispatchIndex) add(kind streamTailFilterKind, key string, index int, clauseIdx int) {
	switch kind {
	case streamTailFilterObjectKey:
		if d.object == nil {
			d.object = make(map[string][]int)
		}
		d.object[key] = append(d.object[key], clauseIdx)
	case streamTailFilterArrayIndex:
		if d.array == nil {
			d.array = make(map[int][]int)
		}
		d.array[index] = append(d.array[index], clauseIdx)
	default:
		d.any = append(d.any, clauseIdx)
	}
}

func (d *streamClauseDispatchIndex) candidates(path []streamPathSegment, keyBytes []byte) (specific []int, any []int) {
	if len(path) == 0 {
		return nil, d.any
	}
	tail := path[len(path)-1]
	switch tail.kind {
	case streamPathObjectKey:
		if d.object != nil {
			key := bytesToStringUnsafe(keyBytes[tail.keyStart:tail.keyEnd])
			specific = d.object[key]
		}
	case streamPathArrayIndex:
		if d.array != nil {
			specific = d.array[tail.index]
		}
	}
	return specific, d.any
}

func (p *streamFastTopLevelProgram) fieldForKey(key []byte) *streamFastTopLevelField {
	if p == nil || len(p.fields) == 0 {
		return nil
	}
	idx, ok := p.fields[bytesToStringUnsafe(key)]
	if !ok {
		return nil
	}
	return &p.items[idx]
}

func (p *streamFastTopLevelProgram) ensureField(key string) *streamFastTopLevelField {
	if p.fields == nil {
		p.fields = make(map[string]int)
	}
	if idx, ok := p.fields[key]; ok {
		return &p.items[idx]
	}
	idx := len(p.items)
	p.fields[key] = idx
	p.items = append(p.items, streamFastTopLevelField{})
	return &p.items[idx]
}

func newStreamSelectorEngine(selector Selector) (*streamSelectorEngine, error) {
	engine := &streamSelectorEngine{
		selector:      selector,
		emptySelector: selector.IsEmpty(),
		termIDs:       make(map[*Term]int),
		rangeIDs:      make(map[*RangeTerm]int),
		inIDs:         make(map[*InTerm]int),
		existsIDs:     make(map[string]int),
	}
	if engine.emptySelector {
		return engine, nil
	}
	if err := engine.collect(selector); err != nil {
		return nil, err
	}
	engine.useDispatch = engine.shouldUseDispatch()
	engine.buildFastEqPath()
	engine.buildFastTopLevelProgram()
	return engine, nil
}

func (e *streamSelectorEngine) shouldUseDispatch() bool {
	total := len(e.termClauses) + len(e.rangeClauses) + len(e.inClauses) + len(e.existsClauses)
	return total > 4
}

func (e *streamSelectorEngine) buildFastEqPath() {
	term, ok := selectorSimpleSingleEq(e.selector)
	if !ok {
		e.fastEq = nil
		e.fastRecursiveEq = nil
		e.fastTopLevelEq = nil
		return
	}
	id, ok := e.termIDs[term]
	if !ok {
		e.fastEq = nil
		e.fastRecursiveEq = nil
		e.fastTopLevelEq = nil
		return
	}
	var path []streamPathToken
	for i := range e.termClauses {
		if e.termClauses[i].id == id {
			path = e.termClauses[i].path
			break
		}
	}
	if len(path) == 0 {
		e.fastEq = nil
		e.fastRecursiveEq = nil
		e.fastTopLevelEq = nil
		return
	}
	e.fastRecursiveEq = nil
	if len(path) == 2 && path[0].mode == streamPathTokenRecursive && path[1].mode == streamPathTokenLiteral && !path[1].arrayOnly {
		e.fastRecursiveEq = &streamFastRecursiveEqClause{
			id:         id,
			key:        path[1].raw,
			keyBytes:   path[1].rawBytes,
			needle:     termValueNeedle(term),
			ignoreCase: term.IgnoreCase,
		}
	}
	for _, token := range path {
		if token.mode != streamPathTokenLiteral {
			e.fastEq = nil
			e.fastTopLevelEq = nil
			return
		}
	}
	e.fastEq = &streamFastEqClause{
		id:         id,
		path:       path,
		needle:     termValueNeedle(term),
		ignoreCase: term.IgnoreCase,
	}
	if len(path) == 1 && !path[0].arrayOnly {
		e.fastTopLevelEq = &streamFastTopLevelEqClause{
			id:         id,
			key:        path[0].raw,
			keyBytes:   path[0].rawBytes,
			needle:     termValueNeedle(term),
			ignoreCase: term.IgnoreCase,
		}
		return
	}
	e.fastTopLevelEq = nil
}

func (e *streamSelectorEngine) buildFastTopLevelProgram() {
	total := len(e.termClauses) + len(e.rangeClauses) + len(e.inClauses) + len(e.existsClauses)
	if total == 0 {
		e.fastTopLevel = nil
		return
	}
	program := &streamFastTopLevelProgram{
		fields: make(map[string]int, total),
		items:  make([]streamFastTopLevelField, 0, total),
	}

	for i := range e.termClauses {
		key, ok := streamTopLevelLiteralPath(e.termClauses[i].path)
		if !ok {
			e.fastTopLevel = nil
			return
		}
		field := program.ensureField(key)
		field.termIdxs = append(field.termIdxs, i)
	}
	for i := range e.rangeClauses {
		key, ok := streamTopLevelLiteralPath(e.rangeClauses[i].path)
		if !ok {
			e.fastTopLevel = nil
			return
		}
		field := program.ensureField(key)
		field.rangeIdxs = append(field.rangeIdxs, i)
	}
	for i := range e.inClauses {
		key, ok := streamTopLevelLiteralPath(e.inClauses[i].path)
		if !ok {
			e.fastTopLevel = nil
			return
		}
		field := program.ensureField(key)
		field.inIdxs = append(field.inIdxs, i)
	}
	for i := range e.existsClauses {
		key, ok := streamTopLevelLiteralPath(e.existsClauses[i].path)
		if !ok {
			e.fastTopLevel = nil
			return
		}
		field := program.ensureField(key)
		field.existsIdxs = append(field.existsIdxs, i)
	}

	e.fastTopLevel = program
}

func streamTopLevelLiteralPath(path []streamPathToken) (string, bool) {
	if len(path) != 1 {
		return "", false
	}
	token := path[0]
	if token.mode != streamPathTokenLiteral || token.arrayOnly {
		return "", false
	}
	return token.raw, true
}

func selectorSimpleSingleEq(selector Selector) (*Term, bool) {
	if selector.Eq == nil {
		return nil, false
	}
	if selector.Contains != nil || selector.IContains != nil || selector.Prefix != nil || selector.IPrefix != nil {
		return nil, false
	}
	if selector.Range != nil || selector.In != nil || selector.Exists != "" {
		return nil, false
	}
	if selector.Not != nil || len(selector.And) > 0 || len(selector.Or) > 0 {
		return nil, false
	}
	return selector.Eq, true
}

func termValueNeedle(term *Term) string {
	if term == nil {
		return ""
	}
	if term.IgnoreCase {
		return strings.ToLower(term.Value)
	}
	return term.Value
}

func (e *streamSelectorEngine) collect(selector Selector) error {
	if selector.Eq != nil {
		if err := e.addTermClause(selector.Eq, streamTermEq, false); err != nil {
			return err
		}
	}
	if selector.Contains != nil {
		if err := e.addTermClause(selector.Contains, streamTermContains, false); err != nil {
			return err
		}
	}
	if selector.IContains != nil {
		if err := e.addTermClause(selector.IContains, streamTermContains, true); err != nil {
			return err
		}
	}
	if selector.Prefix != nil {
		if err := e.addTermClause(selector.Prefix, streamTermPrefix, false); err != nil {
			return err
		}
	}
	if selector.IPrefix != nil {
		if err := e.addTermClause(selector.IPrefix, streamTermPrefix, true); err != nil {
			return err
		}
	}
	if selector.Range != nil {
		if err := e.addRangeClause(selector.Range); err != nil {
			return err
		}
	}
	if selector.In != nil {
		if err := e.addInClause(selector.In); err != nil {
			return err
		}
	}
	if selector.Exists != "" {
		if err := e.addExistsClause(selector.Exists); err != nil {
			return err
		}
	}
	if selector.Not != nil {
		if err := e.collect(*selector.Not); err != nil {
			return err
		}
	}
	for _, child := range selector.And {
		if err := e.collect(child); err != nil {
			return err
		}
	}
	for _, child := range selector.Or {
		if err := e.collect(child); err != nil {
			return err
		}
	}
	return nil
}

func (e *streamSelectorEngine) addTermClause(term *Term, mode streamTermMode, forceIgnoreCase bool) error {
	if _, exists := e.termIDs[term]; exists {
		return nil
	}
	tokens, err := compileStreamPath(term.Field)
	if err != nil {
		return err
	}
	id := len(e.hits)
	e.hits = append(e.hits, false)
	e.termIDs[term] = id
	hasRecursive, singleRecursive, recursiveIdx := streamPatternRecursiveInfo(tokens)
	tailKind, tailKey, tailIndex := streamPatternTailFilter(tokens)
	needle := term.Value
	ignoreCase := forceIgnoreCase || term.IgnoreCase
	if ignoreCase {
		needle = strings.ToLower(needle)
	}
	e.termClauses = append(e.termClauses, streamTermClause{
		id:              id,
		path:            tokens,
		hasRecursive:    hasRecursive,
		singleRecursive: singleRecursive,
		recursiveIdx:    recursiveIdx,
		tailKind:        tailKind,
		tailKey:         tailKey,
		tailKeyBytes:    []byte(tailKey),
		tailIndex:       tailIndex,
		mode:            mode,
		needle:          needle,
		ignoreCase:      ignoreCase,
	})
	e.termDispatch.add(tailKind, tailKey, tailIndex, len(e.termClauses)-1)
	e.hasTermClauses = true
	return nil
}

func (e *streamSelectorEngine) addRangeClause(term *RangeTerm) error {
	if _, exists := e.rangeIDs[term]; exists {
		return nil
	}
	tokens, err := compileStreamPath(term.Field)
	if err != nil {
		return err
	}
	id := len(e.hits)
	e.hits = append(e.hits, false)
	e.rangeIDs[term] = id
	hasRecursive, singleRecursive, recursiveIdx := streamPatternRecursiveInfo(tokens)
	tailKind, tailKey, tailIndex := streamPatternTailFilter(tokens)
	e.rangeClauses = append(e.rangeClauses, streamRangeClause{
		id:              id,
		path:            tokens,
		hasRecursive:    hasRecursive,
		singleRecursive: singleRecursive,
		recursiveIdx:    recursiveIdx,
		tailKind:        tailKind,
		tailKey:         tailKey,
		tailKeyBytes:    []byte(tailKey),
		tailIndex:       tailIndex,
		term:            term,
	})
	e.rangeDispatch.add(tailKind, tailKey, tailIndex, len(e.rangeClauses)-1)
	e.hasRangeClauses = true
	return nil
}

func (e *streamSelectorEngine) addInClause(term *InTerm) error {
	if _, exists := e.inIDs[term]; exists {
		return nil
	}
	tokens, err := compileStreamPath(term.Field)
	if err != nil {
		return err
	}
	id := len(e.hits)
	e.hits = append(e.hits, false)
	e.inIDs[term] = id
	hasRecursive, singleRecursive, recursiveIdx := streamPatternRecursiveInfo(tokens)
	tailKind, tailKey, tailIndex := streamPatternTailFilter(tokens)
	candidates := make(map[string]struct{}, len(term.Any))
	for _, value := range term.Any {
		candidates[value] = struct{}{}
	}
	e.inClauses = append(e.inClauses, streamInClause{
		id:              id,
		path:            tokens,
		hasRecursive:    hasRecursive,
		singleRecursive: singleRecursive,
		recursiveIdx:    recursiveIdx,
		tailKind:        tailKind,
		tailKey:         tailKey,
		tailKeyBytes:    []byte(tailKey),
		tailIndex:       tailIndex,
		candidate:       candidates,
	})
	e.inDispatch.add(tailKind, tailKey, tailIndex, len(e.inClauses)-1)
	e.hasInClauses = true
	return nil
}

func (e *streamSelectorEngine) addExistsClause(field string) error {
	if _, exists := e.existsIDs[field]; exists {
		return nil
	}
	tokens, err := compileStreamPath(field)
	if err != nil {
		return err
	}
	id := len(e.hits)
	e.hits = append(e.hits, false)
	e.existsIDs[field] = id
	hasRecursive, singleRecursive, recursiveIdx := streamPatternRecursiveInfo(tokens)
	tailKind, tailKey, tailIndex := streamPatternTailFilter(tokens)
	e.existsClauses = append(e.existsClauses, streamExistsClause{
		id:              id,
		path:            tokens,
		hasRecursive:    hasRecursive,
		singleRecursive: singleRecursive,
		recursiveIdx:    recursiveIdx,
		tailKind:        tailKind,
		tailKey:         tailKey,
		tailKeyBytes:    []byte(tailKey),
		tailIndex:       tailIndex,
	})
	e.existsDispatch.add(tailKind, tailKey, tailIndex, len(e.existsClauses)-1)
	e.hasExistsClauses = true
	return nil
}

func (e *streamSelectorEngine) reset() {
	for i := range e.hits {
		e.hits[i] = false
	}
}

func (e *streamSelectorEngine) observe(path []streamPathSegment, keyBytes []byte, kind streamValueKind, value any) {
	if e.emptySelector {
		return
	}
	if e.fastEq != nil {
		clause := e.fastEq
		if e.hits[clause.id] {
			return
		}
		if !streamLiteralPathMatches(clause.path, path, keyBytes) {
			return
		}
		candidate, ok := valueToString(value)
		if !ok {
			return
		}
		if clause.ignoreCase {
			candidate = strings.ToLower(candidate)
		}
		e.hits[clause.id] = candidate == clause.needle
		return
	}
	if e.fastRecursiveEq != nil {
		clause := e.fastRecursiveEq
		if e.hits[clause.id] || !streamPathTailMatches(streamTailFilterObjectKey, clause.keyBytes, 0, path, keyBytes) {
			return
		}
		candidate, ok := valueToString(value)
		if !ok {
			return
		}
		if clause.ignoreCase {
			candidate = strings.ToLower(candidate)
		}
		e.hits[clause.id] = candidate == clause.needle
		return
	}
	switch kind {
	case streamValueObject, streamValueArray:
		if !e.hasExistsClauses {
			return
		}
	case streamValueNull:
		// Null never satisfies eq/in/range clauses and exists only matches non-null.
		return
	case streamValueBool:
		if !e.hasTermClauses && !e.hasInClauses && !e.hasExistsClauses {
			return
		}
	case streamValueNumber:
		if !e.hasTermClauses && !e.hasInClauses && !e.hasRangeClauses && !e.hasExistsClauses {
			return
		}
	case streamValueString:
		if !e.hasTermClauses && !e.hasInClauses && !e.hasRangeClauses && !e.hasExistsClauses {
			return
		}
	}

	var (
		stringCached      string
		stringCachedOK    bool
		stringCacheReady  bool
		lowerStringCached string
		lowerCacheReady   bool
		floatCached       float64
		floatCachedOK     bool
		floatCacheReady   bool
	)

	if e.useDispatch {
		termSpecific, termAny := e.termDispatch.candidates(path, keyBytes)
		for _, clauseIdx := range termSpecific {
			clause := &e.termClauses[clauseIdx]
			if e.hits[clause.id] || !streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
				continue
			}
			if !stringCacheReady {
				stringCached, stringCachedOK = valueToString(value)
				stringCacheReady = true
			}
			if !stringCachedOK {
				continue
			}
			candidate := stringCached
			if clause.ignoreCase {
				if !lowerCacheReady {
					lowerStringCached = strings.ToLower(stringCached)
					lowerCacheReady = true
				}
				candidate = lowerStringCached
			}
			switch clause.mode {
			case streamTermEq:
				e.hits[clause.id] = candidate == clause.needle
			case streamTermContains:
				e.hits[clause.id] = strings.Contains(candidate, clause.needle)
			case streamTermPrefix:
				e.hits[clause.id] = strings.HasPrefix(candidate, clause.needle)
			}
		}
		for _, clauseIdx := range termAny {
			clause := &e.termClauses[clauseIdx]
			if e.hits[clause.id] || !streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
				continue
			}
			if !stringCacheReady {
				stringCached, stringCachedOK = valueToString(value)
				stringCacheReady = true
			}
			if !stringCachedOK {
				continue
			}
			candidate := stringCached
			if clause.ignoreCase {
				if !lowerCacheReady {
					lowerStringCached = strings.ToLower(stringCached)
					lowerCacheReady = true
				}
				candidate = lowerStringCached
			}
			switch clause.mode {
			case streamTermEq:
				e.hits[clause.id] = candidate == clause.needle
			case streamTermContains:
				e.hits[clause.id] = strings.Contains(candidate, clause.needle)
			case streamTermPrefix:
				e.hits[clause.id] = strings.HasPrefix(candidate, clause.needle)
			}
		}

		rangeSpecific, rangeAny := e.rangeDispatch.candidates(path, keyBytes)
		for _, clauseIdx := range rangeSpecific {
			clause := &e.rangeClauses[clauseIdx]
			if e.hits[clause.id] || !streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
				continue
			}
			if !floatCacheReady {
				floatCached, floatCachedOK = valueToFloat(value)
				floatCacheReady = true
			}
			if !floatCachedOK {
				continue
			}
			number := floatCached
			term := clause.term
			if term.GTE != nil && number < *term.GTE {
				continue
			}
			if term.GT != nil && number <= *term.GT {
				continue
			}
			if term.LTE != nil && number > *term.LTE {
				continue
			}
			if term.LT != nil && number >= *term.LT {
				continue
			}
			e.hits[clause.id] = true
		}
		for _, clauseIdx := range rangeAny {
			clause := &e.rangeClauses[clauseIdx]
			if e.hits[clause.id] || !streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
				continue
			}
			if !floatCacheReady {
				floatCached, floatCachedOK = valueToFloat(value)
				floatCacheReady = true
			}
			if !floatCachedOK {
				continue
			}
			number := floatCached
			term := clause.term
			if term.GTE != nil && number < *term.GTE {
				continue
			}
			if term.GT != nil && number <= *term.GT {
				continue
			}
			if term.LTE != nil && number > *term.LTE {
				continue
			}
			if term.LT != nil && number >= *term.LT {
				continue
			}
			e.hits[clause.id] = true
		}

		inSpecific, inAny := e.inDispatch.candidates(path, keyBytes)
		for _, clauseIdx := range inSpecific {
			clause := &e.inClauses[clauseIdx]
			if e.hits[clause.id] || !streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
				continue
			}
			if !stringCacheReady {
				stringCached, stringCachedOK = valueToString(value)
				stringCacheReady = true
			}
			if !stringCachedOK {
				continue
			}
			candidate := stringCached
			_, exists := clause.candidate[candidate]
			e.hits[clause.id] = exists
		}
		for _, clauseIdx := range inAny {
			clause := &e.inClauses[clauseIdx]
			if e.hits[clause.id] || !streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
				continue
			}
			if !stringCacheReady {
				stringCached, stringCachedOK = valueToString(value)
				stringCacheReady = true
			}
			if !stringCachedOK {
				continue
			}
			candidate := stringCached
			_, exists := clause.candidate[candidate]
			e.hits[clause.id] = exists
		}

		existsSpecific, existsAny := e.existsDispatch.candidates(path, keyBytes)
		for _, clauseIdx := range existsSpecific {
			clause := &e.existsClauses[clauseIdx]
			if e.hits[clause.id] || !streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
				continue
			}
			e.hits[clause.id] = true
		}
		for _, clauseIdx := range existsAny {
			clause := &e.existsClauses[clauseIdx]
			if e.hits[clause.id] || !streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
				continue
			}
			e.hits[clause.id] = true
		}
		return
	}

	for i := range e.termClauses {
		clause := &e.termClauses[i]
		if e.hits[clause.id] || !streamPathTailMatches(clause.tailKind, clause.tailKeyBytes, clause.tailIndex, path, keyBytes) ||
			!streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
			continue
		}
		if !stringCacheReady {
			stringCached, stringCachedOK = valueToString(value)
			stringCacheReady = true
		}
		if !stringCachedOK {
			continue
		}
		candidate := stringCached
		if clause.ignoreCase {
			if !lowerCacheReady {
				lowerStringCached = strings.ToLower(stringCached)
				lowerCacheReady = true
			}
			candidate = lowerStringCached
		}
		switch clause.mode {
		case streamTermEq:
			e.hits[clause.id] = candidate == clause.needle
		case streamTermContains:
			e.hits[clause.id] = strings.Contains(candidate, clause.needle)
		case streamTermPrefix:
			e.hits[clause.id] = strings.HasPrefix(candidate, clause.needle)
		}
	}

	for i := range e.rangeClauses {
		clause := &e.rangeClauses[i]
		if e.hits[clause.id] || !streamPathTailMatches(clause.tailKind, clause.tailKeyBytes, clause.tailIndex, path, keyBytes) ||
			!streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
			continue
		}
		if !floatCacheReady {
			floatCached, floatCachedOK = valueToFloat(value)
			floatCacheReady = true
		}
		if !floatCachedOK {
			continue
		}
		number := floatCached
		term := clause.term
		if term.GTE != nil && number < *term.GTE {
			continue
		}
		if term.GT != nil && number <= *term.GT {
			continue
		}
		if term.LTE != nil && number > *term.LTE {
			continue
		}
		if term.LT != nil && number >= *term.LT {
			continue
		}
		e.hits[clause.id] = true
	}

	for i := range e.inClauses {
		clause := &e.inClauses[i]
		if e.hits[clause.id] || !streamPathTailMatches(clause.tailKind, clause.tailKeyBytes, clause.tailIndex, path, keyBytes) ||
			!streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
			continue
		}
		if !stringCacheReady {
			stringCached, stringCachedOK = valueToString(value)
			stringCacheReady = true
		}
		if !stringCachedOK {
			continue
		}
		candidate := stringCached
		_, exists := clause.candidate[candidate]
		e.hits[clause.id] = exists
	}

	for i := range e.existsClauses {
		clause := &e.existsClauses[i]
		if e.hits[clause.id] || !streamPathTailMatches(clause.tailKind, clause.tailKeyBytes, clause.tailIndex, path, keyBytes) ||
			!streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
			continue
		}
		e.hits[clause.id] = true
	}
}

func (e *streamSelectorEngine) stringValueNeeded(path []streamPathSegment, keyBytes []byte) bool {
	if e.emptySelector {
		return false
	}
	if e.fastEq != nil {
		if e.hits[e.fastEq.id] {
			return false
		}
		return streamLiteralPathMatches(e.fastEq.path, path, keyBytes)
	}
	if e.fastRecursiveEq != nil {
		if e.hits[e.fastRecursiveEq.id] {
			return false
		}
		return streamPathTailMatches(streamTailFilterObjectKey, e.fastRecursiveEq.keyBytes, 0, path, keyBytes)
	}
	if e.useDispatch {
		termSpecific, termAny := e.termDispatch.candidates(path, keyBytes)
		for _, clauseIdx := range termSpecific {
			clause := &e.termClauses[clauseIdx]
			if e.hits[clause.id] {
				continue
			}
			if streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
				return true
			}
		}
		for _, clauseIdx := range termAny {
			clause := &e.termClauses[clauseIdx]
			if e.hits[clause.id] {
				continue
			}
			if streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
				return true
			}
		}

		rangeSpecific, rangeAny := e.rangeDispatch.candidates(path, keyBytes)
		for _, clauseIdx := range rangeSpecific {
			clause := &e.rangeClauses[clauseIdx]
			if e.hits[clause.id] {
				continue
			}
			if streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
				return true
			}
		}
		for _, clauseIdx := range rangeAny {
			clause := &e.rangeClauses[clauseIdx]
			if e.hits[clause.id] {
				continue
			}
			if streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
				return true
			}
		}

		inSpecific, inAny := e.inDispatch.candidates(path, keyBytes)
		for _, clauseIdx := range inSpecific {
			clause := &e.inClauses[clauseIdx]
			if e.hits[clause.id] {
				continue
			}
			if streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
				return true
			}
		}
		for _, clauseIdx := range inAny {
			clause := &e.inClauses[clauseIdx]
			if e.hits[clause.id] {
				continue
			}
			if streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
				return true
			}
		}
		return false
	}

	for i := range e.termClauses {
		clause := &e.termClauses[i]
		if e.hits[clause.id] {
			continue
		}
		if streamPathTailMatches(clause.tailKind, clause.tailKeyBytes, clause.tailIndex, path, keyBytes) &&
			streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
			return true
		}
	}
	for i := range e.rangeClauses {
		clause := &e.rangeClauses[i]
		if e.hits[clause.id] {
			continue
		}
		if streamPathTailMatches(clause.tailKind, clause.tailKeyBytes, clause.tailIndex, path, keyBytes) &&
			streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
			return true
		}
	}
	for i := range e.inClauses {
		clause := &e.inClauses[i]
		if e.hits[clause.id] {
			continue
		}
		if streamPathTailMatches(clause.tailKind, clause.tailKeyBytes, clause.tailIndex, path, keyBytes) &&
			streamPathMatchesKnownRecursive(clause.path, path, keyBytes, clause.hasRecursive, clause.singleRecursive, clause.recursiveIdx) {
			return true
		}
	}
	return false
}

func (e *streamSelectorEngine) match(rootIsObject bool) bool {
	if e.emptySelector {
		return true
	}
	if !rootIsObject {
		return false
	}
	return e.matchSelector(e.selector)
}

func (e *streamSelectorEngine) matchSelector(selector Selector) bool {
	if len(selector.Or) > 0 {
		for _, branch := range selector.Or {
			if e.matchSelector(branch) {
				return true
			}
		}
		return false
	}
	if selector.Not != nil && e.matchSelector(*selector.Not) {
		return false
	}
	if selector.Eq != nil && !e.hits[e.termIDs[selector.Eq]] {
		return false
	}
	if selector.Contains != nil && !e.hits[e.termIDs[selector.Contains]] {
		return false
	}
	if selector.IContains != nil && !e.hits[e.termIDs[selector.IContains]] {
		return false
	}
	if selector.Prefix != nil && !e.hits[e.termIDs[selector.Prefix]] {
		return false
	}
	if selector.IPrefix != nil && !e.hits[e.termIDs[selector.IPrefix]] {
		return false
	}
	if selector.Range != nil && !e.hits[e.rangeIDs[selector.Range]] {
		return false
	}
	if selector.In != nil && !e.hits[e.inIDs[selector.In]] {
		return false
	}
	if selector.Exists != "" && !e.hits[e.existsIDs[selector.Exists]] {
		return false
	}
	for _, clause := range selector.And {
		if !e.matchSelector(clause) {
			return false
		}
	}
	return true
}

func compileStreamPath(field string) ([]streamPathToken, error) {
	segments, err := selectorPathSegments(field)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, fmt.Errorf("selector field %q empty", field)
	}
	out := make([]streamPathToken, 0, len(segments))
	for _, segment := range segments {
		switch segment {
		case "*":
			out = append(out, streamPathToken{mode: streamPathTokenObjectWildcard})
		case "[]":
			out = append(out, streamPathToken{mode: streamPathTokenArrayWildcard})
		case "**":
			out = append(out, streamPathToken{mode: streamPathTokenAnyChild})
		case "...":
			out = append(out, streamPathToken{mode: streamPathTokenRecursive})
		default:
			index, err := strconv.Atoi(segment)
			if err == nil && index >= 0 {
				out = append(out, streamPathToken{
					mode:      streamPathTokenLiteral,
					raw:       segment,
					arrayOnly: true,
					index:     index,
				})
				continue
			}
			out = append(out, streamPathToken{
				mode:     streamPathTokenLiteral,
				raw:      segment,
				rawBytes: []byte(segment),
			})
		}
	}
	return out, nil
}

func streamPathMatchesKnownRecursive(pattern []streamPathToken, path []streamPathSegment, keyBytes []byte, hasRecursive bool, singleRecursive bool, recursiveIdx int) bool {
	if !hasRecursive {
		return streamPathMatchesLinear(pattern, path, keyBytes)
	}
	if singleRecursive {
		return streamPathMatchesSingleRecursive(pattern, path, keyBytes, recursiveIdx)
	}
	return streamPathMatchesRecursiveMemo(pattern, path, keyBytes)
}

func streamPatternRecursiveInfo(pattern []streamPathToken) (bool, bool, int) {
	recursiveIdx := -1
	for i, token := range pattern {
		if token.mode != streamPathTokenRecursive {
			continue
		}
		if recursiveIdx >= 0 {
			return true, false, -1
		}
		recursiveIdx = i
	}
	if recursiveIdx >= 0 {
		return true, true, recursiveIdx
	}
	return false, false, -1
}

func streamPatternTailFilter(pattern []streamPathToken) (streamTailFilterKind, string, int) {
	if len(pattern) == 0 {
		return streamTailFilterNone, "", 0
	}
	last := pattern[len(pattern)-1]
	if last.mode != streamPathTokenLiteral {
		return streamTailFilterNone, "", 0
	}
	if last.arrayOnly {
		return streamTailFilterArrayIndex, "", last.index
	}
	return streamTailFilterObjectKey, last.raw, 0
}

func streamPathTailMatches(kind streamTailFilterKind, key []byte, index int, path []streamPathSegment, keyBytes []byte) bool {
	if kind == streamTailFilterNone {
		return true
	}
	if len(path) == 0 {
		return false
	}
	segment := path[len(path)-1]
	switch kind {
	case streamTailFilterObjectKey:
		if segment.kind != streamPathObjectKey {
			return false
		}
		return bytes.Equal(keyBytes[segment.keyStart:segment.keyEnd], key)
	case streamTailFilterArrayIndex:
		return segment.kind == streamPathArrayIndex && segment.index == index
	default:
		return true
	}
}

func streamLiteralPathMatches(pattern []streamPathToken, path []streamPathSegment, keyBytes []byte) bool {
	if len(pattern) != len(path) {
		return false
	}
	for i, token := range pattern {
		if token.mode != streamPathTokenLiteral {
			return false
		}
		segment := path[i]
		switch segment.kind {
		case streamPathObjectKey:
			if token.arrayOnly {
				return false
			}
			key := keyBytes[segment.keyStart:segment.keyEnd]
			if !streamPathLiteralMatchesKey(token, key) {
				return false
			}
		case streamPathArrayIndex:
			if !token.arrayOnly || segment.index != token.index {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func streamPathMatchesLinear(pattern []streamPathToken, path []streamPathSegment, keyBytes []byte) bool {
	if len(pattern) != len(path) {
		return false
	}
	for i := range pattern {
		if !streamPathTokenMatchesSegment(pattern[i], path[i], keyBytes) {
			return false
		}
	}
	return true
}

func streamPathMatchesRecursiveMemo(pattern []streamPathToken, path []streamPathSegment, keyBytes []byte) bool {
	pathLen := len(path)
	patternLen := len(pattern)
	stride := pathLen + 1
	cells := (patternLen + 1) * stride

	var stackMemo [512]uint8
	var memo []uint8
	if cells <= len(stackMemo) {
		memo = stackMemo[:cells]
	} else {
		memo = make([]uint8, cells)
	}
	return streamPathMatchesRecursiveMemoFrom(pattern, path, keyBytes, 0, 0, memo, stride)
}

func streamPathMatchesSingleRecursive(pattern []streamPathToken, path []streamPathSegment, keyBytes []byte, recursiveIdx int) bool {
	if recursiveIdx < 0 || recursiveIdx >= len(pattern) {
		return false
	}
	for i := 0; i < recursiveIdx; i++ {
		if i >= len(path) || !streamPathTokenMatchesSegment(pattern[i], path[i], keyBytes) {
			return false
		}
	}
	if recursiveIdx == len(pattern)-1 {
		return true
	}
	right := pattern[recursiveIdx+1:]
	if len(path) < recursiveIdx+len(right) {
		return false
	}
	start := len(path) - len(right)
	if start < recursiveIdx {
		return false
	}
	for i := 0; i < len(right); i++ {
		if !streamPathTokenMatchesSegment(right[i], path[start+i], keyBytes) {
			return false
		}
	}
	return true
}

func streamPathMatchesRecursiveMemoFrom(pattern []streamPathToken, path []streamPathSegment, keyBytes []byte, patternIdx, pathIdx int, memo []uint8, stride int) bool {
	memoIdx := patternIdx*stride + pathIdx
	switch memo[memoIdx] {
	case 1:
		return false
	case 2:
		return true
	}

	matched := false
	if patternIdx == len(pattern) {
		matched = pathIdx == len(path)
	} else {
		token := pattern[patternIdx]
		if token.mode == streamPathTokenRecursive {
			if patternIdx == len(pattern)-1 {
				matched = true
			} else {
				for i := pathIdx; i <= len(path); i++ {
					if streamPathMatchesRecursiveMemoFrom(pattern, path, keyBytes, patternIdx+1, i, memo, stride) {
						matched = true
						break
					}
				}
			}
		} else if pathIdx < len(path) && streamPathTokenMatchesSegment(token, path[pathIdx], keyBytes) {
			matched = streamPathMatchesRecursiveMemoFrom(pattern, path, keyBytes, patternIdx+1, pathIdx+1, memo, stride)
		}
	}
	if matched {
		memo[memoIdx] = 2
	} else {
		memo[memoIdx] = 1
	}
	return matched
}

func streamPathTokenMatchesSegment(token streamPathToken, segment streamPathSegment, keyBytes []byte) bool {
	switch token.mode {
	case streamPathTokenObjectWildcard:
		return segment.kind == streamPathObjectKey
	case streamPathTokenArrayWildcard:
		return segment.kind == streamPathArrayIndex
	case streamPathTokenAnyChild:
		return segment.kind == streamPathObjectKey || segment.kind == streamPathArrayIndex
	case streamPathTokenLiteral:
		switch segment.kind {
		case streamPathObjectKey:
			if token.arrayOnly {
				return false
			}
			key := keyBytes[segment.keyStart:segment.keyEnd]
			return streamPathLiteralMatchesKey(token, key)
		case streamPathArrayIndex:
			return token.arrayOnly && segment.index == token.index
		default:
			return false
		}
	default:
		return false
	}
}

func streamPathLiteralMatchesKey(token streamPathToken, key []byte) bool {
	if token.rawBytes != nil {
		return bytes.Equal(token.rawBytes, key)
	}
	return streamStringEqualsBytes(token.raw, key)
}

func streamStringEqualsBytes(s string, b []byte) bool {
	if len(s) != len(b) {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] != b[i] {
			return false
		}
	}
	return true
}
