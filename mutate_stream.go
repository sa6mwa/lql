package lql

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"sync"
	"unicode/utf8"
)

var (
	jsonTrueLiteral  = []byte("true")
	jsonFalseLiteral = []byte("false")
	jsonNullLiteral  = []byte("null")
	jsonNewline      = []byte{'\n'}

	mutateStreamCaptureHint = newStreamAdaptiveHint(256, 256, 8*1024*1024)
)

const defaultMutateStreamSpoolMemoryBytes = 1 * 1024 * 1024

// MutateStreamStopReason classifies graceful early-stop outcomes.
type MutateStreamStopReason string

const (
	// MutateStreamStopNone indicates normal end-of-stream completion.
	MutateStreamStopNone MutateStreamStopReason = ""
	// MutateStreamStopCallbackStop indicates callback requested graceful stop.
	MutateStreamStopCallbackStop MutateStreamStopReason = "callback_stop"
)

// MutateStreamResult reports deterministic run summary for MutateStream.
type MutateStreamResult struct {
	CandidatesSeen    int64
	CandidatesWritten int64
	BytesRead         int64
	BytesWritten      int64
	BytesCaptured     int64
	SpillCount        int64
	SpillBytes        int64
	StoppedEarly      bool
	StopReason        MutateStreamStopReason
	LastOffset        int64
}

// MutateStreamRequest configures streaming mutation evaluation.
type MutateStreamRequest struct {
	Ctx    context.Context
	Reader io.Reader
	Writer io.Writer
	Mode   MutateStreamMode
	// Plan reuses compiled mutation state across stream invocations.
	// When set, Mutations must be empty.
	Plan      MutateStreamPlan
	Mutations []Mutation
	// SpoolMemoryBytes sets the in-memory callback payload threshold.
	// Values <= 0 default to 1 MiB.
	SpoolMemoryBytes int64
	// SpoolTempDir sets the temp directory used when callback payloads spill to disk.
	// Empty defaults to /tmp.
	SpoolTempDir string
	// SpoolFilePattern controls os.CreateTemp naming for spilled callback payloads.
	// Empty defaults to "lql-spool-*.json".
	SpoolFilePattern string
	// DisableInternalSpool requires caller-managed payload sink when callback
	// capture is enabled.
	DisableInternalSpool bool
	// PayloadSinkFactory creates a candidate payload sink for caller-managed
	// callback capture.
	PayloadSinkFactory MutateStreamPayloadSinkFactory
	// MaxCandidateBytes is measured on canonical emitted candidate bytes
	// (compact JSON form after mutation for object roots) from the first
	// non-whitespace byte to closing token.
	MaxCandidateBytes int64
	OnValue           func(MutateStreamValue) error
}

// MutateStreamPayloadSink receives candidate payload bytes in callback mode.
type MutateStreamPayloadSink interface {
	io.Writer
	Finalize() error
	Open() (io.ReadCloser, error)
	Bytes() []byte
	SizeHint() int
	Cleanup() error
}

// MutateStreamPayloadSinkRequest describes one candidate sink allocation.
type MutateStreamPayloadSinkRequest struct {
	Offset int64
}

// MutateStreamPayloadSinkFactory creates per-candidate payload sinks.
type MutateStreamPayloadSinkFactory func(MutateStreamPayloadSinkRequest) (MutateStreamPayloadSink, error)

// MutateStreamValue describes one mutated candidate value from the stream.
// JSON, Value, and OpenJSON are valid only during callback invocation.
type MutateStreamValue struct {
	// Value mirrors JSON for compatibility with older callback handlers.
	// Prefer JSON for new code.
	Value json.RawMessage
	JSON  []byte
	// OpenJSON returns payload bytes from offset 0, whether in-memory or spooled.
	OpenJSON func() (io.ReadCloser, error)
	Size     int64
	Offset   int64
}

// MutateStreamPlan reuses compiled mutation state for MutateStream.
//
// A zero-value plan is treated as unset.
type MutateStreamPlan struct {
	template *mutateCompiledProgram
}

type mutateCompiledProgram struct {
	compiled  []compiledStreamMutation
	rootRules []mutateActiveRule
}

// IsZero reports whether the plan is unset.
func (p MutateStreamPlan) IsZero() bool {
	return p.template == nil
}

// NewMutateStreamPlan compiles mutations for reuse across MutateStream calls.
func NewMutateStreamPlan(mutations []Mutation) (MutateStreamPlan, error) {
	program, err := compileMutateProgram(mutations)
	if err != nil {
		return MutateStreamPlan{}, err
	}
	return MutateStreamPlan{template: program}, nil
}

// MutateStreamWithResult applies mutations to JSON values from a stream and
// emits values in deterministic input order. Top-level arrays are treated as
// streams of candidate values (including nested top-level arrays).
func MutateStreamWithResult(req MutateStreamRequest) (result MutateStreamResult, err error) {
	result = MutateStreamResult{LastOffset: -1}
	if req.Reader == nil {
		return result, &StreamError{Code: StreamErrorInvalidBody, Detail: "mutate stream reader required", Offset: -1}
	}
	if req.OnValue == nil && req.Writer == nil {
		return result, &StreamError{Code: StreamErrorInvalidBody, Detail: "mutate stream callback or writer required", Offset: -1}
	}
	if req.OnValue != nil && req.DisableInternalSpool && req.PayloadSinkFactory == nil {
		return result, &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: "mutate stream caller spool requested but payload sink factory is nil",
			Offset: -1,
		}
	}
	if !req.Plan.IsZero() && len(req.Mutations) > 0 {
		return result, &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: "mutate stream request must set either mutations or plan, not both",
			Offset: -1,
		}
	}
	switch req.Mode {
	case MutateModeAuto, MutateSingleValueOnly, MutateObjectRootOnly, MutateSingleObjectOnly:
	default:
		return result, &StreamError{
			Code:   StreamErrorInvalidBody,
			Detail: fmt.Sprintf("unknown mutate stream mode %d", req.Mode),
			Offset: -1,
		}
	}

	state := acquireMutateStreamState()
	state.reset(req)
	state.result = result
	defer func() {
		cleanupErr := state.candidateSink.cleanupCandidate()
		if cleanupErr != nil && err == nil {
			offset := int64(-1)
			if state.reader != nil {
				offset = state.reader.Offset()
			}
			err = &StreamError{
				Code:   StreamErrorInternal,
				Detail: "mutate stream payload cleanup failed",
				Offset: offset,
				Err:    cleanupErr,
			}
		}
		if cleanupErr != nil {
			_ = state.candidateSink.releaseAll()
		}
		result = state.result
		releaseMutateStreamState(state)
	}()
	if !req.Plan.IsZero() {
		if req.Plan.template == nil {
			return result, &StreamError{
				Code:   StreamErrorInvalidBody,
				Detail: "mutate stream plan is invalid",
				Offset: -1,
			}
		}
		state.compiled = req.Plan.template.compiled
		state.rootRules = req.Plan.template.rootRules
		state.requiresMutation = len(state.compiled) > 0
	} else if len(req.Mutations) > 0 {
		state.requiresMutation = true
		if err := state.compileMutationProgram(req.Mutations); err != nil {
			return result, &StreamError{
				Code:   StreamErrorInvalidBody,
				Detail: "invalid mutation program",
				Offset: -1,
				Err:    err,
			}
		}
	}

	if buffered, ok := req.Reader.(*bufio.Reader); ok {
		state.readerStorage.Reset(state.ctx, buffered)
	} else {
		if state.bufferedReader == nil {
			state.bufferedReader = bufio.NewReaderSize(req.Reader, 64*1024)
		} else {
			state.bufferedReader.Reset(req.Reader)
		}
		state.readerStorage.Reset(state.ctx, state.bufferedReader)
	}
	state.reader = &state.readerStorage
	for {
		start, err := readNonSpaceByte(state.reader)
		if err != nil {
			if err == io.EOF {
				if state.requiresSingleTopLevelValue() && state.seenTopLevel == 0 {
					return result, &StreamError{Code: StreamErrorInvalidBody, Detail: "mutate stream mode requires exactly one top-level JSON value", Offset: -1}
				}
				state.result.LastOffset = state.reader.Offset()
				state.result.BytesRead = state.reader.Offset()
				return state.result, nil
			}
			return result, state.wrapStreamError(err, state.reader.Offset())
		}
		if state.requiresSingleTopLevelValue() && state.seenTopLevel > 0 {
			return result, &StreamError{
				Code:   StreamErrorInvalidBody,
				Detail: "mutate stream mode requires exactly one top-level JSON value",
				Offset: state.reader.Offset() - 1,
			}
		}
		if state.requiresObjectRoot() && start != '{' {
			return result, &StreamError{
				Code:   StreamErrorInvalidBody,
				Detail: "mutate stream mode requires top-level JSON object root",
				Offset: state.reader.Offset() - 1,
			}
		}
		if err := state.consumeCandidate(start); err != nil {
			return result, err
		}
		state.seenTopLevel++
		if state.stopRequested {
			return state.result, nil
		}
	}
}

// MutateStream applies mutations to JSON values from a stream and emits values
// in deterministic input order. Top-level arrays are treated as streams of
// candidate values (including nested top-level arrays).
func MutateStream(req MutateStreamRequest) error {
	_, err := MutateStreamWithResult(req)
	return err
}

var mutateStreamStatePool = sync.Pool{
	New: func() any {
		return &mutateStreamState{
			valueHint: 256,
			path:      make([]streamPathSegment, 0, 32),
			keyBytes:  make([]byte, 0, 256),
		}
	},
}

func acquireMutateStreamState() *mutateStreamState {
	return mutateStreamStatePool.Get().(*mutateStreamState)
}

func releaseMutateStreamState(state *mutateStreamState) {
	if state == nil {
		return
	}
	_ = state.candidateSink.releaseAll()
	state.ctx = nil
	state.reader = nil
	state.readerStorage = streamByteReader{}
	state.onValue = nil
	state.writer = nil
	state.mode = MutateModeAuto
	state.maxCandidateSize = 0
	state.spoolCfg = streamSpoolConfig{}
	state.seenTopLevel = 0
	state.disableSpool = false
	state.payloadSinkMaker = nil
	state.requiresMutation = false
	state.compiled = nil
	state.rootRules = nil
	state.stringBuf = state.stringBuf[:0]
	state.numberBuf = state.numberBuf[:0]
	state.scratchBuf = state.scratchBuf[:0]
	state.valueHint = 256
	state.candidateHint = 0
	state.path = state.path[:0]
	state.keyBytes = state.keyBytes[:0]
	state.openJSON = nil
	state.result = MutateStreamResult{}
	state.stopRequested = false
	for i := range state.missingCache {
		state.missingCache[i].rules = state.missingCache[i].rules[:0]
		state.missingCache[i].root = nil
		for j := range state.missingCache[i].members {
			state.missingCache[i].members[j].key = ""
			state.missingCache[i].members[j].value = nil
		}
		state.missingCache[i].members = state.missingCache[i].members[:0]
		state.missingCache[i].hasValue = false
	}
	state.missingCache = state.missingCache[:0]
	state.releaseMaterializedPayloads()
	state.candidateSink.writer = nil
	state.candidateSink.capture = false
	state.candidateSink.size = 0
	state.candidateSink.max = 0
	mutateStreamStatePool.Put(state)
}

type mutateStreamState struct {
	ctx              context.Context
	reader           *streamByteReader
	readerStorage    streamByteReader
	bufferedReader   *bufio.Reader
	payloadReader    *bufio.Reader
	fileReader       *bufio.Reader
	payloadSrc       bytes.Reader
	payloadReaderBuf streamByteReader
	onValue          func(MutateStreamValue) error
	writer           io.Writer
	mode             MutateStreamMode
	maxCandidateSize int64
	spoolCfg         streamSpoolConfig
	seenTopLevel     int64
	disableSpool     bool
	payloadSinkMaker MutateStreamPayloadSinkFactory

	requiresMutation bool
	compiled         []compiledStreamMutation
	rootRules        []mutateActiveRule

	stringBuf     []byte
	numberBuf     []byte
	scratchBuf    []byte
	fileReadBuf   []byte
	base64Buf     []byte
	valueHint     int
	candidateHint int
	path          []streamPathSegment
	keyBytes      []byte

	candidateSink mutateCandidateSink
	openJSON      func() (io.ReadCloser, error)
	result        MutateStreamResult
	stopRequested bool

	missingCache []mutateMissingCacheEntry

	materializedPayloads [][]byte
	materializeWriter    mutateSliceWriter
	numberStateWriter    mutateSliceWriter
}

type mutateMissingMember struct {
	key   string
	value *mutateMissingNode
}

type mutateMissingCacheEntry struct {
	rules    []mutateActiveRule
	root     *mutateMissingNode
	members  []mutateMissingMember
	hasValue bool
}

type mutateMissingNodeKind uint8

const (
	mutateMissingNodeObject mutateMissingNodeKind = iota
	mutateMissingNodeLiteral
	mutateMissingNodeFile
)

type mutateMissingNode struct {
	kind    mutateMissingNodeKind
	literal []byte
	file    *mutationFileValue
	members []mutateMissingMember
}

type mutateMissingFileMarker struct {
	file *mutationFileValue
}

func (s *mutateStreamState) reset(req MutateStreamRequest) {
	s.ctx = normalizeStreamContext(req.Ctx)
	s.reader = nil
	s.payloadSrc.Reset(nil)
	s.payloadReaderBuf = streamByteReader{}
	s.onValue = req.OnValue
	s.writer = req.Writer
	s.mode = req.Mode
	s.maxCandidateSize = req.MaxCandidateBytes
	s.spoolCfg = normalizeMutateStreamSpoolConfig(req.SpoolMemoryBytes, req.SpoolTempDir, req.SpoolFilePattern)
	s.seenTopLevel = 0
	s.disableSpool = req.DisableInternalSpool
	s.payloadSinkMaker = req.PayloadSinkFactory
	s.requiresMutation = false
	s.compiled = nil
	s.rootRules = nil
	s.stringBuf = s.stringBuf[:0]
	s.numberBuf = s.numberBuf[:0]
	s.scratchBuf = s.scratchBuf[:0]
	s.valueHint = 256
	s.candidateHint = mutateStreamCaptureHint.Load()
	s.path = s.path[:0]
	s.keyBytes = s.keyBytes[:0]
	s.openJSON = s.openCurrentPayload
	s.result = MutateStreamResult{LastOffset: -1}
	s.stopRequested = false
	for i := range s.missingCache {
		s.missingCache[i].rules = s.missingCache[i].rules[:0]
		s.missingCache[i].root = nil
		for j := range s.missingCache[i].members {
			s.missingCache[i].members[j].key = ""
			s.missingCache[i].members[j].value = nil
		}
		s.missingCache[i].members = s.missingCache[i].members[:0]
		s.missingCache[i].hasValue = false
	}
	s.missingCache = s.missingCache[:0]
	s.releaseMaterializedPayloads()
	s.candidateSink.writer = nil
	s.candidateSink.capture = false
	s.candidateSink.size = 0
	s.candidateSink.max = 0
}

func (s *mutateStreamState) releaseMaterializedPayloads() {
	for i := range s.materializedPayloads {
		streamJSONBytePool.release(s.materializedPayloads[i])
		s.materializedPayloads[i] = nil
	}
	s.materializedPayloads = s.materializedPayloads[:0]
}

type compiledStreamMutation struct {
	mut      Mutation
	tokens   []streamPathToken
	wildcard bool
	setJSON  []byte
}

type mutateActiveRule struct {
	idx int
	pos int
}

func (s *mutateStreamState) compileMutationProgram(muts []Mutation) error {
	s.compiled = nil
	s.rootRules = nil
	if len(muts) == 0 {
		return nil
	}
	s.compiled = make([]compiledStreamMutation, 0, len(muts))
	for i, mut := range muts {
		tokens, err := compileMutationTokens(mut.Path)
		if err != nil {
			return err
		}
		compiled := compiledStreamMutation{
			mut:      mut,
			tokens:   tokens,
			wildcard: pathHasWildcard(mut.Path),
		}
		if mut.Kind == MutationSet && !mut.hasFileValue() {
			payload, err := json.Marshal(mut.Value)
			if err != nil {
				return err
			}
			compiled.setJSON = payload
		}
		s.compiled = append(s.compiled, compiled)
		s.rootRules = append(s.rootRules, mutateActiveRule{idx: i, pos: 0})
	}
	return nil
}

func compileMutateProgram(muts []Mutation) (*mutateCompiledProgram, error) {
	state := mutateStreamState{}
	if err := state.compileMutationProgram(muts); err != nil {
		return nil, err
	}
	program := &mutateCompiledProgram{}
	if len(state.compiled) > 0 {
		program.compiled = make([]compiledStreamMutation, len(state.compiled))
		copy(program.compiled, state.compiled)
	}
	if len(state.rootRules) > 0 {
		program.rootRules = make([]mutateActiveRule, len(state.rootRules))
		copy(program.rootRules, state.rootRules)
	}
	return program, nil
}

func compileMutationTokens(path []string) ([]streamPathToken, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("mutation path empty")
	}
	out := make([]streamPathToken, 0, len(path))
	for _, segment := range path {
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
					rawBytes:  []byte(segment),
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

func (s *mutateStreamState) consumeCandidate(start byte) (err error) {
	s.releaseMaterializedPayloads()
	defer s.releaseMaterializedPayloads()
	candidateStart := s.reader.Offset() - 1
	if start == '[' && s.flattenTopArray() {
		if err := s.consumeTopArray(); err != nil {
			return s.wrapStreamError(err, s.reader.Offset())
		}
		return nil
	}

	sink := &s.candidateSink
	if err := sink.reset(
		s.writer,
		s.onValue != nil,
		s.maxCandidateSize,
		s.candidateHint,
		s.spoolCfg,
		s.disableSpool,
		s.payloadSinkMaker,
		candidateStart,
	); err != nil {
		return s.wrapStreamError(err, s.reader.Offset())
	}
	defer func() {
		cleanupErr := sink.cleanupCandidate()
		if cleanupErr != nil && err == nil {
			err = &StreamError{
				Code:   StreamErrorInternal,
				Detail: "mutate stream payload cleanup failed",
				Offset: s.reader.Offset(),
				Err:    cleanupErr,
			}
		}
	}()

	if s.requiresMutation && start == '{' {
		if err := s.mutateValue(start, s.rootRules, sink); err != nil {
			return s.wrapStreamError(err, s.reader.Offset())
		}
	} else {
		if err := s.writeValue(start, sink); err != nil {
			return s.wrapStreamError(err, s.reader.Offset())
		}
	}

	if s.writer != nil {
		if _, err := s.writer.Write(jsonNewline); err != nil {
			return s.wrapStreamError(err, s.reader.Offset())
		}
	}

	size := sink.size
	s.result.CandidatesSeen++
	s.result.LastOffset = s.reader.Offset()
	s.result.BytesRead = s.reader.Offset()
	s.result.BytesCaptured += sink.capturedBytes()
	s.result.SpillCount += sink.spillCount()
	s.result.SpillBytes += sink.spillBytes()
	if s.onValue != nil {
		if err := sink.finalizeCapture(); err != nil {
			return s.wrapStreamError(err, s.reader.Offset())
		}
		payload := sink.payload()
		if err := s.onValue(MutateStreamValue{
			Value:    json.RawMessage(payload),
			JSON:     payload,
			OpenJSON: s.openJSON,
			Size:     size,
			Offset:   candidateStart,
		}); err != nil {
			if errors.Is(err, ErrStreamStop) {
				s.stop(MutateStreamStopCallbackStop)
			} else {
				return err
			}
		}
	}
	s.result.CandidatesWritten++
	if s.writer != nil {
		s.result.BytesWritten += size + int64(len(jsonNewline))
	}
	if sizeHint := sink.sizeHint(); sizeHint > 0 {
		s.candidateHint = sizeHint
		mutateStreamCaptureHint.Observe(sizeHint)
	}
	return nil
}

func (s *mutateStreamState) flattenTopArray() bool {
	switch s.mode {
	case MutateSingleValueOnly, MutateObjectRootOnly, MutateSingleObjectOnly:
		return false
	default:
		return true
	}
}

func (s *mutateStreamState) requiresSingleTopLevelValue() bool {
	switch s.mode {
	case MutateSingleValueOnly, MutateSingleObjectOnly:
		return true
	default:
		return false
	}
}

func (s *mutateStreamState) requiresObjectRoot() bool {
	switch s.mode {
	case MutateObjectRootOnly, MutateSingleObjectOnly:
		return true
	default:
		return false
	}
}

func normalizeMutateStreamSpoolConfig(memoryBytes int64, tempDir, pattern string) streamSpoolConfig {
	cfg := normalizeStreamSpoolConfig(memoryBytes, tempDir, pattern)
	if memoryBytes <= 0 {
		cfg.memoryBytes = defaultMutateStreamSpoolMemoryBytes
	}
	return cfg
}

func (s *mutateStreamState) openCurrentPayload() (io.ReadCloser, error) {
	return s.candidateSink.openJSON()
}

func (s *mutateStreamState) consumeTopArray() error {
	start, err := readNonSpaceByte(s.reader)
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
		if err := s.consumeCandidate(start); err != nil {
			return err
		}
		if s.stopRequested {
			return nil
		}
		next, err := readNonSpaceByte(s.reader)
		if err != nil {
			return err
		}
		switch next {
		case ',':
			start, err = readNonSpaceByte(s.reader)
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

func (s *mutateStreamState) stop(reason MutateStreamStopReason) {
	s.stopRequested = true
	s.result.StoppedEarly = true
	s.result.StopReason = reason
}

func (s *mutateStreamState) mutateValue(start byte, rules []mutateActiveRule, out mutateByteWriter) error {
	if len(rules) == 0 {
		return s.writeValue(start, out)
	}

	state := mutateNodeState{kind: mutateValueStateSource, start: start}
	var pendingStack [32]mutateActiveRule
	pending := pendingStack[:0]
	rulePos := 0
	for idx := range s.compiled {
		if rulePos >= len(rules) {
			break
		}
		if rules[rulePos].idx > idx {
			continue
		}
		groupStart := rulePos
		for rulePos < len(rules) && rules[rulePos].idx == idx {
			rulePos++
		}

		compiled := s.compiled[idx]
		hasDirect := false
		for i := groupStart; i < rulePos; i++ {
			if rules[i].pos >= len(compiled.tokens) {
				hasDirect = true
				break
			}
		}
		if hasDirect {
			switch compiled.mut.Kind {
			case MutationSet:
				if len(pending) > 0 {
					if err := s.validatePendingState(state, pending); err != nil {
						return err
					}
					state.kind = mutateValueStateRemoved
					state.payload = nil
					state.fileValue = nil
				} else if state.kind == mutateValueStateSource {
					if err := s.skipValue(state.start); err != nil {
						return err
					}
				}
				pending = pending[:0]
				if compiled.mut.hasFileValue() {
					state.kind = mutateValueStateFile
					state.payload = nil
					state.fileValue = compiled.mut.fileValue
				} else {
					state.kind = mutateValueStatePayload
					state.payload = compiled.setJSON
					state.fileValue = nil
				}
			case MutationRemove:
				if len(pending) > 0 {
					if err := s.validatePendingState(state, pending); err != nil {
						return err
					}
					state.kind = mutateValueStateRemoved
					state.payload = nil
					state.fileValue = nil
				} else if state.kind == mutateValueStateSource {
					if err := s.skipValue(state.start); err != nil {
						return err
					}
				}
				pending = pending[:0]
				state.kind = mutateValueStateRemoved
				state.payload = nil
				state.fileValue = nil
			case MutationIncrement:
				var err error
				state, err = s.materializePendingState(state, pending)
				if err != nil {
					return err
				}
				pending = pending[:0]

				var (
					value float64
					ok    bool
				)
				switch state.kind {
				case mutateValueStateRemoved:
					value = 0
					ok = true
				case mutateValueStatePayload:
					value, ok, err = parseJSONNumericPayload(state.payload)
					if err != nil {
						return err
					}
				case mutateValueStateNumber:
					if state.isInt {
						value = float64(state.intVal)
					} else {
						value = state.floatVal
					}
					ok = true
				case mutateValueStateSource:
					value, ok, err = s.consumeNumericSourceValue(state.start)
					if err != nil {
						return err
					}
				default:
					return fmt.Errorf("internal mutate stream state")
				}
				if !ok {
					return fmt.Errorf("value at mutation path is not numeric")
				}
				isInt, intVal, floatVal := normalizeNumberState(value + compiled.mut.Delta)
				state.kind = mutateValueStateNumber
				state.payload = nil
				state.isInt = isInt
				if isInt {
					state.intVal = intVal
				} else {
					state.floatVal = floatVal
				}
			default:
				return fmt.Errorf("unknown mutation kind")
			}
		}
		for i := groupStart; i < rulePos; i++ {
			if rules[i].pos < len(compiled.tokens) {
				pending = append(pending, rules[i])
			}
		}
	}

	switch state.kind {
	case mutateValueStateRemoved:
		return s.emitMissingValue(pending, out)
	case mutateValueStateFile:
		return s.mutateFileValue(state.fileValue, pending, out)
	case mutateValueStatePayload:
		return s.mutatePayloadValue(state.payload, pending, out)
	case mutateValueStateNumber:
		if len(pending) == 0 {
			return s.writeNodeState(state, out)
		}
		materialized, err := s.materializePendingState(state, pending)
		if err != nil {
			return err
		}
		if materialized.kind == mutateValueStateRemoved {
			return nil
		}
		return s.writeNodeState(materialized, out)
	default:
		return s.mutateSourceValue(state.start, pending, out)
	}
}

type mutateNodeState struct {
	kind      mutateValueState
	start     byte
	payload   []byte
	fileValue *mutationFileValue
	isInt     bool
	intVal    int64
	floatVal  float64
}

type mutateValueState uint8

const (
	mutateValueStateSource mutateValueState = iota
	mutateValueStatePayload
	mutateValueStateFile
	mutateValueStateRemoved
	mutateValueStateNumber
)

func (s *mutateStreamState) validatePendingState(state mutateNodeState, pending []mutateActiveRule) error {
	if len(pending) == 0 {
		return nil
	}
	return s.applyPendingState(state, pending, mutateDiscardWriter{})
}

func (s *mutateStreamState) materializePendingState(state mutateNodeState, pending []mutateActiveRule) (mutateNodeState, error) {
	if len(pending) == 0 {
		return state, nil
	}
	buf := streamJSONBytePool.acquire(s.valueHint)
	s.materializeWriter.buf = buf[:0]
	err := s.applyPendingState(state, pending, &s.materializeWriter)
	if err != nil {
		streamJSONBytePool.release(s.materializeWriter.buf)
		return mutateNodeState{}, err
	}
	if len(s.materializeWriter.buf) == 0 {
		streamJSONBytePool.release(s.materializeWriter.buf)
		return mutateNodeState{kind: mutateValueStateRemoved}, nil
	}
	payload := s.materializeWriter.buf
	s.materializedPayloads = append(s.materializedPayloads, payload)
	if len(payload) > 0 {
		s.valueHint = len(payload)
	}
	return mutateNodeState{kind: mutateValueStatePayload, payload: payload}, nil
}

func (s *mutateStreamState) applyPendingState(state mutateNodeState, pending []mutateActiveRule, out mutateByteWriter) error {
	switch state.kind {
	case mutateValueStateRemoved:
		return s.emitMissingValue(pending, out)
	case mutateValueStateFile:
		return s.mutateFileValue(state.fileValue, pending, out)
	case mutateValueStatePayload:
		return s.mutatePayloadValue(state.payload, pending, out)
	case mutateValueStateNumber:
		buf := streamJSONBytePool.acquire(64)
		s.numberStateWriter.buf = buf[:0]
		if err := s.writeNodeState(state, &s.numberStateWriter); err != nil {
			streamJSONBytePool.release(s.numberStateWriter.buf)
			return err
		}
		err := s.mutatePayloadValue(s.numberStateWriter.buf, pending, out)
		streamJSONBytePool.release(s.numberStateWriter.buf)
		return err
	default:
		return s.mutateSourceValue(state.start, pending, out)
	}
}

func (s *mutateStreamState) writeNodeState(state mutateNodeState, out mutateByteWriter) error {
	switch state.kind {
	case mutateValueStateFile:
		return s.emitFileMutationValue(state.fileValue, out)
	case mutateValueStatePayload:
		_, err := out.Write(state.payload)
		return err
	case mutateValueStateNumber:
		s.scratchBuf = s.scratchBuf[:0]
		if state.isInt {
			s.scratchBuf = strconv.AppendInt(s.scratchBuf, state.intVal, 10)
		} else {
			s.scratchBuf = strconv.AppendFloat(s.scratchBuf, state.floatVal, 'g', -1, 64)
		}
		_, err := out.Write(s.scratchBuf)
		return err
	case mutateValueStateSource:
		return s.writeValue(state.start, out)
	case mutateValueStateRemoved:
		return nil
	default:
		return fmt.Errorf("internal mutate stream state")
	}
}

func (s *mutateStreamState) mutateSourceValue(start byte, rules []mutateActiveRule, out mutateByteWriter) error {
	if len(rules) == 0 {
		return s.writeValue(start, out)
	}
	switch start {
	case '{':
		return s.mutateObjectValue(rules, out)
	case '[':
		if hasCreateNonWildcardRule(s.compiled, rules) {
			if err := s.skipValue(start); err != nil {
				return err
			}
			return s.emitMissingValue(rules, out)
		}
		return s.mutateArrayValue(rules, out)
	default:
		if hasCreateNonWildcardRule(s.compiled, rules) {
			if err := s.skipValue(start); err != nil {
				return err
			}
			return s.emitMissingValue(rules, out)
		}
		return s.writeValue(start, out)
	}
}

func (s *mutateStreamState) mutatePayloadValue(payload []byte, rules []mutateActiveRule, out mutateByteWriter) error {
	if len(rules) == 0 {
		_, err := out.Write(payload)
		return err
	}
	if len(payload) == 0 {
		return io.ErrUnexpectedEOF
	}
	prev := s.reader
	defer func() { s.reader = prev }()
	s.payloadSrc.Reset(payload)
	if s.payloadReader == nil {
		s.payloadReader = bufio.NewReaderSize(&s.payloadSrc, 64*1024)
	} else {
		s.payloadReader.Reset(&s.payloadSrc)
	}
	s.payloadReaderBuf.Reset(s.ctx, s.payloadReader)
	s.reader = &s.payloadReaderBuf
	start, err := readNonSpaceByte(s.reader)
	if err != nil {
		return err
	}
	if err := s.mutateSourceValue(start, rules, out); err != nil {
		return err
	}
	_, err = readNonSpaceByte(s.reader)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	return io.ErrUnexpectedEOF
}

func (s *mutateStreamState) mutateFileValue(value *mutationFileValue, rules []mutateActiveRule, out mutateByteWriter) error {
	if len(rules) == 0 {
		return s.emitFileMutationValue(value, out)
	}
	if hasCreateNonWildcardRule(s.compiled, rules) {
		return s.emitMissingValue(rules, out)
	}
	return s.emitFileMutationValue(value, out)
}

func (s *mutateStreamState) emitFileMutationValue(value *mutationFileValue, out mutateByteWriter) error {
	if value == nil {
		return fmt.Errorf("file-backed mutation value is nil")
	}
	mode := value.mode
	if mode == mutationFileValueModeAuto {
		textlike, err := s.inspectFileMutationTextlike(value)
		if err != nil {
			return err
		}
		if textlike {
			mode = mutationFileValueModeText
		} else {
			mode = mutationFileValueModeBase64
		}
	}
	switch mode {
	case mutationFileValueModeText:
		return s.emitTextFileMutationValue(value, out)
	case mutationFileValueModeBase64:
		return s.emitBase64FileMutationValue(value, out)
	default:
		return fmt.Errorf("unknown file-backed mutation mode")
	}
}

func (s *mutateStreamState) inspectFileMutationTextlike(value *mutationFileValue) (bool, error) {
	rc, err := value.open()
	if err != nil {
		return false, err
	}
	defer rc.Close()

	reader := s.fileMutationReader(rc)
	for {
		r, size, err := reader.ReadRune()
		if err == io.EOF {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if r == 0 {
			return false, nil
		}
		if r == utf8.RuneError && size == 1 {
			return false, nil
		}
	}
}

func (s *mutateStreamState) emitTextFileMutationValue(value *mutationFileValue, out mutateByteWriter) error {
	rc, err := value.open()
	if err != nil {
		return err
	}
	defer rc.Close()

	if err := out.WriteByte('"'); err != nil {
		return err
	}
	reader := s.fileMutationReader(rc)
	for {
		r, size, err := reader.ReadRune()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if r == 0 {
			return fmt.Errorf("file-backed text mutation %q contains NUL byte", value.path)
		}
		if r == utf8.RuneError && size == 1 {
			return fmt.Errorf("file-backed text mutation %q contains invalid UTF-8", value.path)
		}
		s.scratchBuf = s.scratchBuf[:0]
		switch r {
		case '"', '\\':
			s.scratchBuf = append(s.scratchBuf, '\\', byte(r))
		case '\b':
			s.scratchBuf = append(s.scratchBuf, '\\', 'b')
		case '\f':
			s.scratchBuf = append(s.scratchBuf, '\\', 'f')
		case '\n':
			s.scratchBuf = append(s.scratchBuf, '\\', 'n')
		case '\r':
			s.scratchBuf = append(s.scratchBuf, '\\', 'r')
		case '\t':
			s.scratchBuf = append(s.scratchBuf, '\\', 't')
		default:
			if r < 0x20 || r == '\u2028' || r == '\u2029' {
				s.scratchBuf = appendUnicodeEscape(s.scratchBuf, r)
			} else {
				s.scratchBuf = utf8.AppendRune(s.scratchBuf, r)
			}
		}
		if _, err := out.Write(s.scratchBuf); err != nil {
			return err
		}
	}
	return out.WriteByte('"')
}

func (s *mutateStreamState) emitBase64FileMutationValue(value *mutationFileValue, out mutateByteWriter) error {
	rc, err := value.open()
	if err != nil {
		return err
	}
	defer rc.Close()

	if err := out.WriteByte('"'); err != nil {
		return err
	}
	readBuf, encodeBuf := s.fileMutationBase64Buffers()
	carryLen := 0
	for {
		n, readErr := rc.Read(readBuf[carryLen:cap(readBuf)])
		if n > 0 {
			total := carryLen + n
			encodeLen := total - (total % 3)
			if encodeLen > 0 {
				encodedLen := base64.StdEncoding.EncodedLen(encodeLen)
				base64.StdEncoding.Encode(encodeBuf[:encodedLen], readBuf[:encodeLen])
				if _, err := out.Write(encodeBuf[:encodedLen]); err != nil {
					return err
				}
			}
			carryLen = total - encodeLen
			if carryLen > 0 {
				copy(readBuf[:carryLen], readBuf[encodeLen:total])
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if carryLen > 0 {
		encodedLen := base64.StdEncoding.EncodedLen(carryLen)
		base64.StdEncoding.Encode(encodeBuf[:encodedLen], readBuf[:carryLen])
		if _, err := out.Write(encodeBuf[:encodedLen]); err != nil {
			return err
		}
	}
	return out.WriteByte('"')
}

func (s *mutateStreamState) fileMutationReader(r io.Reader) *bufio.Reader {
	if s.fileReader == nil {
		s.fileReader = bufio.NewReaderSize(r, 64*1024)
		return s.fileReader
	}
	s.fileReader.Reset(r)
	return s.fileReader
}

func (s *mutateStreamState) fileMutationBase64Buffers() ([]byte, []byte) {
	if cap(s.fileReadBuf) < 64*1024+2 {
		s.fileReadBuf = make([]byte, 64*1024+2)
	} else {
		s.fileReadBuf = s.fileReadBuf[:64*1024+2]
	}
	needEncoded := base64.StdEncoding.EncodedLen(cap(s.fileReadBuf))
	if cap(s.base64Buf) < needEncoded {
		s.base64Buf = make([]byte, needEncoded)
	} else {
		s.base64Buf = s.base64Buf[:needEncoded]
	}
	return s.fileReadBuf, s.base64Buf
}

func (s *mutateStreamState) mutateObjectValue(rules []mutateActiveRule, out mutateByteWriter) error {
	if err := out.WriteByte('{'); err != nil {
		return err
	}
	next, err := readNonSpaceByte(s.reader)
	if err != nil {
		return err
	}
	if next == '}' {
		if err := s.appendMissingObjectMembers(rules, nil, nil, true, out); err != nil {
			return err
		}
		return out.WriteByte('}')
	}

	first := true
	var potentialStack [8]string
	potentialAdds := potentialStack[:0]
	for _, rule := range rules {
		mut := s.compiled[rule.idx]
		if mut.wildcard || mut.mut.Kind == MutationRemove || rule.pos >= len(mut.tokens) {
			continue
		}
		token := mut.tokens[rule.pos]
		if token.mode != streamPathTokenLiteral {
			continue
		}
		if _, exists := potentialIndex(potentialAdds, token.raw); exists {
			continue
		}
		if len(potentialAdds) == cap(potentialAdds) {
			next := make([]string, len(potentialAdds), len(potentialAdds)*2)
			copy(next, potentialAdds)
			potentialAdds = next
		}
		potentialAdds = append(potentialAdds, token.raw)
	}
	var seen []bool
	var seenStack [8]bool
	if len(potentialAdds) > 0 {
		if len(potentialAdds) <= len(seenStack) {
			seen = seenStack[:len(potentialAdds)]
		} else {
			seen = make([]bool, len(potentialAdds))
		}
	}
	for {
		if next != '"' {
			return fmt.Errorf("expected string object key")
		}
		key, err := s.readString(s.reader)
		if err != nil {
			return err
		}
		keyStr := bytesToStringUnsafe(key)
		if seen != nil {
			if idx, track := potentialIndex(potentialAdds, keyStr); track {
				seen[idx] = true
			}
		}

		colon, err := readNonSpaceByte(s.reader)
		if err != nil {
			return err
		}
		if colon != ':' {
			return fmt.Errorf("expected ':' after object key")
		}
		valueStart, err := readNonSpaceByte(s.reader)
		if err != nil {
			return err
		}

		var childRulesStack [16]mutateActiveRule
		childRules := s.advanceRules(childRulesStack[:0], rules, true, key, 0)
		emit := simulateMutationExistence(s.compiled, childRules, true)
		if emit {
			if !first {
				if err := out.WriteByte(','); err != nil {
					return err
				}
			}
			if err := s.writeJSONString(out, key); err != nil {
				return err
			}
			if err := out.WriteByte(':'); err != nil {
				return err
			}
			if err := s.mutateValue(valueStart, childRules, out); err != nil {
				return err
			}
			first = false
		} else {
			if err := s.mutateValue(valueStart, childRules, mutateDiscardWriter{}); err != nil {
				return err
			}
		}

		next, err = readNonSpaceByte(s.reader)
		if err != nil {
			return err
		}
		switch next {
		case ',':
			next, err = readNonSpaceByte(s.reader)
			if err != nil {
				return err
			}
		case '}':
			needMissing := len(potentialAdds) == 0
			if !needMissing {
				for _, seenKey := range seen {
					if !seenKey {
						needMissing = true
						break
					}
				}
			}
			if needMissing {
				if err := s.appendMissingObjectMembers(rules, potentialAdds, seen, first, out); err != nil {
					return err
				}
			}
			return out.WriteByte('}')
		default:
			return fmt.Errorf("expected ',' or '}' in object, got %q", next)
		}
	}
}

func potentialIndex(keys []string, target string) (int, bool) {
	for idx, key := range keys {
		if key == target {
			return idx, true
		}
	}
	return -1, false
}

func (s *mutateStreamState) appendMissingObjectMembers(rules []mutateActiveRule, potential []string, seen []bool, first bool, out mutateByteWriter) error {
	if !hasCreateNonWildcardRule(s.compiled, rules) {
		return nil
	}
	cached, err := s.missingValueForRules(rules)
	if err != nil {
		return err
	}
	if !cached.hasValue || len(cached.members) == 0 {
		return nil
	}
	for _, member := range cached.members {
		key := member.key
		if len(seen) > 0 {
			if idx, track := potentialIndex(potential, key); track && seen[idx] {
				continue
			}
		}
		if !first {
			if err := out.WriteByte(','); err != nil {
				return err
			}
		}
		if err := s.writeJSONStringString(out, key); err != nil {
			return err
		}
		if err := out.WriteByte(':'); err != nil {
			return err
		}
		if err := s.emitMissingNodeValue(member.value, out); err != nil {
			return err
		}
		first = false
	}
	return nil
}

func (s *mutateStreamState) mutateArrayValue(rules []mutateActiveRule, out mutateByteWriter) error {
	if err := out.WriteByte('['); err != nil {
		return err
	}
	next, err := readNonSpaceByte(s.reader)
	if err != nil {
		return err
	}
	if next == ']' {
		return out.WriteByte(']')
	}
	first := true
	index := 0
	for {
		if !first {
			if err := out.WriteByte(','); err != nil {
				return err
			}
		}
		var childRulesStack [16]mutateActiveRule
		childRules := s.advanceRules(childRulesStack[:0], rules, false, nil, index)
		emit := simulateMutationExistence(s.compiled, childRules, true)
		if emit {
			if err := s.mutateValue(next, childRules, out); err != nil {
				return err
			}
		} else {
			if err := s.mutateValue(next, childRules, mutateDiscardWriter{}); err != nil {
				return err
			}
			if _, err := out.Write(jsonNullLiteral); err != nil {
				return err
			}
		}
		index++
		first = false

		next, err = readNonSpaceByte(s.reader)
		if err != nil {
			return err
		}
		switch next {
		case ',':
			next, err = readNonSpaceByte(s.reader)
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

func (s *mutateStreamState) emitMissingValue(rules []mutateActiveRule, out mutateByteWriter) error {
	cached, err := s.missingValueForRules(rules)
	if err != nil {
		return err
	}
	if !cached.hasValue || cached.root == nil {
		return nil
	}
	return s.emitMissingNodeValue(cached.root, out)
}

func (s *mutateStreamState) emitMissingNodeValue(node *mutateMissingNode, out mutateByteWriter) error {
	if node == nil {
		return nil
	}
	switch node.kind {
	case mutateMissingNodeLiteral:
		_, err := out.Write(node.literal)
		return err
	case mutateMissingNodeFile:
		return s.emitFileMutationValue(node.file, out)
	case mutateMissingNodeObject:
		if err := out.WriteByte('{'); err != nil {
			return err
		}
		for idx, member := range node.members {
			if idx > 0 {
				if err := out.WriteByte(','); err != nil {
					return err
				}
			}
			if err := s.writeJSONStringString(out, member.key); err != nil {
				return err
			}
			if err := out.WriteByte(':'); err != nil {
				return err
			}
			if err := s.emitMissingNodeValue(member.value, out); err != nil {
				return err
			}
		}
		return out.WriteByte('}')
	default:
		return fmt.Errorf("unknown missing node kind")
	}
}

func hasCreateNonWildcardRule(compiled []compiledStreamMutation, rules []mutateActiveRule) bool {
	for _, rule := range rules {
		mut := compiled[rule.idx]
		if mut.wildcard {
			continue
		}
		if mut.mut.Kind == MutationRemove {
			continue
		}
		if rule.pos >= len(mut.tokens) {
			continue
		}
		return true
	}
	return false
}

func simulateMutationExistence(compiled []compiledStreamMutation, rules []mutateActiveRule, exists bool) bool {
	if len(rules) == 0 {
		return exists
	}

	applyGroup := func(idx int, hasDirect bool) {
		if idx < 0 {
			return
		}
		mut := compiled[idx]
		if hasDirect {
			switch mut.mut.Kind {
			case MutationRemove:
				exists = false
			case MutationSet, MutationIncrement:
				exists = true
			}
			return
		}
		if !exists && !mut.wildcard && mut.mut.Kind != MutationRemove {
			exists = true
		}
	}

	currentIdx := -1
	hasDirect := false
	for _, rule := range rules {
		if rule.idx != currentIdx {
			applyGroup(currentIdx, hasDirect)
			currentIdx = rule.idx
			hasDirect = false
		}
		if rule.pos >= len(compiled[rule.idx].tokens) {
			hasDirect = true
		}
	}
	applyGroup(currentIdx, hasDirect)
	return exists
}

func (s *mutateStreamState) buildMissingObjectNode(rules []mutateActiveRule) (*mutateMissingNode, error) {
	if len(rules) == 0 {
		return nil, nil
	}
	type statePos struct {
		idx int
		pos int
	}
	unique := make(map[statePos]struct{}, len(rules))
	ordered := make([]statePos, 0, len(rules))
	for _, rule := range rules {
		key := statePos(rule)
		if _, exists := unique[key]; exists {
			continue
		}
		unique[key] = struct{}{}
		ordered = append(ordered, key)
	}

	relative := make([]Mutation, 0, len(ordered))
	for _, key := range ordered {
		compiled := s.compiled[key.idx]
		if key.pos >= len(compiled.tokens) {
			continue
		}
		path := make([]string, 0, len(compiled.tokens)-key.pos)
		for _, tok := range compiled.tokens[key.pos:] {
			switch tok.mode {
			case streamPathTokenObjectWildcard:
				path = append(path, "*")
			case streamPathTokenArrayWildcard:
				path = append(path, "[]")
			case streamPathTokenAnyChild:
				path = append(path, "**")
			case streamPathTokenRecursive:
				path = append(path, "...")
			case streamPathTokenLiteral:
				path = append(path, tok.raw)
			default:
				return nil, fmt.Errorf("unknown mutation token mode")
			}
		}
		relative = append(relative, Mutation{
			Path:      path,
			Kind:      compiled.mut.Kind,
			Value:     compiled.mut.Value,
			Delta:     compiled.mut.Delta,
			fileValue: compiled.mut.fileValue,
		})
	}
	if len(relative) == 0 {
		return nil, nil
	}
	doc := make(map[string]any)
	if err := applyMissingMutationsToDoc(doc, relative); err != nil {
		return nil, err
	}
	if len(doc) == 0 {
		return nil, nil
	}
	root, err := missingNodeFromAny(doc)
	if err != nil {
		return nil, err
	}
	if root == nil {
		return nil, nil
	}
	if root.kind != mutateMissingNodeObject {
		return nil, fmt.Errorf("missing object root must be object")
	}
	return root, nil
}

func applyMissingMutationsToDoc(doc map[string]any, muts []Mutation) error {
	for _, mut := range muts {
		internal := mut
		if mut.hasFileValue() {
			internal.Value = &mutateMissingFileMarker{file: mut.fileValue}
			internal.fileValue = nil
		}
		if err := applyMutation(doc, internal); err != nil {
			return err
		}
	}
	return nil
}

func missingNodeFromAny(value any) (*mutateMissingNode, error) {
	switch v := value.(type) {
	case map[string]any:
		node := &mutateMissingNode{kind: mutateMissingNodeObject}
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		node.members = make([]mutateMissingMember, 0, len(keys))
		for _, key := range keys {
			child, err := missingNodeFromAny(v[key])
			if err != nil {
				return nil, err
			}
			node.members = append(node.members, mutateMissingMember{key: key, value: child})
		}
		return node, nil
	case *mutateMissingFileMarker:
		return &mutateMissingNode{kind: mutateMissingNodeFile, file: v.file}, nil
	default:
		payload, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return &mutateMissingNode{kind: mutateMissingNodeLiteral, literal: payload}, nil
	}
}

func activeRulesEqual(a, b []mutateActiveRule) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *mutateStreamState) missingValueForRules(rules []mutateActiveRule) (*mutateMissingCacheEntry, error) {
	for i := range s.missingCache {
		if activeRulesEqual(s.missingCache[i].rules, rules) {
			return &s.missingCache[i], nil
		}
	}

	root, err := s.buildMissingObjectNode(rules)
	if err != nil {
		return nil, err
	}

	entry := mutateMissingCacheEntry{}
	if len(rules) > 0 {
		entry.rules = make([]mutateActiveRule, len(rules))
		copy(entry.rules, rules)
	}
	if root == nil || len(root.members) == 0 {
		entry.hasValue = false
		s.missingCache = append(s.missingCache, entry)
		return &s.missingCache[len(s.missingCache)-1], nil
	}
	entry.hasValue = true
	entry.root = root
	entry.members = make([]mutateMissingMember, len(root.members))
	copy(entry.members, root.members)
	s.missingCache = append(s.missingCache, entry)
	return &s.missingCache[len(s.missingCache)-1], nil
}

func (s *mutateStreamState) advanceRules(dst []mutateActiveRule, rules []mutateActiveRule, objectSegment bool, key []byte, index int) []mutateActiveRule {
	if len(rules) == 0 {
		return nil
	}
	out := dst[:0]
	for _, rule := range rules {
		compiled := s.compiled[rule.idx]
		if rule.pos >= len(compiled.tokens) {
			continue
		}
		var posStack [8]int
		positions := advanceMutationPositions(compiled.tokens, rule.pos, objectSegment, key, index, posStack[:0])
		for _, pos := range positions {
			next := mutateActiveRule{idx: rule.idx, pos: pos}
			if containsActiveRule(out, next) {
				continue
			}
			out = append(out, next)
		}
	}
	return out
}

func containsActiveRule(rules []mutateActiveRule, target mutateActiveRule) bool {
	for _, rule := range rules {
		if rule == target {
			return true
		}
	}
	return false
}

func advanceMutationPositions(tokens []streamPathToken, pos int, objectSegment bool, key []byte, index int, out []int) []int {
	if pos >= len(tokens) {
		return out
	}
	token := tokens[pos]
	switch token.mode {
	case streamPathTokenRecursive:
		out = append(out, pos)
		if pos == len(tokens)-1 {
			out = append(out, len(tokens))
			return out
		}
		return advanceMutationPositions(tokens, pos+1, objectSegment, key, index, out)
	case streamPathTokenObjectWildcard:
		if objectSegment {
			return append(out, pos+1)
		}
	case streamPathTokenArrayWildcard:
		if !objectSegment {
			return append(out, pos+1)
		}
	case streamPathTokenAnyChild:
		return append(out, pos+1)
	case streamPathTokenLiteral:
		if objectSegment {
			if streamPathLiteralMatchesKey(token, key) {
				return append(out, pos+1)
			}
			return out
		}
		if token.arrayOnly && token.index == index {
			return append(out, pos+1)
		}
		return out
	}
	return out
}

func (s *mutateStreamState) consumeNumericSourceValue(start byte) (float64, bool, error) {
	if !isNumberStart(start) {
		if err := s.skipValue(start); err != nil {
			return 0, false, err
		}
		return 0, false, nil
	}
	number, err := s.readNumber(s.reader, start)
	if err != nil {
		return 0, false, err
	}
	f, err := json.Number(bytesToStringUnsafe(number)).Float64()
	if err != nil {
		return 0, false, nil
	}
	return f, true, nil
}

func parseJSONNumericPayload(payload []byte) (float64, bool, error) {
	value, err := decodeJSONAnyStream(payload)
	if err != nil {
		return 0, false, err
	}
	f, ok := toFloat(value)
	return f, ok, nil
}

func normalizeNumberState(f float64) (bool, int64, float64) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return false, 0, f
	}
	rounded := math.Round(f)
	if math.Abs(f-rounded) < 1e-9 {
		return true, int64(rounded), 0
	}
	return false, 0, f
}

func (s *mutateStreamState) skipValue(start byte) error {
	return s.writeValue(start, mutateDiscardWriter{})
}

func (s *mutateStreamState) writeValue(start byte, out mutateByteWriter) error {
	switch start {
	case '{':
		if err := out.WriteByte('{'); err != nil {
			return err
		}
		return s.writeObject(out)
	case '[':
		if err := out.WriteByte('['); err != nil {
			return err
		}
		return s.writeArray(out)
	case '"':
		return s.copyRawJSONString(s.reader, out)
	case 't':
		if err := expectLiteral(s.reader, "rue"); err != nil {
			return err
		}
		_, err := out.Write(jsonTrueLiteral)
		return err
	case 'f':
		if err := expectLiteral(s.reader, "alse"); err != nil {
			return err
		}
		_, err := out.Write(jsonFalseLiteral)
		return err
	case 'n':
		if err := expectLiteral(s.reader, "ull"); err != nil {
			return err
		}
		_, err := out.Write(jsonNullLiteral)
		return err
	default:
		if !isNumberStart(start) {
			return fmt.Errorf("unexpected value start %q", start)
		}
		number, err := s.readNumber(s.reader, start)
		if err != nil {
			return err
		}
		_, err = out.Write(number)
		return err
	}
}

func (s *mutateStreamState) writeObject(out mutateByteWriter) error {
	next, err := readNonSpaceByte(s.reader)
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
		key, err := s.readString(s.reader)
		if err != nil {
			return err
		}
		if !first {
			if err := out.WriteByte(','); err != nil {
				return err
			}
		}
		if err := s.writeJSONString(out, key); err != nil {
			return err
		}
		if err := out.WriteByte(':'); err != nil {
			return err
		}

		colon, err := readNonSpaceByte(s.reader)
		if err != nil {
			return err
		}
		if colon != ':' {
			return fmt.Errorf("expected ':' after object key")
		}
		valueStart, err := readNonSpaceByte(s.reader)
		if err != nil {
			return err
		}
		if err := s.writeValue(valueStart, out); err != nil {
			return err
		}

		next, err = readNonSpaceByte(s.reader)
		if err != nil {
			return err
		}
		switch next {
		case ',':
			next, err = readNonSpaceByte(s.reader)
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

func (s *mutateStreamState) writeArray(out mutateByteWriter) error {
	next, err := readNonSpaceByte(s.reader)
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
		if err := s.writeValue(next, out); err != nil {
			return err
		}
		first = false

		next, err = readNonSpaceByte(s.reader)
		if err != nil {
			return err
		}
		switch next {
		case ',':
			next, err = readNonSpaceByte(s.reader)
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

func (s *mutateStreamState) writeJSONString(out mutateByteWriter, value []byte) error {
	s.scratchBuf = appendJSONString(s.scratchBuf[:0], value)
	_, err := out.Write(s.scratchBuf)
	return err
}

func (s *mutateStreamState) writeJSONStringString(out mutateByteWriter, value string) error {
	s.scratchBuf = appendJSONStringString(s.scratchBuf[:0], value)
	_, err := out.Write(s.scratchBuf)
	return err
}

func (s *mutateStreamState) readString(reader *streamByteReader) ([]byte, error) {
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

func (s *mutateStreamState) copyRawJSONString(reader *streamByteReader, out mutateByteWriter) error {
	if err := out.WriteByte('"'); err != nil {
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
				if _, err := out.Write(chunk[:prefix]); err != nil {
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
			return out.WriteByte('"')
		case '\\':
			if err := out.WriteByte('\\'); err != nil {
				return err
			}
			escaped, err := reader.ReadByte()
			if err != nil {
				return err
			}
			if err := out.WriteByte(escaped); err != nil {
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
					if err := out.WriteByte(h); err != nil {
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

func (s *mutateStreamState) readNumber(reader *streamByteReader, first byte) ([]byte, error) {
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

func (s *mutateStreamState) readFractionDigits(reader *streamByteReader) error {
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

func (s *mutateStreamState) readExponentDigits(reader *streamByteReader) error {
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

func (s *mutateStreamState) wrapStreamError(err error, offset int64) error {
	if err == nil {
		return nil
	}
	var streamErr *StreamError
	if errors.As(err, &streamErr) {
		return streamErr
	}
	var tooLarge *mutateCandidateTooLargeError
	if errors.As(err, &tooLarge) {
		return &StreamError{
			Code:   StreamErrorDocumentTooLarge,
			Detail: fmt.Sprintf("candidate exceeds max bytes (%d > %d)", tooLarge.size, tooLarge.max),
			Offset: offset,
			Err:    err,
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &StreamError{Code: StreamErrorContextCanceled, Detail: "context canceled", Offset: offset, Err: err}
	}
	return &StreamError{Code: StreamErrorInvalidBody, Detail: "invalid json stream", Offset: offset, Err: err}
}

type mutateByteWriter interface {
	Write([]byte) (int, error)
	WriteByte(byte) error
}

type mutateSliceWriter struct {
	buf []byte
}

func (w *mutateSliceWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *mutateSliceWriter) WriteByte(b byte) error {
	w.buf = append(w.buf, b)
	return nil
}

type mutateCandidateTooLargeError struct {
	size int64
	max  int64
}

func (e *mutateCandidateTooLargeError) Error() string {
	if e == nil {
		return "candidate too large"
	}
	return fmt.Sprintf("candidate exceeds max bytes (%d > %d)", e.size, e.max)
}

type mutateCandidateSink struct {
	writer      io.Writer
	capture     bool
	size        int64
	max         int64
	oneByte     [1]byte
	spoolCfg    streamSpoolConfig
	spool       streamCandidateSpool
	spoolInit   bool
	payloadSink MutateStreamPayloadSink
	internal    bool
}

type mutateDiscardWriter struct{}

func (mutateDiscardWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (mutateDiscardWriter) WriteByte(byte) error {
	return nil
}

func (s *mutateCandidateSink) reset(
	writer io.Writer,
	capture bool,
	max int64,
	hint int,
	cfg streamSpoolConfig,
	disableInternal bool,
	payloadSinkFactory MutateStreamPayloadSinkFactory,
	offset int64,
) error {
	s.writer = writer
	s.capture = capture
	s.max = max
	s.size = 0
	s.payloadSink = nil
	s.internal = false
	if s.capture {
		if payloadSinkFactory != nil {
			payload, err := payloadSinkFactory(MutateStreamPayloadSinkRequest{Offset: offset})
			if err != nil {
				return err
			}
			if payload == nil {
				return fmt.Errorf("mutate payload sink factory returned nil")
			}
			s.payloadSink = payload
			return nil
		}
		if disableInternal {
			return fmt.Errorf("mutate payload sink required when internal spool is disabled")
		}
		s.spoolCfg = cfg
		if !s.spoolInit {
			s.spool = streamCandidateSpool{
				cfg: cfg,
				mem: acquireSpoolMem(cfg, hint),
			}
			s.spoolInit = true
		} else {
			s.spool.resetForCandidate(cfg, hint)
		}
		s.internal = true
		return nil
	}
	return nil
}

func (s *mutateCandidateSink) payload() []byte {
	if !s.capture {
		return nil
	}
	if s.internal {
		if !s.spoolInit {
			return nil
		}
		return s.spool.PayloadBytes()
	}
	if s.payloadSink == nil {
		return nil
	}
	return s.payloadSink.Bytes()
}

func (s *mutateCandidateSink) openJSON() (io.ReadCloser, error) {
	if !s.capture {
		return nil, fmt.Errorf("mutate stream candidate payload unavailable")
	}
	if s.internal {
		if !s.spoolInit {
			return nil, fmt.Errorf("mutate stream candidate payload unavailable")
		}
		return s.spool.Open()
	}
	if s.payloadSink == nil {
		return nil, fmt.Errorf("mutate stream candidate payload unavailable")
	}
	return s.payloadSink.Open()
}

func (s *mutateCandidateSink) finalizeCapture() error {
	if !s.capture {
		return nil
	}
	if s.internal {
		if !s.spoolInit {
			return nil
		}
		return s.spool.Finalize()
	}
	if s.payloadSink == nil {
		return nil
	}
	return s.payloadSink.Finalize()
}

func (s *mutateCandidateSink) sizeHint() int {
	if !s.capture {
		return 0
	}
	if s.internal {
		if !s.spoolInit {
			return 0
		}
		return s.spool.SizeHint()
	}
	if s.payloadSink == nil {
		return 0
	}
	return s.payloadSink.SizeHint()
}

func (s *mutateCandidateSink) capturedBytes() int64 {
	if !s.capture {
		return 0
	}
	if s.internal {
		if !s.spoolInit {
			return 0
		}
		return s.spool.Size()
	}
	if stats, ok := s.payloadSink.(interface{ CapturedBytes() int64 }); ok {
		return stats.CapturedBytes()
	}
	return 0
}

func (s *mutateCandidateSink) spillCount() int64 {
	if !s.capture {
		return 0
	}
	if s.internal {
		if !s.spoolInit {
			return 0
		}
		return s.spool.SpillCount()
	}
	if stats, ok := s.payloadSink.(interface{ SpillCount() int64 }); ok {
		return stats.SpillCount()
	}
	return 0
}

func (s *mutateCandidateSink) spillBytes() int64 {
	if !s.capture {
		return 0
	}
	if s.internal {
		if !s.spoolInit {
			return 0
		}
		return s.spool.SpillBytes()
	}
	if stats, ok := s.payloadSink.(interface{ SpillBytes() int64 }); ok {
		return stats.SpillBytes()
	}
	return 0
}

func (s *mutateCandidateSink) cleanupCandidate() error {
	var firstErr error
	if s.capture {
		if s.internal && s.spoolInit {
			if err := s.spool.cleanup(false); err != nil {
				firstErr = err
			}
		}
		if !s.internal && s.payloadSink != nil {
			if err := s.payloadSink.Cleanup(); err != nil {
				firstErr = err
			}
		}
	}
	s.writer = nil
	s.capture = false
	s.size = 0
	s.max = 0
	s.payloadSink = nil
	s.internal = false
	return firstErr
}

func (s *mutateCandidateSink) releaseAll() error {
	var firstErr error
	if s.spoolInit {
		if err := s.spool.Cleanup(); err != nil {
			firstErr = err
		}
		s.spoolInit = false
	}
	s.writer = nil
	s.capture = false
	s.size = 0
	s.max = 0
	s.payloadSink = nil
	s.internal = false
	return firstErr
}

func (s *mutateCandidateSink) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	next := s.size + int64(len(p))
	if s.max > 0 && next > s.max {
		return 0, &mutateCandidateTooLargeError{size: next, max: s.max}
	}
	if s.capture {
		var (
			n   int
			err error
		)
		if s.internal {
			n, err = s.spool.Write(p)
		} else if s.payloadSink != nil {
			n, err = s.payloadSink.Write(p)
		} else {
			return 0, fmt.Errorf("mutate stream payload sink unavailable")
		}
		if err != nil {
			return n, err
		}
		if n != len(p) {
			return n, io.ErrShortWrite
		}
	}
	if s.writer != nil {
		n, err := s.writer.Write(p)
		if err != nil {
			s.size += int64(n)
		}
		if err != nil {
			return n, err
		}
		if n != len(p) {
			return n, io.ErrShortWrite
		}
		s.size = next
		return len(p), nil
	}
	s.size = next
	return len(p), nil
}

func (s *mutateCandidateSink) WriteByte(b byte) error {
	s.oneByte[0] = b
	_, err := s.Write(s.oneByte[:])
	return err
}

func decodeJSONAnyStream(payload []byte) (any, error) {
	var value any
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	err := dec.Decode(&trailing)
	if err == io.EOF {
		return value, nil
	}
	if err != nil {
		return nil, err
	}
	return nil, io.ErrUnexpectedEOF
}
