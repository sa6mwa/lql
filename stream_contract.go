package lql

import (
	"context"
	"errors"
	"fmt"
)

// QueryStreamMode controls payload behavior for QueryStream callbacks.
type QueryStreamMode uint8

const (
	// QueryModeAuto preserves backward-compatible behavior using IncludeJSON.
	QueryModeAuto QueryStreamMode = iota
	// QueryDecisionOnly reports only match/unmatch state.
	QueryDecisionOnly
	// QueryDecisionPlusValue includes candidate payload access.
	QueryDecisionPlusValue
)

// MutateStreamMode controls top-level framing and root-shape validation.
type MutateStreamMode uint8

const (
	// MutateModeAuto preserves backward-compatible behavior:
	// multiple top-level values are allowed and top-level arrays are flattened.
	MutateModeAuto MutateStreamMode = iota
	// MutateSingleValueOnly requires exactly one top-level JSON value and disables
	// top-level array flattening.
	MutateSingleValueOnly
	// MutateObjectRootOnly requires each top-level value to be a JSON object root.
	// Top-level arrays/scalars/null are rejected.
	MutateObjectRootOnly
	// MutateSingleObjectOnly combines MutateSingleValueOnly and MutateObjectRootOnly.
	MutateSingleObjectOnly
)

// StreamErrorCode is a machine-usable error class for streaming contracts.
type StreamErrorCode string

const (
	// StreamErrorInvalidSelector indicates selector parse/compile incompatibility.
	StreamErrorInvalidSelector StreamErrorCode = "invalid_selector"
	// StreamErrorInvalidBody indicates malformed JSON/body or stream framing input.
	StreamErrorInvalidBody StreamErrorCode = "invalid_body"
	// StreamErrorDocumentTooLarge indicates a configured candidate-size limit was exceeded.
	StreamErrorDocumentTooLarge StreamErrorCode = "document_too_large"
	// StreamErrorContextCanceled indicates request cancellation or timeout.
	StreamErrorContextCanceled StreamErrorCode = "context_canceled"
	// StreamErrorInternal indicates an unexpected internal execution failure.
	StreamErrorInternal StreamErrorCode = "internal"
)

// StreamError is returned by QueryStream and MutateStream.
type StreamError struct {
	Code   StreamErrorCode
	Detail string
	Offset int64
	Path   string
	Err    error
}

func (e *StreamError) Error() string {
	if e == nil {
		return ""
	}
	base := string(e.Code)
	if e.Detail != "" {
		base += ": " + e.Detail
	}
	if e.Path != "" {
		base += " path=" + e.Path
	}
	if e.Offset >= 0 {
		base += fmt.Sprintf(" offset=%d", e.Offset)
	}
	if e.Err != nil {
		base += ": " + e.Err.Error()
	}
	return base
}

func (e *StreamError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func normalizeStreamContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// AsStreamError unwraps err into *StreamError.
func AsStreamError(err error) (*StreamError, bool) {
	if err == nil {
		return nil, false
	}
	var streamErr *StreamError
	if errors.As(err, &streamErr) {
		return streamErr, true
	}
	return nil, false
}

// StreamErrorCodeOf returns StreamError code if err unwraps to *StreamError.
func StreamErrorCodeOf(err error) (StreamErrorCode, bool) {
	streamErr, ok := AsStreamError(err)
	if !ok || streamErr == nil {
		return "", false
	}
	return streamErr.Code, true
}

// IsStreamErrorCode reports whether err unwraps to *StreamError with code.
func IsStreamErrorCode(err error, code StreamErrorCode) bool {
	got, ok := StreamErrorCodeOf(err)
	return ok && got == code
}

// IsStreamInvalidSelector reports whether err is a StreamErrorInvalidSelector.
func IsStreamInvalidSelector(err error) bool {
	return IsStreamErrorCode(err, StreamErrorInvalidSelector)
}

// IsStreamInvalidBody reports whether err is a StreamErrorInvalidBody.
func IsStreamInvalidBody(err error) bool {
	return IsStreamErrorCode(err, StreamErrorInvalidBody)
}

// IsStreamDocumentTooLarge reports whether err is a StreamErrorDocumentTooLarge.
func IsStreamDocumentTooLarge(err error) bool {
	return IsStreamErrorCode(err, StreamErrorDocumentTooLarge)
}

// IsStreamContextCanceled reports whether err is a StreamErrorContextCanceled.
func IsStreamContextCanceled(err error) bool {
	return IsStreamErrorCode(err, StreamErrorContextCanceled)
}

// IsStreamInternal reports whether err is a StreamErrorInternal.
func IsStreamInternal(err error) bool {
	return IsStreamErrorCode(err, StreamErrorInternal)
}
