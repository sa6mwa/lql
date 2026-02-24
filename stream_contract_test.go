package lql

import (
	"errors"
	"testing"
)

func TestStreamErrorHelpers(t *testing.T) {
	base := &StreamError{Code: StreamErrorInvalidBody, Detail: "bad"}
	wrapped := errors.Join(errors.New("outer"), base)

	streamErr, ok := AsStreamError(wrapped)
	if !ok || streamErr == nil || streamErr.Code != StreamErrorInvalidBody {
		t.Fatalf("AsStreamError failed: ok=%v err=%+v", ok, streamErr)
	}

	code, ok := StreamErrorCodeOf(wrapped)
	if !ok || code != StreamErrorInvalidBody {
		t.Fatalf("StreamErrorCodeOf failed: ok=%v code=%s", ok, code)
	}
	if !IsStreamInvalidBody(wrapped) {
		t.Fatalf("expected IsStreamInvalidBody true")
	}
	if IsStreamInvalidSelector(wrapped) || IsStreamDocumentTooLarge(wrapped) || IsStreamContextCanceled(wrapped) || IsStreamInternal(wrapped) {
		t.Fatalf("unexpected helper match for wrong code")
	}

	nonStream := errors.New("x")
	if _, ok := AsStreamError(nonStream); ok {
		t.Fatalf("expected non-stream error to fail AsStreamError")
	}
	if _, ok := StreamErrorCodeOf(nonStream); ok {
		t.Fatalf("expected non-stream error to fail StreamErrorCodeOf")
	}
	if IsStreamInvalidBody(nonStream) {
		t.Fatalf("expected IsStreamInvalidBody false for non-stream error")
	}
}

func TestStreamErrorHelpersAllCodesWrapped(t *testing.T) {
	cases := []struct {
		code  StreamErrorCode
		match func(error) bool
	}{
		{StreamErrorInvalidSelector, IsStreamInvalidSelector},
		{StreamErrorInvalidBody, IsStreamInvalidBody},
		{StreamErrorDocumentTooLarge, IsStreamDocumentTooLarge},
		{StreamErrorContextCanceled, IsStreamContextCanceled},
		{StreamErrorInternal, IsStreamInternal},
	}

	for _, tc := range cases {
		t.Run(string(tc.code), func(t *testing.T) {
			root := &StreamError{Code: tc.code, Detail: "x"}
			wrapped := errors.Join(errors.New("outer"), errors.Join(errors.New("mid"), root))

			code, ok := StreamErrorCodeOf(wrapped)
			if !ok || code != tc.code {
				t.Fatalf("StreamErrorCodeOf mismatch: ok=%v code=%s want=%s", ok, code, tc.code)
			}
			if !IsStreamErrorCode(wrapped, tc.code) {
				t.Fatalf("IsStreamErrorCode expected true")
			}
			if !tc.match(wrapped) {
				t.Fatalf("specific helper expected true for code %s", tc.code)
			}
		})
	}
}

func TestErrStreamStopSentinelSupportsWrapping(t *testing.T) {
	wrapped := errors.Join(errors.New("outer"), ErrStreamStop)
	if !errors.Is(wrapped, ErrStreamStop) {
		t.Fatalf("expected errors.Is match for ErrStreamStop")
	}
}
