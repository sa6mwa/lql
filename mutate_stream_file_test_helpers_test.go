package lql

import (
	"bytes"
	"fmt"
	"io"
)

type staticReadCloser struct {
	reader bytes.Reader
}

func (r *staticReadCloser) Reset(payload []byte) {
	r.reader.Reset(payload)
}

func (r *staticReadCloser) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *staticReadCloser) Close() error {
	return nil
}

type nonAllocMutateFileResolver struct {
	payloads map[string][]byte
	readers  map[string]*staticReadCloser
	opens    map[string]int
}

func (r *nonAllocMutateFileResolver) Open(path string) (io.ReadCloser, error) {
	payload, ok := r.payloads[path]
	if !ok {
		return nil, fmt.Errorf("unknown test payload %q", path)
	}
	if r.readers == nil {
		r.readers = make(map[string]*staticReadCloser, len(r.payloads))
	}
	reader, ok := r.readers[path]
	if !ok {
		reader = &staticReadCloser{}
		r.readers[path] = reader
	}
	if r.opens != nil {
		r.opens[path]++
	}
	reader.Reset(payload)
	return reader, nil
}

type failingMutateFileResolver struct {
	err error
}

func (r failingMutateFileResolver) Open(string) (io.ReadCloser, error) {
	return nil, r.err
}

type failingReadCloser struct {
	payload []byte
	offset  int
	failErr error
}

func (r *failingReadCloser) Read(p []byte) (int, error) {
	if r.offset >= len(r.payload) {
		return 0, r.failErr
	}
	n := copy(p, r.payload[r.offset:])
	r.offset += n
	if r.offset >= len(r.payload) {
		return n, r.failErr
	}
	return n, nil
}

func (r *failingReadCloser) Close() error {
	return nil
}

type failingReadMutateFileResolver struct {
	payload []byte
	err     error
	reader  failingReadCloser
}

func (r *failingReadMutateFileResolver) Open(string) (io.ReadCloser, error) {
	r.reader.payload = r.payload
	r.reader.offset = 0
	r.reader.failErr = r.err
	return &r.reader, nil
}
