package lql

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
)

const reusableQueryPayloadSinkWriteBufSize = 64 * 1024

// ReusableQueryPayloadSinkFactoryOptions configures reusable caller-managed
// payload sinks for QueryStream.
type ReusableQueryPayloadSinkFactoryOptions struct {
	SpoolMemoryBytes int64
	SpoolTempDir     string
	SpoolFilePattern string
}

// ReusableQueryPayloadSinkFactory provides a caller-managed QueryStream
// payload sink factory that reuses in-memory buffers and spill files across
// candidates to reduce allocation and filesystem churn.
//
// Call Close when the factory is no longer needed to release temp files and
// pooled buffers.
type ReusableQueryPayloadSinkFactory struct {
	cfg streamSpoolConfig

	mu     sync.Mutex
	closed bool
	pool   []*reusableQueryPayloadSink
	all    []*reusableQueryPayloadSink
}

// NewReusableQueryPayloadSinkFactory builds a reusable caller-managed sink
// factory for QueryStreamRequest.PayloadSinkFactory.
func NewReusableQueryPayloadSinkFactory(opts ReusableQueryPayloadSinkFactoryOptions) *ReusableQueryPayloadSinkFactory {
	return &ReusableQueryPayloadSinkFactory{
		cfg: normalizeStreamSpoolConfig(opts.SpoolMemoryBytes, opts.SpoolTempDir, opts.SpoolFilePattern),
	}
}

// Factory returns a QueryStream payload sink factory function.
func (f *ReusableQueryPayloadSinkFactory) Factory() QueryStreamPayloadSinkFactory {
	return f.NewSink
}

// NewSink implements QueryStreamPayloadSinkFactory.
func (f *ReusableQueryPayloadSinkFactory) NewSink(QueryStreamPayloadSinkRequest) (QueryStreamPayloadSink, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, fmt.Errorf("reusable query payload sink factory is closed")
	}

	n := len(f.pool)
	if n > 0 {
		sink := f.pool[n-1]
		f.pool = f.pool[:n-1]
		sink.resetForCandidate()
		return sink, nil
	}

	sink := &reusableQueryPayloadSink{
		factory: f,
		cfg:     f.cfg,
		mem:     acquireSpoolMem(f.cfg, defaultStreamSpoolInitialMem),
	}
	f.all = append(f.all, sink)
	return sink, nil
}

// Close releases all reusable sink resources including any temp files.
func (f *ReusableQueryPayloadSinkFactory) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	sinks := f.all
	f.pool = nil
	f.all = nil
	f.mu.Unlock()

	var firstErr error
	for i := range sinks {
		if err := sinks[i].closeResources(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (f *ReusableQueryPayloadSinkFactory) releaseSink(sink *reusableQueryPayloadSink) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return sink.closeResources()
	}
	f.pool = append(f.pool, sink)
	return nil
}

type reusableQueryPayloadSink struct {
	factory *ReusableQueryPayloadSinkFactory
	cfg     streamSpoolConfig

	mem      []byte
	file     *os.File
	fileBuf  *bufio.Writer
	usingTmp bool
	size     int64
}

func (s *reusableQueryPayloadSink) resetForCandidate() {
	s.mem = s.mem[:0]
	s.usingTmp = false
	s.size = 0
}

func (s *reusableQueryPayloadSink) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if !s.usingTmp {
		next := int64(len(s.mem) + len(p))
		if next <= s.cfg.memoryBytes {
			s.mem = append(s.mem, p...)
			s.size += int64(len(p))
			return len(p), nil
		}
		if err := s.switchToTempFile(); err != nil {
			return 0, err
		}
	}
	n, err := s.fileBuf.Write(p)
	s.size += int64(n)
	if err != nil {
		return n, err
	}
	if n != len(p) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

func (s *reusableQueryPayloadSink) WriteByte(b byte) error {
	if !s.usingTmp {
		next := int64(len(s.mem) + 1)
		if next <= s.cfg.memoryBytes {
			s.mem = append(s.mem, b)
			s.size++
			return nil
		}
		if err := s.switchToTempFile(); err != nil {
			return err
		}
	}
	if err := s.fileBuf.WriteByte(b); err != nil {
		return err
	}
	s.size++
	return nil
}

func (s *reusableQueryPayloadSink) Finalize() error {
	if !s.usingTmp {
		return nil
	}
	return s.fileBuf.Flush()
}

func (s *reusableQueryPayloadSink) Open() (io.ReadCloser, error) {
	if !s.usingTmp {
		return io.NopCloser(bytes.NewReader(s.mem)), nil
	}
	if err := s.Finalize(); err != nil {
		return nil, err
	}
	return os.Open(s.file.Name())
}

func (s *reusableQueryPayloadSink) Bytes() []byte {
	if s.usingTmp {
		return nil
	}
	return s.mem
}

func (s *reusableQueryPayloadSink) SizeHint() int {
	if s.size <= 0 {
		return 0
	}
	if s.size > int64(^uint(0)>>1) {
		return int(^uint(0) >> 1)
	}
	return int(s.size)
}

func (s *reusableQueryPayloadSink) Cleanup() error {
	if s.usingTmp && s.fileBuf != nil {
		if err := s.fileBuf.Flush(); err != nil {
			return err
		}
		if _, err := s.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
	}
	s.resetForCandidate()
	return s.factory.releaseSink(s)
}

func (s *reusableQueryPayloadSink) switchToTempFile() error {
	if s.file == nil {
		file, err := os.CreateTemp(s.cfg.tempDir, s.cfg.filePattern)
		if err != nil {
			return fmt.Errorf("create reusable spool file: %w", err)
		}
		if err := file.Chmod(0o600); err != nil {
			_ = file.Close()
			_ = os.Remove(file.Name())
			return fmt.Errorf("chmod reusable spool file: %w", err)
		}
		s.file = file
		s.fileBuf = bufio.NewWriterSize(file, reusableQueryPayloadSinkWriteBufSize)
	} else {
		if err := s.file.Truncate(0); err != nil {
			return err
		}
		if _, err := s.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		s.fileBuf.Reset(s.file)
	}
	if len(s.mem) > 0 {
		n, err := s.fileBuf.Write(s.mem)
		if err != nil {
			return err
		}
		if n != len(s.mem) {
			return io.ErrShortWrite
		}
		s.mem = s.mem[:0]
	}
	s.usingTmp = true
	return nil
}

func (s *reusableQueryPayloadSink) closeResources() error {
	var firstErr error
	if s.fileBuf != nil {
		if err := s.fileBuf.Flush(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.fileBuf = nil
	}
	if s.file != nil {
		name := s.file.Name()
		if err := s.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := os.Remove(name); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
		s.file = nil
	}
	if s.mem != nil {
		streamJSONBytePool.release(s.mem)
		s.mem = nil
	}
	s.usingTmp = false
	s.size = 0
	return firstErr
}
