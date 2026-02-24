package lql

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	defaultStreamSpoolMemoryBytes = 3 * 1024 * 1024
	defaultStreamSpoolPattern     = "lql-spool-*.json"
	defaultStreamSpoolTempDir     = "/tmp"
	defaultStreamSpoolInitialMem  = 64 * 1024
	streamSpoolJumpThreshold      = 1 * 1024 * 1024
)

type streamSpoolConfig struct {
	memoryBytes int64
	tempDir     string
	filePattern string
}

func normalizeStreamSpoolConfig(memoryBytes int64, tempDir, pattern string) streamSpoolConfig {
	cfg := streamSpoolConfig{
		memoryBytes: memoryBytes,
		tempDir:     tempDir,
		filePattern: pattern,
	}
	if cfg.memoryBytes <= 0 {
		cfg.memoryBytes = defaultStreamSpoolMemoryBytes
	}
	if cfg.tempDir == "" {
		cfg.tempDir = defaultStreamSpoolTempDir
	}
	if cfg.filePattern == "" {
		cfg.filePattern = defaultStreamSpoolPattern
	}
	return cfg
}

type streamCandidateSpool struct {
	cfg        streamSpoolConfig
	mem        []byte
	file       *os.File
	fileBuf    *bufio.Writer
	filePath   string
	spilled    bool
	size       int64
	spillCount int64
	spillBytes int64
	oneByte    [1]byte
}

var streamCandidateSpoolPool sync.Pool

func acquireStreamCandidateSpool(cfg streamSpoolConfig, hint int) *streamCandidateSpool {
	if value := streamCandidateSpoolPool.Get(); value != nil {
		spool := value.(*streamCandidateSpool)
		spool.resetForCandidate(cfg, hint)
		return spool
	}
	return newStreamCandidateSpool(cfg, hint)
}

func releaseStreamCandidateSpool(spool *streamCandidateSpool) {
	if spool == nil {
		return
	}
	streamCandidateSpoolPool.Put(spool)
}

func newStreamCandidateSpool(cfg streamSpoolConfig, hint int) *streamCandidateSpool {
	return &streamCandidateSpool{cfg: cfg, mem: acquireSpoolMem(cfg, hint)}
}

func acquireSpoolMem(cfg streamSpoolConfig, hint int) []byte {
	if hint < 0 {
		hint = 0
	}
	capHint := hint
	if capHint == 0 && cfg.memoryBytes > 0 {
		capHint = defaultStreamSpoolInitialMem
	}
	if cfg.memoryBytes > 0 && int64(capHint) > cfg.memoryBytes {
		capHint = int(cfg.memoryBytes)
	}
	mem := streamJSONBytePool.acquire(capHint)
	return mem[:0]
}

func (s *streamCandidateSpool) resetForCandidate(cfg streamSpoolConfig, hint int) {
	if s.file != nil && (s.cfg.tempDir != cfg.tempDir || s.cfg.filePattern != cfg.filePattern) {
		if s.fileBuf != nil {
			_ = s.fileBuf.Flush()
			s.fileBuf = nil
		}
		_ = s.file.Close()
		if s.filePath != "" {
			_ = os.Remove(s.filePath)
		}
		s.file = nil
		s.filePath = ""
	}
	s.cfg = cfg
	s.spilled = false
	s.size = 0
	s.spillCount = 0
	s.spillBytes = 0
	if s.mem == nil {
		s.mem = acquireSpoolMem(cfg, hint)
		return
	}
	if hint > cap(s.mem) {
		streamJSONBytePool.release(s.mem)
		s.mem = acquireSpoolMem(cfg, hint)
		return
	}
	s.mem = s.mem[:0]
}

func (s *streamCandidateSpool) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if !s.spilled {
		next := int64(len(s.mem) + len(p))
		if next <= s.cfg.memoryBytes {
			if err := s.ensureMemCapacity(int(next)); err != nil {
				return 0, err
			}
			s.mem = append(s.mem, p...)
			s.size += int64(len(p))
			return len(p), nil
		}
		if err := s.spillToDisk(); err != nil {
			return 0, err
		}
	}
	var (
		n   int
		err error
	)
	if s.fileBuf != nil {
		n, err = s.fileBuf.Write(p)
	} else {
		n, err = s.file.Write(p)
	}
	s.size += int64(n)
	s.spillBytes += int64(n)
	if err != nil {
		return n, err
	}
	if n != len(p) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

func (s *streamCandidateSpool) WriteByte(b byte) error {
	if !s.spilled {
		next := int64(len(s.mem) + 1)
		if next <= s.cfg.memoryBytes {
			if err := s.ensureMemCapacity(int(next)); err != nil {
				return err
			}
			s.mem = append(s.mem, b)
			s.size++
			return nil
		}
		if err := s.spillToDisk(); err != nil {
			return err
		}
	}
	if s.fileBuf != nil {
		if err := s.fileBuf.WriteByte(b); err != nil {
			return err
		}
		s.size++
		s.spillBytes++
		return nil
	}
	s.oneByte[0] = b
	n, err := s.file.Write(s.oneByte[:])
	s.size += int64(n)
	s.spillBytes += int64(n)
	if err != nil {
		return err
	}
	if n != 1 {
		return io.ErrShortWrite
	}
	return nil
}

func (s *streamCandidateSpool) spillToDisk() error {
	if s.file == nil {
		file, err := os.CreateTemp(s.cfg.tempDir, s.cfg.filePattern)
		if err != nil {
			return fmt.Errorf("create spool file: %w", err)
		}
		if err := file.Chmod(0o600); err != nil {
			_ = file.Close()
			_ = os.Remove(file.Name())
			return fmt.Errorf("chmod spool file: %w", err)
		}
		s.file = file
		s.filePath = file.Name()
		if s.fileBuf == nil {
			s.fileBuf = bufio.NewWriterSize(file, 64*1024)
		} else {
			s.fileBuf.Reset(file)
		}
	} else {
		if err := s.file.Truncate(0); err != nil {
			return fmt.Errorf("truncate spool file: %w", err)
		}
		if _, err := s.file.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("seek spool file: %w", err)
		}
		if s.fileBuf == nil {
			s.fileBuf = bufio.NewWriterSize(s.file, 64*1024)
		} else {
			s.fileBuf.Reset(s.file)
		}
	}
	if len(s.mem) > 0 {
		n, err := s.fileBuf.Write(s.mem)
		if err != nil {
			_ = s.file.Close()
			_ = os.Remove(s.filePath)
			s.file = nil
			s.filePath = ""
			return fmt.Errorf("write spool file: %w", err)
		}
		if n != len(s.mem) {
			_ = s.file.Close()
			_ = os.Remove(s.filePath)
			s.file = nil
			s.filePath = ""
			return io.ErrShortWrite
		}
		s.spillBytes += int64(n)
		streamJSONBytePool.release(s.mem)
		s.mem = nil
	}
	s.spilled = true
	s.spillCount++
	return nil
}

func (s *streamCandidateSpool) ensureMemCapacity(next int) error {
	if next <= cap(s.mem) {
		return nil
	}
	target := cap(s.mem)
	if target <= 0 {
		target = defaultStreamSpoolInitialMem
	}
	maxCap := int(s.cfg.memoryBytes)
	if maxCap > 0 {
		target = maxCap
	} else {
		for target < next {
			if target < streamSpoolJumpThreshold {
				target *= 2
			} else {
				target += target / 2
			}
		}
		if maxCap > 0 && target > maxCap {
			target = maxCap
		}
	}
	if target < next {
		target = next
	}
	nextBuf := streamJSONBytePool.acquire(target)
	nextBuf = nextBuf[:len(s.mem)]
	copy(nextBuf, s.mem)
	if s.mem != nil {
		streamJSONBytePool.release(s.mem)
	}
	s.mem = nextBuf
	return nil
}

func (s *streamCandidateSpool) PayloadBytes() []byte {
	if s.spilled {
		return nil
	}
	return s.mem
}

func (s *streamCandidateSpool) Open() (io.ReadCloser, error) {
	if err := s.Finalize(); err != nil {
		return nil, err
	}
	if !s.spilled {
		return io.NopCloser(bytes.NewReader(s.mem)), nil
	}
	if s.file == nil {
		return nil, fmt.Errorf("spool file unavailable")
	}
	f, err := os.Open(s.filePath)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (s *streamCandidateSpool) Finalize() error {
	if !s.spilled || s.fileBuf == nil {
		return nil
	}
	return s.fileBuf.Flush()
}

func (s *streamCandidateSpool) Size() int64 {
	return s.size
}

func (s *streamCandidateSpool) SpillCount() int64 {
	return s.spillCount
}

func (s *streamCandidateSpool) SpillBytes() int64 {
	return s.spillBytes
}

func (s *streamCandidateSpool) SizeHint() int {
	if s.size <= 0 {
		return 0
	}
	if s.size > int64(^uint(0)>>1) {
		return int(^uint(0) >> 1)
	}
	return int(s.size)
}

func (s *streamCandidateSpool) Cleanup() error {
	return s.cleanup(true)
}

func (s *streamCandidateSpool) cleanup(releaseMem bool) error {
	var firstErr error
	if s.file != nil {
		if releaseMem {
			if s.fileBuf != nil {
				if err := s.fileBuf.Flush(); err != nil && firstErr == nil {
					firstErr = err
				}
			}
			if err := s.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			if err := os.Remove(s.filePath); err != nil && !os.IsNotExist(err) && firstErr == nil {
				firstErr = err
			}
			s.file = nil
			s.filePath = ""
		} else if s.spilled {
			if s.fileBuf != nil {
				if err := s.fileBuf.Flush(); err != nil && firstErr == nil {
					firstErr = err
				}
			}
			if err := s.file.Truncate(0); err != nil && firstErr == nil {
				firstErr = err
			}
			if _, err := s.file.Seek(0, io.SeekStart); err != nil && firstErr == nil {
				firstErr = err
			}
			if s.fileBuf != nil {
				s.fileBuf.Reset(s.file)
			}
		}
	}
	s.size = 0
	s.spillCount = 0
	s.spillBytes = 0
	s.spilled = false
	return firstErr
}
