package lql

import (
	"bufio"
	"context"
)

type streamByteReader struct {
	r        *bufio.Reader
	ctx      context.Context
	checkCtx bool
	offset   int64
}

func (r *streamByteReader) Reset(ctx context.Context, br *bufio.Reader) {
	nctx := normalizeStreamContext(ctx)
	r.r = br
	r.ctx = nctx
	r.checkCtx = nctx.Done() != nil
	r.offset = 0
}

func (r *streamByteReader) Offset() int64 {
	return r.offset
}

func (r *streamByteReader) ReadByte() (byte, error) {
	if r.checkCtx {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
	}
	b, err := r.r.ReadByte()
	if err != nil {
		return 0, err
	}
	r.offset++
	return b, nil
}

func (r *streamByteReader) UnreadByte() error {
	err := r.r.UnreadByte()
	if err != nil {
		return err
	}
	if r.offset > 0 {
		r.offset--
	}
	return nil
}

func (r *streamByteReader) Peek(n int) ([]byte, error) {
	if r.checkCtx {
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}
	}
	return r.r.Peek(n)
}

func (r *streamByteReader) Buffered() int {
	return r.r.Buffered()
}

func (r *streamByteReader) Discard(n int) (int, error) {
	if r.checkCtx {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
	}
	m, err := r.r.Discard(n)
	r.offset += int64(m)
	return m, err
}
