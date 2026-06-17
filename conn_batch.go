package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

type batch struct {
	err      error
	ctx      context.Context
	conn     *connect
	block    *proto.Block
	released bool
	release  func(*connect, error)
}

func (b *batch) Abort() error {
	if b.released {
		return ErrBatchAlreadyReleased
	}
	b.released = true
	b.release(b.conn, fmt.Errorf("batch aborted"))
	return nil
}

func (b *batch) Append(v ...any) error {
	if b.released {
		return ErrBatchAlreadyReleased
	}
	if b.err != nil {
		return b.err
	}
	if err := b.block.Append(v...); err != nil {
		b.err = err
		return err
	}
	return nil
}

func (b *batch) AppendStruct(v any) error {
	if b.released {
		return ErrBatchAlreadyReleased
	}
	if b.err != nil {
		return b.err
	}
	if err := b.block.AppendStruct(v); err != nil {
		b.err = err
		return err
	}
	return nil
}

func (b *batch) IsReleased() bool {
	return b.released
}

func (b *batch) Send() error {
	if b.released {
		return ErrBatchAlreadyReleased
	}
	b.released = true
	defer func() {
		b.release(b.conn, b.err)
	}()
	if b.err != nil {
		return b.err
	}
	select {
	case <-b.ctx.Done():
		b.err = b.ctx.Err()
		return b.err
	default:
	}
	if b.conn.conn != nil {
		if deadline, ok := b.ctx.Deadline(); ok {
			b.conn.conn.SetDeadline(deadline)
			defer b.conn.conn.SetDeadline(time.Time{})
		}
	}
	if b.ctx.Done() != nil {
		done := make(chan struct{})
		defer close(done)
		go func() {
			select {
			case <-b.ctx.Done():
				if b.conn.conn != nil {
					b.conn.conn.Close()
				}
			case <-done:
			}
		}()
	}
	if err := b.conn.sendData(b.block, ""); err != nil {
		b.err = err
		if b.ctx.Err() != nil {
			b.err = b.ctx.Err()
		}
		return b.err
	}
	if err := b.conn.process(b.ctx); err != nil {
		b.err = err
		if b.ctx.Err() != nil {
			b.err = b.ctx.Err()
		}
		return b.err
	}
	return nil
}

var _ Batch = (*batch)(nil)