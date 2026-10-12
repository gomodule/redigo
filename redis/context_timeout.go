package redis

import (
	"context"
	"errors"
	"time"
)

// ConnWithContextTimeout supports a command context and an explicit read timeout.
// The explicit read timeout overrides DialReadTimeout. The context constrains
// the entire command. A zero read timeout removes only the read-stage limit.
type ConnWithContextTimeout interface {
	Conn
	DoContextWithTimeout(context.Context, time.Duration, string, ...interface{}) (interface{}, error)
}

// DoContextWithTimeout executes a command with an explicit read timeout, overriding
// DialReadTimeout. The context bounds the entire command, including writes, and
// cancellation closes an in-flight connection. A zero timeout disables only the
// read-stage limit. Encoding runs synchronously; a blocking RedisArg must return
// before this function can finish. The context must not be nil.
func DoContextWithTimeout(c Conn, ctx context.Context, readTimeout time.Duration, cmd string, args ...interface{}) (interface{}, error) {
	cc, ok := c.(ConnWithContextTimeout)
	if !ok {
		return nil, errors.New("redis: connection does not support ConnWithContextTimeout")
	}
	return cc.DoContextWithTimeout(ctx, readTimeout, cmd, args...)
}

func earlierDeadline(a, b time.Time) time.Time {
	if a.IsZero() || (!b.IsZero() && b.Before(a)) {
		return b
	}
	return a
}

func commandContextError(ctx context.Context, deadline time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func (c *conn) DoContextWithTimeout(ctx context.Context, readTimeout time.Duration, cmd string, args ...interface{}) (reply interface{}, err error) {
	deadline, _ := ctx.Deadline()
	if err := commandContextError(ctx, deadline); err != nil {
		return nil, err
	}
	if readTimeout < 0 {
		return nil, errors.New("redis: negative explicit read timeout")
	}
	if ctx.Done() == nil && deadline.IsZero() {
		return c.DoWithTimeout(readTimeout, cmd, args...)
	}
	stopAndWait := watchCommandContext(ctx, func() { _ = c.fatal(ctx.Err()) })
	defer func() {
		// Stop/join before this physical connection can be returned to a pool.
		stopAndWait()
		if contextErr := commandContextError(ctx, deadline); contextErr != nil {
			reply, err = nil, c.fatal(contextErr)
		}
		// Do not leave a command deadline on a later legacy write with timeout=0.
		if !deadline.IsZero() && c.Err() == nil {
			if resetErr := c.conn.SetDeadline(time.Time{}); resetErr != nil {
				// The command already has a complete reply. Retire the connection,
				// but do not replace that known result with a cleanup error.
				_ = c.fatal(resetErr)
			}
		}
	}()
	// Encoding and I/O stay on the caller's goroutine. A blocking custom RedisArg
	// cannot be forcibly interrupted and must finish before this call returns.
	return c.doWithTimeout(readTimeout, deadline, cmd, args...)
}

func (ac *activeConn) DoContextWithTimeout(ctx context.Context, timeout time.Duration, cmd string, args ...interface{}) (interface{}, error) {
	pc := ac.pc
	if pc == nil {
		return nil, errConnClosed
	}
	cc, ok := pc.c.(ConnWithContextTimeout)
	if !ok {
		return nil, errors.New("redis: pooled connection does not support ConnWithContextTimeout")
	}
	deadline, _ := ctx.Deadline()
	if err := commandContextError(ctx, deadline); err != nil {
		return nil, err
	}
	if timeout < 0 {
		return nil, errors.New("redis: negative explicit read timeout")
	}
	ci := lookupCommandInfo(cmd)
	ac.state = (ac.state | ci.Set) &^ ci.Clear
	return cc.DoContextWithTimeout(ctx, timeout, cmd, args...)
}

func (ec errorConn) DoContextWithTimeout(context.Context, time.Duration, string, ...interface{}) (interface{}, error) {
	return nil, ec.err
}

func (c *loggingConn) DoContextWithTimeout(ctx context.Context, timeout time.Duration, cmd string, args ...interface{}) (interface{}, error) {
	reply, err := DoContextWithTimeout(c.Conn, ctx, timeout, cmd, args...)
	c.print("DoContextWithTimeout", cmd, args, reply, err)
	return reply, err
}
