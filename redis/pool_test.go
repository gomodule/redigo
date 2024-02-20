// Copyright 2011 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redis_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

const (
	testGoRoutines = 1
)

var (
	_ redis.DoContextHandler = (*poolTestConn)(nil)
	_ redis.CloseHandler     = (*poolTestConn)(nil)
	_ redis.ErrHandler       = (*poolTestConn)(nil)
	_ redis.SendHandler      = (*poolTestConn)(nil)
)

type poolTestConn struct {
	d   *poolDialer
	err error
}

func newPoolTestConn(d *poolDialer) *poolTestConn {
	return &poolTestConn{
		d: d,
	}
}

func (c *poolTestConn) CloseHandler(handler io.Closer) io.Closer {
	return redis.CloserFunc(func() error {
		c.d.closeConn()
		return handler.Close()
	})
}

func (c *poolTestConn) ErrHandler(handler redis.ErrorReporter) redis.ErrorReporter {
	return redis.ErrorReporterFunc(func() error {
		return c.err
	})
}

func (c *poolTestConn) DoContextHandler(handler redis.DoContexter) redis.DoContexter {
	return redis.DoContexterFunc(func(ctx context.Context, commandName string, args ...interface{}) (interface{}, error) {
		if commandName == "ERR" {
			c.err = args[0].(error)
			commandName = "PING"
		}
		if commandName != "" {
			c.d.commands = append(c.d.commands, commandName)
		}
		return handler.DoContext(ctx, commandName, args...)
	})
}

func (c *poolTestConn) SendHandler(handler redis.Sender) redis.Sender {
	return redis.SenderFunc(func(commandName string, args ...interface{}) error {
		c.d.commands = append(c.d.commands, commandName)
		return handler.Send(commandName, args...)
	})
}

type poolDialer struct {
	mu       sync.Mutex
	t        testing.TB
	dialed   int
	open     int
	commands []string
	dialErr  error
	pool     *redis.Pool
}

func newTestPool(t testing.TB, options ...redis.PoolOption) *poolDialer {
	t.Helper()

	d := &poolDialer{t: t}
	return newTestPoolDialer(t, d, options...)
}

func newTestPoolDial(t testing.TB, options ...redis.PoolOption) *poolDialer {
	t.Helper()

	d := &poolDialer{t: t}
	options = append(options, redis.PoolDial(d.dial))
	return newTestPoolDialer(t, d, options...)
}

func newTestPoolDialContext(t testing.TB, options ...redis.PoolOption) *poolDialer {
	t.Helper()

	d := &poolDialer{t: t}
	options = append(options, redis.PoolDialContext(d.dialContext))
	return newTestPoolDialer(t, d, options...)
}

func newTestPoolDialer(t testing.TB, d *poolDialer, options ...redis.PoolOption) *poolDialer {
	t.Helper()

	p, err := redis.NewPool(append(options, redis.DialOptions(redis.DialConnOptions(redis.ConnUse(d))))...)
	require.NoError(t, err)

	d.pool = p
	return d
}

func (d *poolDialer) ValidateClose() {
	d.t.Helper()
	require.NoError(d.t, d.pool.Close())
}

func (d *poolDialer) Clone() interface{} {
	return newPoolTestConn(d)
}

func (d *poolDialer) dial(options ...redis.DialOption) (*redis.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.dialed++
	if d.dialErr != nil {
		return nil, d.dialErr
	}

	c, err := redis.DialDefaultServer(options...)
	if err != nil {
		return nil, err
	}

	d.open++

	return c, nil
}

func (d *poolDialer) closeConn() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.open > 0 {
		d.open--
	}
}

func (d *poolDialer) dialContext(ctx context.Context, options ...redis.DialOption) (*redis.Conn, error) {
	return d.dial(options...)
}

func (d *poolDialer) check(message string, dialed, open, inuse int) {
	d.t.Helper()
	d.checkAll(message, dialed, open, inuse, 0, 0)
}

func (d *poolDialer) checkAll(message string, dialed, open, inuse int, waitCountMax int64, waitDurationMax time.Duration) {
	d.t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()

	require.Equal(d.t, dialed, d.dialed, "dialed")
	require.Equal(d.t, open, d.open, "open")

	stats := d.pool.Stats()

	require.Equal(d.t, open, stats.ActiveCount, "active")
	require.Equal(d.t, open-inuse, stats.IdleCount, "idle")
	require.GreaterOrEqual(d.t, waitCountMax, stats.WaitCount, "wait count")
	if waitCountMax == 0 {
		require.Zero(d.t, stats.WaitDuration, message)
		return
	}

	require.Greater(d.t, waitDurationMax, stats.WaitDuration, "wait duration")
}

func TestPoolReuse(t *testing.T) {
	d := newTestPoolDial(t, redis.MaxIdle(2))

	for i := 0; i < 10; i++ {
		c1 := d.pool.Get()
		_, err := c1.Do("PING")
		require.NoError(t, err)
		c2 := d.pool.Get()
		_, err = c2.Do("PING")
		require.NoError(t, err)
		require.NoError(t, c1.Close())
		require.NoError(t, c2.Close())
	}

	d.check("before close", 2, 2, 0)
	d.ValidateClose()
	d.check("after close", 2, 0, 0)
}

func TestPoolMaxIdle(t *testing.T) {
	d := newTestPoolDial(t, redis.MaxIdle(2))
	defer d.ValidateClose()

	for i := 0; i < 10; i++ {
		c1 := d.pool.Get()
		_, err := c1.Do("PING")
		require.NoError(t, err)
		c2 := d.pool.Get()
		_, err = c2.Do("PING")
		require.NoError(t, err)
		c3 := d.pool.Get()
		_, err = c3.Do("PING")
		require.NoError(t, err)
		require.NoError(t, c1.Close())
		require.NoError(t, c2.Close())
		require.NoError(t, c3.Close())
	}
	d.check("before close", 12, 2, 0)
	d.ValidateClose()
	d.check("after close", 12, 0, 0)
}

func TestPoolError(t *testing.T) {
	d := newTestPoolDial(t, redis.MaxIdle(2))
	defer d.ValidateClose()

	c := d.pool.Get()
	_, err := c.Do("ERR", io.EOF)
	require.NoError(t, err)
	require.Error(t, c.Err())
	require.NoError(t, c.Close())

	c = d.pool.Get()
	_, err = c.Do("ERR", io.EOF)
	require.NoError(t, err)
	require.NoError(t, c.Close())

	d.check(".", 2, 0, 0)
}

func TestPoolClose(t *testing.T) {
	d := newTestPoolDial(t, redis.MaxIdle(2))
	defer d.ValidateClose()

	c1 := d.pool.Get()
	_, err := c1.Do("PING")
	require.NoError(t, err)
	c2 := d.pool.Get()
	_, err = c2.Do("PING")
	require.NoError(t, err)
	c3 := d.pool.Get()
	_, err = c3.Do("PING")
	require.NoError(t, err)

	require.NoError(t, c1.Close())
	_, err = c1.Do("PING")
	require.Error(t, err)

	require.NoError(t, c2.Close())
	require.Error(t, c2.Close())
	require.NoError(t, d.pool.Close())

	d.check("after pool close", 3, 1, 1)

	_, err = c1.Do("PING")
	require.Error(t, err)
	require.NoError(t, c3.Close())

	d.check("after conn close", 3, 0, 0)

	c1 = d.pool.Get()
	_, err = c1.Do("PING")
	require.Error(t, err)

	_, err = d.pool.GetContext(context.Background())
	require.Error(t, err)
}

func TestPoolClosedConn(t *testing.T) {
	d := newTestPoolDial(t,
		redis.MaxIdle(2),
		redis.IdleTimeout(300*time.Second),
	)
	defer d.ValidateClose()

	c := d.pool.Get()
	require.NoError(t, c.Err())
	require.NoError(t, c.Close())
	require.Error(t, c.Err())
	_, err := c.Do("PING")
	require.Error(t, err)
	require.Error(t, c.Send("PING"))
	require.Error(t, c.Flush())
	require.Error(t, c.Flush())
	_, err = c.Receive()
	require.Error(t, err)
}

func TestPoolIdleTimeout(t *testing.T) {
	idleTimeout := 300 * time.Second
	d := newTestPoolDial(t,
		redis.MaxIdle(2),
		redis.IdleTimeout(idleTimeout),
	)
	defer d.ValidateClose()

	now := time.Now()
	redis.SetNowFunc(func() time.Time { return now })
	defer redis.SetNowFunc(time.Now)

	c := d.pool.Get()
	_, err := c.Do("PING")
	require.NoError(t, err)
	require.NoError(t, c.Close())

	d.check("1", 1, 1, 0)

	now = now.Add(idleTimeout + 1)

	c = d.pool.Get()
	_, err = c.Do("PING")
	require.NoError(t, err)
	require.NoError(t, c.Close())

	d.check("2", 2, 1, 0)
}

func TestPoolMaxLifetime(t *testing.T) {
	maxConnLifetime := 300 * time.Second
	d := newTestPoolDial(t,
		redis.MaxIdle(2),
		redis.MaxConnLifetime(maxConnLifetime),
	)
	defer d.ValidateClose()

	now := time.Now()
	redis.SetNowFunc(func() time.Time { return now })
	defer redis.SetNowFunc(time.Now)

	c := d.pool.Get()
	_, err := c.Do("PING")
	require.NoError(t, err)
	require.NoError(t, c.Close())

	d.check("1", 1, 1, 0)

	now = now.Add(maxConnLifetime + 1)

	c = d.pool.Get()
	_, err = c.Do("PING")
	require.NoError(t, err)
	require.NoError(t, c.Close())

	d.check("2", 2, 1, 0)
}

func TestPoolConcurrentSendReceive(t *testing.T) {
	dial := func(options ...redis.DialOption) (*redis.Conn, error) {
		return redis.DialDefaultServer(options...)
	}
	d := newTestPool(t, redis.PoolDial(dial))
	defer d.ValidateClose()

	c := d.pool.Get()
	errs := make(chan error, 1)
	go func() {
		_, err := c.Receive()
		errs <- err
	}()
	require.NoError(t, c.Send("PING"))
	require.NoError(t, c.Flush())
	require.NoError(t, <-errs)
	_, err := c.Do("")
	require.NoError(t, err)
	require.NoError(t, c.Close())
}

func TestPoolBorrowCheck(t *testing.T) {
	d := newTestPoolDial(t,
		redis.MaxIdle(2),
		redis.TestOnBorrow(func(context.Context, *redis.Conn, time.Time) error {
			return redis.Error("BLAH")
		}),
	)
	defer d.ValidateClose()

	for i := 0; i < 10; i++ {
		c := d.pool.Get()
		_, err := c.Do("PING")
		require.NoError(t, err)
		require.NoError(t, c.Close())
	}
	d.check("1", 10, 1, 0)
}

func TestPoolMaxActive(t *testing.T) {
	d := newTestPoolDial(t,
		redis.MaxIdle(2),
		redis.MaxActive(2),
	)
	defer d.ValidateClose()

	c1 := d.pool.Get()
	_, err := c1.Do("PING")
	require.NoError(t, err)
	c2 := d.pool.Get()
	_, err = c2.Do("PING")
	require.NoError(t, err)

	d.check("1", 2, 2, 2)

	c3 := d.pool.Get()
	_, err = c3.Do("PING")
	require.EqualError(t, err, redis.ErrPoolExhausted.Error())

	require.NoError(t, c3.Close())
	d.check("2", 2, 2, 2)
	require.NoError(t, c2.Close())
	d.check("3", 2, 2, 1)

	c3 = d.pool.Get()
	_, err = c3.Do("PING")
	require.NoError(t, err)
	require.NoError(t, c3.Close())

	d.check("4", 2, 2, 1)
}

func TestPoolWaitStats(t *testing.T) {
	d := newTestPoolDial(t,
		redis.MaxIdle(2),
		redis.MaxActive(2),
		redis.Wait(true),
	)
	defer d.ValidateClose()

	c1 := d.pool.Get()
	_, err := c1.Do("PING")
	require.NoError(t, err)
	c2 := d.pool.Get()
	_, err = c2.Do("PING")
	require.NoError(t, err)

	d.checkAll("1", 2, 2, 2, 0, 0)

	start := time.Now()
	errs := make(chan error, 1)
	// TODO: Check this as it looks like it might not be doing the right thing.
	go func() {
		time.Sleep(time.Millisecond * 100)
		errs <- c1.Close()
	}()

	c3 := d.pool.Get()
	d.checkAll("2", 2, 2, 2, 1, time.Since(start))

	_, err = c3.Do("PING")
	require.NoError(t, err)
	require.NoError(t, <-errs)
}

func TestPoolMonitorCleanup(t *testing.T) {
	d := newTestPoolDial(t,
		redis.MaxIdle(2),
		redis.MaxIdle(2),
		redis.MaxActive(2),
	)
	defer d.ValidateClose()

	c := d.pool.Get()
	require.NoError(t, c.Send("MONITOR"))
	require.Error(t, c.Close())

	d.check("", 1, 0, 0)
}

func TestPoolPubSubCleanup(t *testing.T) {
	d := newTestPoolDial(t,
		redis.MaxIdle(2),
		redis.MaxIdle(2),
		redis.MaxActive(2),
	)
	defer d.ValidateClose()

	c := d.pool.Get()
	require.NoError(t, c.Send("SUBSCRIBE", "x"))
	require.NoError(t, c.Close())

	want := []string{"SUBSCRIBE", "UNSUBSCRIBE", "ECHO"}
	require.Equal(t, want, d.commands)
	d.commands = nil

	c = d.pool.Get()
	require.NoError(t, c.Send("PSUBSCRIBE", "x*"))
	require.NoError(t, c.Close())

	want = []string{"PSUBSCRIBE", "PUNSUBSCRIBE", "ECHO"}
	require.Equal(t, want, d.commands)
}

func TestPoolTransactionCleanup(t *testing.T) {
	d := newTestPoolDial(t,
		redis.MaxIdle(2),
		redis.MaxActive(2),
	)
	defer d.ValidateClose()

	c := d.pool.Get()
	_, err := c.Do("WATCH", "key")
	require.NoError(t, err)
	_, err = c.Do("PING")
	require.NoError(t, err)
	require.NoError(t, c.Close())

	want := []string{"WATCH", "PING", "UNWATCH"}
	require.Equal(t, want, d.commands)
	d.commands = nil

	c = d.pool.Get()
	_, err = c.Do("WATCH", "key")
	require.NoError(t, err)
	_, err = c.Do("UNWATCH")
	require.NoError(t, err)
	_, err = c.Do("PING")
	require.NoError(t, err)
	require.NoError(t, c.Close())

	want = []string{"WATCH", "UNWATCH", "PING"}
	require.Equal(t, want, d.commands)
	d.commands = nil

	c = d.pool.Get()
	_, err = c.Do("WATCH", "key")
	require.NoError(t, err)
	_, err = c.Do("MULTI")
	require.NoError(t, err)
	_, err = c.Do("PING")
	require.NoError(t, err)
	require.NoError(t, c.Close())

	want = []string{"WATCH", "MULTI", "PING", "DISCARD"}
	require.Equal(t, want, d.commands)
	d.commands = nil

	c = d.pool.Get()
	_, err = c.Do("WATCH", "key")
	require.NoError(t, err)
	_, err = c.Do("MULTI")
	require.NoError(t, err)
	_, err = c.Do("DISCARD")
	require.NoError(t, err)
	_, err = c.Do("PING")
	require.NoError(t, err)
	require.NoError(t, c.Close())

	want = []string{"WATCH", "MULTI", "DISCARD", "PING"}
	require.Equal(t, want, d.commands)
	d.commands = nil

	c = d.pool.Get()
	_, err = c.Do("WATCH", "key")
	require.NoError(t, err)
	_, err = c.Do("MULTI")
	require.NoError(t, err)
	_, err = c.Do("EXEC")
	require.NoError(t, err)
	_, err = c.Do("PING")
	require.NoError(t, err)
	require.NoError(t, c.Close())

	want = []string{"WATCH", "MULTI", "EXEC", "PING"}
	require.Equal(t, want, d.commands)
}

func startGoroutines(p *redis.Pool, cmd string, args ...interface{}) chan error {
	errs := make(chan error, testGoRoutines*2)
	for i := 0; i < testGoRoutines; i++ {
		go func() {
			c := p.Get()
			defer func() {
				errs <- c.Close()
			}()

			_, err := c.Do(cmd, args...)
			errs <- err
		}()
	}

	return errs
}

func TestWaitPool(t *testing.T) {
	d := newTestPoolDial(t,
		redis.MaxIdle(1),
		redis.MaxActive(1),
		redis.Wait(true),
	)
	defer d.ValidateClose()

	c := d.pool.Get()
	start := time.Now()
	errs := startGoroutines(d.pool, "PING")
	d.check("before close", 1, 1, 1)
	require.NoError(t, c.Close())
	timeout := time.After(2 * time.Second)
	for i := 0; i < cap(errs); i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
		case <-timeout:
			t.Fatalf("timeout waiting for blocked goroutine %d", i)
		}
	}
	d.checkAll("done", 1, 1, 0, testGoRoutines, time.Since(start)*testGoRoutines)
}

func TestWaitPoolClose(t *testing.T) {
	d := newTestPoolDial(t,
		redis.MaxIdle(1),
		redis.MaxActive(1),
		redis.Wait(true),
	)
	defer d.ValidateClose()

	c := d.pool.Get()
	_, err := c.Do("PING")
	require.NoError(t, err)

	start := time.Now()
	errs := startGoroutines(d.pool, "PING")
	d.check("before close", 1, 1, 1)
	require.NoError(t, d.pool.Close())
	timeout := time.After(2 * time.Second)
	var nils int
	for i := 0; i < cap(errs); i++ {
		select {
		case err := <-errs:
			if err != nil {
				require.EqualError(t, err, redis.ErrPoolClosed.Error())
				continue
			}
			nils++
		case <-timeout:
			t.Fatal("timeout waiting for blocked goroutine")
		}
	}
	require.Equal(t, testGoRoutines, nils)
	require.NoError(t, c.Close())
	d.checkAll("done", 1, 0, 0, testGoRoutines, time.Since(start)*testGoRoutines)
}

func TestWaitPoolCommandError(t *testing.T) {
	testErr := errors.New("test")
	d := newTestPoolDial(t,
		redis.MaxIdle(1),
		redis.MaxActive(1),
		redis.Wait(true),
	)
	defer d.ValidateClose()

	c := d.pool.Get()
	start := time.Now()
	errs := startGoroutines(d.pool, "ERR", testErr)
	d.check("before close", 1, 1, 1)
	require.NoError(t, c.Close())
	timeout := time.After(2 * time.Second)
	for i := 0; i < cap(errs); i++ {
		select {
		case err := <-errs:
			require.NoError(t, err)
		case <-timeout:
			t.Fatalf("timeout waiting for blocked goroutine %d", i)
		}
	}
	d.checkAll("done", 1, 0, 0, testGoRoutines, time.Since(start)*testGoRoutines)
}

func TestWaitPoolDialError(t *testing.T) {
	testErr := errors.New("test")
	d := newTestPoolDial(t,
		redis.MaxIdle(1),
		redis.MaxActive(1),
		redis.Wait(true),
	)
	defer d.ValidateClose()

	c := d.pool.Get()
	start := time.Now()
	errs := startGoroutines(d.pool, "ERR", testErr)
	d.check("before close", 1, 1, 1)

	d.dialErr = errors.New("dial")
	require.NoError(t, c.Close())

	nilCount := 0
	errCount := 0
	timeout := time.After(2 * time.Second)
	for i := 0; i < cap(errs); i++ {
		select {
		case err := <-errs:
			switch err {
			case nil:
				nilCount++
			case d.dialErr:
				errCount++
			default:
				t.Fatalf("expected dial error or nil, got %v", err)
			}
		case <-timeout:
			t.Fatalf("timeout waiting for blocked goroutine %d", i)
		}
	}
	require.Equal(t, testGoRoutines+1, nilCount)
	require.Equal(t, testGoRoutines-1, errCount)
	d.checkAll("done", testGoRoutines, 0, 0, testGoRoutines-1, time.Since(start)*testGoRoutines)
}

// Borrowing requires us to iterate over the idle connections, unlock the pool,
// and perform a blocking operation to check the connection still works. If
// TestOnBorrow fails, we must reacquire the lock and continue iteration. This
// test ensures that iteration will work correctly if multiple threads are
// iterating simultaneously.
func TestLocking_TestOnBorrowFails_PoolDoesntCrash(t *testing.T) {
	const count = 100

	// First we'll Create a pool where the pilfering of idle connections fails.
	d := newTestPoolDial(t,
		redis.MaxIdle(count),
		redis.MaxActive(count),
		redis.TestOnBorrow(func(context.Context, *redis.Conn, time.Time) error {
			return errors.New("No way back into the real world.")
		}),
	)
	defer d.ValidateClose()

	// Fill the pool with idle connections.
	conns := make([]*redis.PoolConn, count)
	for i := range conns {
		conns[i] = d.pool.Get()
	}
	for i := range conns {
		conns[i].Close()
	}

	// Spawn a bunch of goroutines to thrash the pool.
	var wg sync.WaitGroup
	wg.Add(count)
	errs := make(chan error, count*2)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			c := d.pool.Get()
			defer func() {
				errs <- c.Close()
			}()
			errs <- c.Err()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, cap(errs), d.dialed)
}

func BenchmarkPoolGet(b *testing.B) {
	dial := func(options ...redis.DialOption) (*redis.Conn, error) {
		return redis.DialDefaultServer(options...)
	}

	d := newTestPool(b,
		redis.PoolDial(dial),
		redis.MaxIdle(2),
	)
	defer d.ValidateClose()

	c := d.pool.Get()
	require.NoError(b, c.Err())
	require.NoError(b, c.Close())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c = d.pool.Get()
		require.NoError(b, c.Close())
	}
}

func BenchmarkPoolDo(b *testing.B) {
	dial := func(options ...redis.DialOption) (*redis.Conn, error) {
		return redis.DialDefaultServer(options...)
	}
	d := newTestPool(b,
		redis.PoolDial(dial),
		redis.MaxIdle(2),
	)
	defer d.ValidateClose()

	func() {
		// Setup required data.
		c := d.pool.Get()
		require.NoError(b, c.Err())
		defer func() {
			require.NoError(b, c.Close())
		}()

		_, err := c.Do("MSET", "foo", "bar", "bar", "baz")
		require.NoError(b, err)
		_, err = c.Do("HMSET", "hfoo", "bar", "baz", "qux", "quux", "thing", "bob")
		require.NoError(b, err)
	}()

	tests := map[string]struct {
		cmd  string
		args []interface{}
	}{
		"set": {
			cmd:  "SET",
			args: []interface{}{"foo", "bar"},
		},
		"get": {
			cmd:  "GET",
			args: []interface{}{"foo"},
		},
		"mget": {
			cmd:  "MGET",
			args: []interface{}{"foo", "bar"},
		},
		"hmset": {
			cmd:  "HMSET",
			args: []interface{}{"hfoo", "bar", "baz", "qux", "baz1", "qux1", "stuff"},
		},
		"hgetall": {
			cmd:  "HGETALL",
			args: []interface{}{"hfoo"},
		},
		"blank": {
			cmd: "",
		},
		"ping": {
			cmd: "PING",
		},
	}
	for name, tc := range tests {
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				c := d.pool.Get()
				_, err := c.Do(tc.cmd, tc.args...)
				require.NoError(b, err)
				require.NoError(b, c.Close())
			}
		})
	}
}

func BenchmarkPoolGetErr(b *testing.B) {
	dial := func(options ...redis.DialOption) (*redis.Conn, error) {
		return redis.DialDefaultServer(options...)
	}
	d := newTestPool(b,
		redis.PoolDial(dial),
		redis.MaxIdle(2),
	)
	defer d.ValidateClose()

	c := d.pool.Get()
	require.NoError(b, c.Err())
	require.NoError(b, c.Close())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c = d.pool.Get()
		require.NoError(b, c.Err())
		require.NoError(b, c.Close())
	}
}

func BenchmarkPoolGetPing(b *testing.B) {
	dial := func(options ...redis.DialOption) (*redis.Conn, error) {
		return redis.DialDefaultServer(options...)
	}
	d := newTestPool(b,
		redis.PoolDial(dial),
		redis.MaxIdle(2),
	)
	defer d.ValidateClose()

	c := d.pool.Get()
	require.NoError(b, c.Err())
	require.NoError(b, c.Close())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c = d.pool.Get()
		_, err := c.Do("PING")
		require.NoError(b, err)
		require.NoError(b, c.Close())
	}
}

func TestWaitPoolGetContext(t *testing.T) {
	d := newTestPoolDial(t,
		redis.MaxIdle(1),
		redis.MaxActive(1),
		redis.Wait(true),
	)
	defer d.ValidateClose()

	c, err := d.pool.GetContext(context.Background())
	require.NoError(t, err)
	require.NoError(t, c.Close())
}

func TestWaitPoolGetContextIssue520(t *testing.T) {
	d := newTestPoolDial(t,
		redis.MaxIdle(1),
		redis.MaxActive(1),
		redis.Wait(true),
	)
	defer d.ValidateClose()

	ctx1, cancel1 := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel1()

	c, err := d.pool.GetContext(ctx1)
	require.EqualError(t, err, context.DeadlineExceeded.Error())
	defer func() {
		require.NoError(t, c.Close())
	}()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	c2, err := d.pool.GetContext(ctx2)
	require.NoError(t, err)
	require.NoError(t, c2.Close())
}

func TestWaitPoolGetContextWithDialContext(t *testing.T) {
	d := newTestPoolDialContext(t,
		redis.MaxIdle(1),
		redis.MaxActive(1),
		redis.Wait(true),
	)
	defer d.ValidateClose()

	c, err := d.pool.GetContext(context.Background())
	require.NoError(t, err)
	require.NoError(t, c.Close())
}

func TestPoolGetContext_DialContext(t *testing.T) {
	var isPassed bool
	f := func(ctx context.Context, network, addr string) (net.Conn, error) {
		isPassed = true
		return &testConn{}, nil
	}

	dial := func(ctx context.Context, options ...redis.DialOption) (*redis.Conn, error) {
		return redis.DialContext(ctx, "", "", append([]redis.DialOption{redis.DialContextFunc(f)}, options...)...)
	}
	d := newTestPool(t,
		redis.PoolDialContext(dial),
		redis.MaxIdle(1),
		redis.MaxActive(1),
		redis.Wait(true),
	)
	defer d.ValidateClose()

	_, err := d.pool.GetContext(context.Background())
	require.NoError(t, err)
	require.True(t, isPassed)
}

func TestPoolGetContext_DialContext_CanceledContext(t *testing.T) {
	addr, err := redis.DefaultServerAddr()
	require.NoError(t, err)

	dial := func(ctx context.Context, options ...redis.DialOption) (*redis.Conn, error) {
		return redis.DialContext(ctx, "tcp", addr, options...)
	}
	d := newTestPool(t,
		redis.PoolDialContext(dial),
		redis.MaxIdle(1),
		redis.MaxActive(1),
		redis.Wait(true),
	)
	defer d.ValidateClose()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = d.pool.GetContext(ctx)
	require.Error(t, err)
}

func TestWaitPoolGetAfterClose(t *testing.T) {
	d := newTestPoolDial(t,
		redis.MaxIdle(1),
		redis.MaxActive(1),
		redis.Wait(true),
	)
	d.ValidateClose()

	_, err := d.pool.GetContext(context.Background())
	require.Error(t, err)
}

func TestWaitPoolGetCanceledContext(t *testing.T) {
	t.Run("without vacant connection in the pool", func(t *testing.T) {
		d := newTestPoolDial(t,
			redis.MaxIdle(1),
			redis.MaxActive(1),
			redis.Wait(true),
		)
		defer d.ValidateClose()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		c := d.pool.Get()
		defer func() {
			require.NoError(t, c.Close())
		}()
		_, err := d.pool.GetContext(ctx)
		require.EqualError(t, err, context.Canceled.Error())
	})

	t.Run("with vacant connection in the pool", func(t *testing.T) {
		d := newTestPoolDial(t,
			redis.MaxIdle(1),
			redis.MaxActive(1),
			redis.Wait(true),
		)
		defer d.ValidateClose()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := d.pool.GetContext(ctx)
		require.EqualError(t, err, context.Canceled.Error())
	})
}
