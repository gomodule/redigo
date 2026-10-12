package redis_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	redigo "github.com/gomodule/redigo/redis"
)

type observedConn struct {
	net.Conn
	delay                       time.Duration
	mu                          sync.Mutex
	readDeadline, writeDeadline time.Time
	closed                      budgetAtomicBool
}

func (c *observedConn) Write(p []byte) (int, error) {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return c.Conn.Write(p)
}
func (c *observedConn) SetReadDeadline(d time.Time) error {
	c.mu.Lock()
	c.readDeadline = d
	c.mu.Unlock()
	return c.Conn.SetReadDeadline(d)
}
func (c *observedConn) SetWriteDeadline(d time.Time) error {
	c.mu.Lock()
	c.writeDeadline = d
	c.mu.Unlock()
	return c.Conn.SetWriteDeadline(d)
}
func (c *observedConn) Close() error { c.closed.Store(true); return c.Conn.Close() }

func dialBudgetConn(t *testing.T, address string, read, write, delay time.Duration) (redigo.Conn, *observedConn) {
	t.Helper()
	raw, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	o := &observedConn{Conn: raw, delay: delay}
	c := redigo.NewConn(o, read, write)
	t.Cleanup(func() { _ = c.Close() })
	return c, o
}

func TestExplicitReadOverridesDefault(t *testing.T) {
	address, _ := startBudgetServer(t, func(c net.Conn, _ []string) {
		time.Sleep(180 * time.Millisecond)
		_, _ = io.WriteString(c, "$2\r\nOK\r\n")
	})
	c, _ := dialBudgetConn(t, address, 100*time.Millisecond, 100*time.Millisecond, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	reply, err := redigo.DoContextWithTimeout(c, ctx, 500*time.Millisecond, "GET", "key")
	t.Logf("default_read=100ms explicit_read=500ms total=500ms elapsed=%v reply=%q error=%v", time.Since(start), reply, err)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWritesConsumeTotalBudget(t *testing.T) {
	address, _ := startBudgetServer(t, func(c net.Conn, _ []string) {
		time.Sleep(60 * time.Millisecond)
		_, _ = io.WriteString(c, "$2\r\nOK\r\n")
	})
	c, o := dialBudgetConn(t, address, 100*time.Millisecond, 100*time.Millisecond, 60*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	start := time.Now()
	_, err := redigo.DoContextWithTimeout(c, ctx, 90*time.Millisecond, "GET", "key")
	o.mu.Lock()
	rd, wd := o.readDeadline, o.writeDeadline
	o.mu.Unlock()
	t.Logf("elapsed=%v error=%v read_deadline_drift=%v write_deadline_drift=%v", time.Since(start), err, rd.Sub(deadline), wd.Sub(deadline))
	if !errors.Is(err, context.DeadlineExceeded) || !rd.Equal(deadline) || !wd.Equal(deadline) || c.Err() == nil {
		t.Fatal("total deadline not enforced")
	}
}

func TestExplicitReadTimeoutStillLimitsRead(t *testing.T) {
	address, _ := startBudgetServer(t, func(net.Conn, []string) {})
	c, _ := dialBudgetConn(t, address, 100*time.Millisecond, time.Second, 0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	_, err := redigo.DoContextWithTimeout(c, ctx, 20*time.Millisecond, "GET", "key")
	t.Logf("elapsed=%v error=%v context_error=%v", time.Since(start), err, ctx.Err())
	if !errors.Is(err, os.ErrDeadlineExceeded) || ctx.Err() != nil || c.Err() == nil {
		t.Fatal("read stage timeout misclassified")
	}
}

func TestZeroReadTimeoutSemantics(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		t.Run(fmt.Sprint(bounded), func(t *testing.T) {
			address, _ := startBudgetServer(t, func(c net.Conn, _ []string) {
				time.Sleep(180 * time.Millisecond)
				_, _ = io.WriteString(c, "$2\r\nOK\r\n")
			})
			c, _ := dialBudgetConn(t, address, 100*time.Millisecond, time.Second, 0)
			ctx := context.Background()
			cancel := func() {}
			if bounded {
				ctx, cancel = context.WithTimeout(ctx, 40*time.Millisecond)
			}
			defer cancel()
			start := time.Now()
			_, err := redigo.DoContextWithTimeout(c, ctx, 0, "GET", "key")
			t.Logf("elapsed=%v error=%v", time.Since(start), err)
			if bounded && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			if !bounded && err != nil {
				t.Fatal(err)
			}
		})
	}
}

func budgetPoolFor(t *testing.T, address string) *redigo.Pool {
	p := &redigo.Pool{MaxIdle: 1, MaxActive: 1, Dial: func() (redigo.Conn, error) {
		return redigo.Dial("tcp", address, redigo.DialReadTimeout(100*time.Millisecond), redigo.DialWriteTimeout(100*time.Millisecond))
	}}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestPreCanceledRequestPreservesPooledConnection(t *testing.T) {
	var commands budgetAtomicInt64
	address, _ := startBudgetServer(t, func(c net.Conn, _ []string) { commands.Add(1); _, _ = io.WriteString(c, "+OK\r\n") })
	p := budgetPoolFor(t, address)
	c, err := p.GetContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = redigo.DoContextWithTimeout(c, ctx, time.Second, "GET", "key")
	_ = c.Close()
	if !errors.Is(err, context.Canceled) || commands.Load() != 0 || p.Stats().IdleCount != 1 {
		t.Fatalf("error=%v commands=%d pool=%+v", err, commands.Load(), p.Stats())
	}
	t.Logf("commands=0 active=%d idle=%d", p.Stats().ActiveCount, p.Stats().IdleCount)
}

func TestCancelDuringReadDiscardsPooledConnection(t *testing.T) {
	seen := make(chan struct{}, 1)
	var calls budgetAtomicInt64
	address, accepted := startBudgetServer(t, func(c net.Conn, _ []string) {
		if calls.Add(1) == 1 {
			seen <- struct{}{}
			return
		}
		_, _ = io.WriteString(c, "+OK\r\n")
	})
	p := budgetPoolFor(t, address)
	c, err := p.GetContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := redigo.DoContextWithTimeout(c, ctx, time.Second, "GET", "key"); done <- e }()
	<-seen
	cancel()
	err = <-done
	_ = c.Close()
	if !errors.Is(err, context.Canceled) || p.Stats().ActiveCount != 0 {
		t.Fatalf("error=%v pool=%+v", err, p.Stats())
	}
	next, err := p.GetContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = redigo.DoContextWithTimeout(next, context.Background(), time.Second, "GET", "next")
	_ = next.Close()
	if err != nil || accepted.Load() != 2 {
		t.Fatalf("error=%v accepted=%d", err, accepted.Load())
	}
	t.Log("cancelled connection discarded; next command used a fresh connection")
}

func TestLateCancelCannotCloseReusedConnection(t *testing.T) {
	address, accepted := startBudgetServer(t, func(c net.Conn, _ []string) { _, _ = io.WriteString(c, "+OK\r\n") })
	p := budgetPoolFor(t, address)
	for i := 0; i < 100; i++ {
		c, err := p.GetContext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		_, err = redigo.DoContextWithTimeout(c, ctx, time.Second, "GET", "old")
		_ = c.Close()
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		next, err := p.GetContext(context.Background())
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel() // old request cancellation happens after the same connection is borrowed again
		_, err = redigo.DoContextWithTimeout(next, context.Background(), time.Second, "GET", "new")
		_ = next.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	if accepted.Load() != 1 {
		t.Fatalf("late cancellation forced reconnect: %d", accepted.Load())
	}
	t.Log("200 calls with 100 late cancellations reused one physical connection")
}

func TestServerErrorRetainsConnection(t *testing.T) {
	var n budgetAtomicInt64
	address, accepted := startBudgetServer(t, func(c net.Conn, _ []string) {
		if n.Add(1) == 1 {
			_, _ = io.WriteString(c, "-WRONGTYPE test\r\n")
		} else {
			_, _ = io.WriteString(c, "+OK\r\n")
		}
	})
	p := budgetPoolFor(t, address)
	c, _ := p.GetContext(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := redigo.DoContextWithTimeout(c, ctx, time.Second, "GET", "key")
	var re redigo.Error
	if !errors.As(err, &re) || c.Err() != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	c, _ = p.GetContext(context.Background())
	_, err = redigo.DoContextWithTimeout(c, ctx, time.Second, "GET", "key")
	_ = c.Close()
	if err != nil || accepted.Load() != 1 {
		t.Fatal(err)
	}
}

func TestDeadlineDoesNotLeakToUnlimitedLegacyWrite(t *testing.T) {
	address, _ := startBudgetServer(t, func(c net.Conn, _ []string) { _, _ = io.WriteString(c, "+OK\r\n") })
	c, _ := dialBudgetConn(t, address, 100*time.Millisecond, 0, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err := redigo.DoContextWithTimeout(c, ctx, time.Second, "GET", "first")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	time.Sleep(30 * time.Millisecond)
	_, err = redigo.DoWithTimeout(c, time.Second, "GET", "legacy")
	if err != nil {
		t.Fatal("stale write deadline", err)
	}
}

func TestCancellationInterruptsBlockedWrite(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	c := redigo.NewConn(client, time.Second, 0)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := redigo.DoContextWithTimeout(c, ctx, time.Second, "SET", "key", "value")
	t.Logf("blocked write: elapsed=%v error=%v", time.Since(start), err)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

type blockingArgument struct{ started, release, finished chan struct{} }

func (a *blockingArgument) RedisArg() interface{} {
	close(a.started)
	<-a.release
	close(a.finished)
	return "key"
}
func TestCallerWaitsForArgumentEncoding(t *testing.T) {
	address, _ := startBudgetServer(t, func(c net.Conn, _ []string) { _, _ = io.WriteString(c, "+OK\r\n") })
	c, o := dialBudgetConn(t, address, time.Second, time.Second, 0)
	a := &blockingArgument{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := redigo.DoContextWithTimeout(c, ctx, time.Second, "GET", a); done <- err }()
	<-a.started
	cancel()
	select {
	case err := <-done:
		t.Fatalf("returned before argument encoding ended: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(a.release)
	err := <-done
	if !errors.Is(err, context.Canceled) || !o.closed.Load() {
		t.Fatal(err)
	}
	select {
	case <-a.finished:
	default:
		t.Fatal("encoder still running")
	}
	t.Log("physical connection cancelled promptly; call waited for controlled encoder to finish")
}

type delayedDeadline struct {
	context.Context
	deadline time.Time
}

func (c delayedDeadline) Deadline() (time.Time, bool) { return c.deadline, true }
func TestExpiredDeadlineWithoutCancellationSignal(t *testing.T) {
	address, _ := startBudgetServer(t, func(c net.Conn, _ []string) { _, _ = io.WriteString(c, "+OK\r\n") })
	c, o := dialBudgetConn(t, address, time.Second, time.Second, 0)
	ctx := delayedDeadline{context.Background(), time.Now().Add(-time.Second)}
	_, err := redigo.DoContextWithTimeout(c, ctx, time.Second, "GET", "key")
	if !errors.Is(err, context.DeadlineExceeded) || o.closed.Load() {
		t.Fatal("preflight should reject without closing healthy connection", err)
	}
}

func TestLoggingWrapperForwardsOptionalInterface(t *testing.T) {
	address, _ := startBudgetServer(t, func(c net.Conn, _ []string) { _, _ = io.WriteString(c, "+OK\r\n") })
	c, _ := dialBudgetConn(t, address, time.Second, time.Second, 0)
	lc := redigo.NewLoggingConn(c, log.New(io.Discard, "", 0), "test")
	_, err := redigo.DoContextWithTimeout(lc, context.Background(), time.Second, "GET", "key")
	if err != nil {
		t.Fatal(err)
	}
}

type resetFailureConn struct {
	net.Conn
	resetErr error
}

func (c *resetFailureConn) SetDeadline(t time.Time) error {
	if t.IsZero() {
		return c.resetErr
	}
	return c.Conn.SetDeadline(t)
}

func TestDeadlineCleanupPreservesCompletedReply(t *testing.T) {
	for _, wire := range []string{"+PONG\r\n", "-WRONGTYPE test\r\n"} {
		t.Run(wire[:len(wire)-2], func(t *testing.T) {
			address, _ := startBudgetServer(t, func(c net.Conn, _ []string) { _, _ = io.WriteString(c, wire) })
			nc, err := net.Dial("tcp", address)
			if err != nil {
				t.Fatal(err)
			}
			resetErr := errors.New("test deadline reset failure")
			c := redigo.NewConn(&resetFailureConn{nc, resetErr}, time.Second, time.Second)
			defer c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			reply, err := redigo.DoContextWithTimeout(c, ctx, time.Second, "PING")
			if wire[0] == '+' {
				if err != nil || reply != "PONG" {
					t.Fatalf("completed reply lost: %v %v", reply, err)
				}
			} else if !errors.Is(err, redigo.Error("WRONGTYPE test")) {
				t.Fatalf("server error lost: %v", err)
			}
			if !errors.Is(c.Err(), resetErr) {
				t.Fatalf("connection not retired: %v", c.Err())
			}
		})
	}
}
