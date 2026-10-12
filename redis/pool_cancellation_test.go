package redis

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests exercise pool ownership independently of a server or command API.
type cancellationTestConn struct {
	closed int32
	err    error
}

func (c *cancellationTestConn) Close() error                                 { atomic.AddInt32(&c.closed, 1); return nil }
func (c *cancellationTestConn) Err() error                                   { return c.err }
func (*cancellationTestConn) Do(string, ...interface{}) (interface{}, error) { return nil, nil }
func (*cancellationTestConn) Send(string, ...interface{}) error              { return nil }
func (*cancellationTestConn) Flush() error                                   { return nil }
func (*cancellationTestConn) Receive() (interface{}, error)                  { return nil, nil }

func warmCancellationPool(t *testing.T, wait bool) (*Pool, *[]*cancellationTestConn) {
	t.Helper()
	var physical []*cancellationTestConn
	p := &Pool{MaxIdle: 3, MaxActive: 3, Wait: wait, Dial: func() (Conn, error) {
		c := &cancellationTestConn{}
		physical = append(physical, c)
		return c, nil
	}}
	var borrowed []Conn
	for i := 0; i < 3; i++ {
		c, err := p.GetContext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		borrowed = append(borrowed, c)
	}
	for _, c := range borrowed {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { p.Close() })
	return p, &physical
}

func TestPoolCancellationBeforeBorrow(t *testing.T) {
	for _, wait := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_wait", true: "wait"}[wait], func(t *testing.T) {
			p, physical := warmCancellationPool(t, wait)
			called := false
			p.TestOnBorrowContext = func(context.Context, Conn, time.Time) error { called = true; return nil }
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			c, err := p.GetContext(ctx)
			if !errors.Is(err, context.Canceled) || !errors.Is(c.Err(), context.Canceled) {
				t.Fatal(err)
			}
			if called || p.ActiveCount() != 3 || p.IdleCount() != 3 || len(*physical) != 3 {
				t.Fatalf("canceled borrow changed pool: %+v, called=%v", p.Stats(), called)
			}
			for _, c := range *physical {
				if atomic.LoadInt32(&c.closed) != 0 {
					t.Fatal("closed healthy connection")
				}
			}
		})
	}
}

func TestPoolCancellationDuringBorrowCheck(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "successful_check", true: "failed_check"}[failed], func(t *testing.T) {
			p, physical := warmCancellationPool(t, true)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			var lastUsed time.Time
			p.TestOnBorrowContext = func(_ context.Context, c Conn, last time.Time) error {
				calls++
				lastUsed = last
				cancel()
				if failed {
					return context.Canceled
				}
				return nil
			}
			_, err := p.GetContext(ctx)
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			want := 3
			if failed {
				want = 2
			}
			if calls != 1 || p.ActiveCount() != want || p.IdleCount() != want || len(*physical) != 3 {
				t.Fatalf("continued after cancellation: %+v, checks=%d, dials=%d", p.Stats(), calls, len(*physical))
			}
			if !failed {
				p.TestOnBorrowContext = func(_ context.Context, _ Conn, last time.Time) error {
					if !last.Equal(lastUsed) {
						t.Error("rollback refreshed idle age")
					}
					return nil
				}
			} else {
				p.TestOnBorrowContext = nil
			}
			// Borrow the entire capacity to expose a leaked or duplicate reservation.
			deadline, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			var got []Conn
			for i := 0; i < 3; i++ {
				c, err := p.GetContext(deadline)
				if err != nil {
					t.Fatal("lost reservation", err)
				}
				got = append(got, c)
				p.TestOnBorrowContext = nil
			}
			for _, c := range got {
				c.Close()
			}
			if p.ActiveCount() != 3 || p.IdleCount() != 3 {
				t.Fatal(p.Stats())
			}
		})
	}
}

func TestPoolCancellationAfterDial(t *testing.T) {
	for _, wait := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		physical := &cancellationTestConn{}
		p := &Pool{MaxIdle: 1, MaxActive: 1, Wait: wait, Dial: func() (Conn, error) { cancel(); return physical, nil }}
		_, err := p.GetContext(ctx)
		if !errors.Is(err, context.Canceled) || p.ActiveCount() != 0 || atomic.LoadInt32(&physical.closed) != 1 {
			t.Fatalf("unowned dial leaked: err=%v, stats=%+v, closes=%d", err, p.Stats(), physical.closed)
		}
		p.Dial = func() (Conn, error) { return &cancellationTestConn{}, nil }
		ctx2, stop := context.WithTimeout(context.Background(), time.Second)
		c, err := p.GetContext(ctx2)
		stop()
		if err != nil {
			t.Fatal(err)
		}
		c.Close()
		p.Close()
	}
}

func TestPoolCancellationWaitingAndClose(t *testing.T) {
	for i := 0; i < 200; i++ {
		p := &Pool{MaxIdle: 1, MaxActive: 1, Wait: true, Dial: func() (Conn, error) { return &cancellationTestConn{}, nil }}
		first, err := p.GetContext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(4)
		go func() {
			defer wg.Done()
			<-start
			c, err := p.GetContext(ctx)
			if err == nil {
				c.Close()
			}
		}()
		go func() { defer wg.Done(); <-start; cancel() }()
		go func() { defer wg.Done(); <-start; first.Close() }()
		go func() { defer wg.Done(); <-start; p.Close() }()
		close(start)
		wg.Wait()
		if p.ActiveCount() != 0 || p.IdleCount() != 0 {
			t.Fatal(p.Stats())
		}
	}
}

func TestPoolCancellationFirstUseAndClose(t *testing.T) {
	for i := 0; i < 200; i++ {
		p := &Pool{MaxActive: 1, Wait: true, Dial: func() (Conn, error) { return &cancellationTestConn{}, nil }}
		var wg sync.WaitGroup
		wg.Add(2)
		start := make(chan struct{})
		go func() {
			defer wg.Done()
			<-start
			c, err := p.GetContext(context.Background())
			if err == nil {
				c.Close()
			}
		}()
		go func() { defer wg.Done(); <-start; p.Close() }()
		close(start)
		wg.Wait()
		if p.ActiveCount() != 0 {
			t.Fatal(p.Stats())
		}
	}
}

type expiredPoolContext struct{ context.Context }

func (expiredPoolContext) Deadline() (time.Time, bool) { return time.Now().Add(-time.Second), true }
func TestPoolCancellationExpiredDeadline(t *testing.T) {
	p := &Pool{Dial: func() (Conn, error) { t.Fatal("dial after expired deadline"); return nil, nil }}
	_, err := p.GetContext(expiredPoolContext{context.Background()})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
