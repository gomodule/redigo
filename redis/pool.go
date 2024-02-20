// Copyright 2012 Gary Burd
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

package redis

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"
)

var (
	nowFunc           = time.Now // for testing
	errMonitorEnabled = errors.New("redigo: monitor enabled")
	errConnClosed     = errors.New("redigo: connection closed")

	// ErrPoolClosed is returned from a pool get when the pool is closed.
	ErrPoolClosed = errors.New("redigo: get on closed pool")

	// ErrPoolExhausted is returned from a pool connection method (Do, Send,
	// Receive, Flush, Err) when the maximum number of database connections in the
	// pool has been reached.
	ErrPoolExhausted = errors.New("redigo: connection pool exhausted")
)

// PoolOption is a function that configures a Pool.
type PoolOption func(*Pool) error

// Dial is an application supplied function for creating and configuring a
// connection.
//
// The connection returned from Dial must not be in a special state
// (subscribed to pubsub channel, transaction started, ...).
func PoolDial(fn func(options ...DialOption) (*Conn, error)) PoolOption {
	return PoolDialContext(func(_ context.Context, options ...DialOption) (*Conn, error) {
		return fn(options...)
	})
}

// DialContext is an application supplied function for creating and configuring a
// connection with the given context.
//
// The connection returned from Dial must not be in a special state
// (subscribed to pubsub channel, transaction started, ...).
func PoolDialContext(fn func(ctx context.Context, options ...DialOption) (*Conn, error)) PoolOption {
	return func(p *Pool) error {
		p.dialContext = fn
		return nil
	}
}

// TestOnBorrow is an optional application supplied function for checking
// the health of an idle connection before the connection is used again by
// the application. Argument t is the time that the connection was returned
// to the pool. If the function returns an error, then the connection is
// closed.
func TestOnBorrow(fn func(ctx context.Context, c *Conn, t time.Time) error) PoolOption {
	return func(p *Pool) error {
		p.testOnBorrow = fn
		return nil
	}
}

// Maximum number of idle connections in the pool.
func MaxIdle(n int) PoolOption {
	return func(p *Pool) error {
		p.maxIdle = n
		return nil
	}
}

// Maximum number of connections allocated by the pool at a given time.
// When zero, there is no limit on the number of connections in the pool.
func MaxActive(n int) PoolOption {
	return func(p *Pool) error {
		p.maxActive = n
		return nil
	}
}

// Close connections after remaining idle for this duration. If the value
// is zero, then idle connections are not closed. Applications should set
// the timeout to a value less than the server's timeout.
func IdleTimeout(d time.Duration) PoolOption {
	return func(p *Pool) error {
		p.idleTimeout = d
		return nil
	}
}

// If Wait is true and the pool is at the MaxActive limit, then Get() waits
// for a connection to be returned to the pool before returning.
func Wait(b bool) PoolOption {
	return func(p *Pool) error {
		p.wait = b
		return nil
	}
}

// Close connections older than this duration. If the value is zero, then
// the pool does not close connections based on age.
func MaxConnLifetime(d time.Duration) PoolOption {
	return func(p *Pool) error {
		p.maxConnLifetime = d
		return nil
	}
}

func DialOptions(options ...DialOption) PoolOption {
	return func(p *Pool) error {
		p.dialOptions = options
		return nil
	}
}

// Pool maintains a pool of connections. The application calls the Get method
// to get a connection from the pool and the connection's Close method to
// return the connection's resources to the pool.
//
// The following example shows how to use a pool in a web application. The
// application creates a pool at application startup and makes it available to
// request handlers using a package level variable. The pool configuration used
// here is an example, not a recommendation.
//
//	func newPool(addr string) *redis.Pool {
//	  return &redis.Pool{
//	    MaxIdle: 3,
//	    IdleTimeout: 240 * time.Second,
//	    // Dial or DialContext must be set. When both are set, DialContext takes precedence over Dial.
//	    Dial: func () (redis.Conn, error) { return redis.Dial("tcp", addr) },
//	  }
//	}
//
//	var (
//	  pool *redis.Pool
//	  redisServer = flag.String("redisServer", ":6379", "")
//	)
//
//	func main() {
//	  flag.Parse()
//	  pool = newPool(*redisServer)
//	  ...
//	}
//
// A request handler gets a connection from the pool and closes the connection
// when the handler is done:
//
//	func serveHome(w http.ResponseWriter, r *http.Request) {
//	    conn := pool.Get()
//	    defer conn.Close()
//	    ...
//	}
//
// Use the Dial function to authenticate connections with the AUTH command or
// select a database with the SELECT command:
//
//	pool := &redis.Pool{
//	  // Other pool configuration not shown in this example.
//	  Dial: func () (redis.Conn, error) {
//	    c, err := redis.Dial("tcp", server)
//	    if err != nil {
//	      return nil, err
//	    }
//	    if _, err := c.Do("AUTH", password); err != nil {
//	      c.Close()
//	      return nil, err
//	    }
//	    if _, err := c.Do("SELECT", db); err != nil {
//	      c.Close()
//	      return nil, err
//	    }
//	    return c, nil
//	  },
//	}
//
// Use the TestOnBorrow function to check the health of an idle connection
// before the connection is returned to the application. This example PINGs
// connections that have been idle more than a minute:
//
//	pool := &redis.Pool{
//	  // Other pool configuration not shown in this example.
//	  TestOnBorrow: func(c redis.Conn, t time.Time) error {
//	    if time.Since(t) < time.Minute {
//	      return nil
//	    }
//	    _, err := c.Do("PING")
//	    return err
//	  },
//	}
type Pool struct {
	// Configuration settings.
	dialContext     func(ctx context.Context, options ...DialOption) (*Conn, error)
	testOnBorrow    func(ctx context.Context, c *Conn, t time.Time) error
	maxIdle         int
	maxActive       int
	idleTimeout     time.Duration
	wait            bool
	maxConnLifetime time.Duration
	dialOptions     []DialOption

	// State.
	mu           sync.Mutex    // mu protects the following fields
	closed       bool          // set to true when the pool is closed.
	active       int           // the number of open connections in the pool
	initOnce     sync.Once     // the init ch once func
	ch           chan struct{} // limits open connections when p.Wait is true
	idle         idleList      // idle connections
	waitCount    int64         // total number of connections waited for.
	waitDuration time.Duration // total time waited for new connections.
}

// NewPool creates a new pool with the given options.
func NewPool(options ...PoolOption) (*Pool, error) {
	p := &Pool{
		maxIdle:     10,
		idleTimeout: 240 * time.Second,
	}

	for _, option := range options {
		if err := option(p); err != nil {
			return nil, err
		}
	}

	return p, nil
}

// Get gets a connection. The application must close the returned connection.
// This method always returns a valid connection so that applications can defer
// error handling to the first use of the connection. If there is an error
// getting an underlying connection, then the connection Err, Do, Send, Flush
// and Receive methods return that error.
func (p *Pool) Get() *PoolConn {
	// GetContext returns errorConn in the first argument when an error occurs.
	c, _ := p.GetContext(context.Background())
	return c
}

// GetContext gets a connection using the provided context.
//
// The provided Context must be non-nil. If the context expires before the
// connection is complete, an error is returned. Any expiration on the context
// will not affect the returned connection.
//
// If the function completes without error, then the application must close the
// returned connection.
func (p *Pool) GetContext(ctx context.Context) (*PoolConn, error) {
	// Wait until there is a vacant connection in the pool.
	waited, err := p.waitVacantConn(ctx)
	if err != nil {
		return newErrorConn(err), err
	}

	p.mu.Lock()

	if waited > 0 {
		p.waitCount++
		p.waitDuration += waited
	}

	// Prune stale connections at the back of the idle list.
	if p.idleTimeout > 0 {
		n := p.idle.count
		for i := 0; i < n && p.idle.back != nil && p.idle.back.t.Add(p.idleTimeout).Before(nowFunc()); i++ {
			pc := p.idle.back
			p.idle.popBack()
			p.mu.Unlock()
			pc.conn.Close() //nolint: errcheck
			p.mu.Lock()
			p.active--
		}
	}

	// Get idle connection from the front of idle list.
	for p.idle.front != nil {
		pc := p.idle.front
		p.idle.popFront()
		p.mu.Unlock()
		if (p.testOnBorrow == nil || p.testOnBorrow(ctx, pc.conn, pc.t) == nil) &&
			(p.maxConnLifetime == 0 || nowFunc().Sub(pc.created) < p.maxConnLifetime) {
			return newActiveConn(pc), nil
		}
		pc.conn.Close() //nolint: errcheck
		p.mu.Lock()
		p.active--
	}

	// Check for pool closed before dialing a new connection.
	if p.closed {
		p.mu.Unlock()
		return newErrorConn(ErrPoolClosed), ErrPoolClosed
	}

	// Handle limit for p.Wait == false.
	if !p.wait && p.maxActive > 0 && p.active >= p.maxActive {
		p.mu.Unlock()
		return newErrorConn(ErrPoolExhausted), ErrPoolExhausted
	}

	p.active++
	p.mu.Unlock()
	c, err := p.dialContext(ctx, p.dialOptions...)
	if err != nil {
		p.mu.Lock()
		p.active--
		if p.ch != nil && !p.closed {
			p.ch <- struct{}{}
		}
		p.mu.Unlock()
		return newErrorConn(err), err
	}

	return newActiveConn(newPoolConn(p, c, nowFunc())), nil
}

// PoolStats contains pool statistics.
type PoolStats struct {
	// ActiveCount is the number of connections in the pool. The count includes
	// idle connections and connections in use.
	ActiveCount int

	// IdleCount is the number of idle connections in the pool.
	IdleCount int

	// WaitCount is the total number of connections waited for.
	// This value is currently not guaranteed to be 100% accurate.
	WaitCount int64

	// WaitDuration is the total time blocked waiting for a new connection.
	// This value is currently not guaranteed to be 100% accurate.
	WaitDuration time.Duration
}

// Stats returns pool's statistics.
func (p *Pool) Stats() PoolStats {
	p.mu.Lock()
	stats := PoolStats{
		ActiveCount:  p.active,
		IdleCount:    p.idle.count,
		WaitCount:    p.waitCount,
		WaitDuration: p.waitDuration,
	}
	p.mu.Unlock()

	return stats
}

// ActiveCount returns the number of connections in the pool. The count
// includes idle connections and connections in use.
func (p *Pool) ActiveCount() int {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()
	return active
}

// IdleCount returns the number of idle connections in the pool.
func (p *Pool) IdleCount() int {
	p.mu.Lock()
	idle := p.idle.count
	p.mu.Unlock()
	return idle
}

// Close releases the resources used by the pool.
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.active -= p.idle.count
	pc := p.idle.front
	p.idle.count = 0
	p.idle.front, p.idle.back = nil, nil
	if p.ch != nil {
		close(p.ch)
	}
	p.mu.Unlock()
	for ; pc != nil; pc = pc.next {
		pc.conn.Close() //nolint: errcheck
	}
	return nil
}

func (p *Pool) lazyInit() {
	p.initOnce.Do(func() {
		p.ch = make(chan struct{}, p.maxActive)
		if p.closed {
			close(p.ch)
		} else {
			for i := 0; i < p.maxActive; i++ {
				p.ch <- struct{}{}
			}
		}
	})
}

// waitVacantConn waits for a vacant connection in pool if waiting
// is enabled and pool size is limited, otherwise returns instantly.
// If ctx expires before that, an error is returned.
//
// If there were no vacant connection in the pool right away it returns the time spent waiting
// for that connection to appear in the pool.
func (p *Pool) waitVacantConn(ctx context.Context) (waited time.Duration, err error) {
	if !p.wait || p.maxActive <= 0 {
		// No wait or no connection limit.
		return 0, nil
	}

	p.lazyInit()

	// wait indicates if we believe it will block so its not 100% accurate
	// however for stats it should be good enough.
	wait := len(p.ch) == 0
	var start time.Time
	if wait {
		start = time.Now()
	}

	select {
	case <-p.ch:
		// Additionally check that context hasn't expired while we were waiting,
		// because `select` picks a random `case` if several of them are "ready".
		select {
		case <-ctx.Done():
			p.ch <- struct{}{}
			return 0, ctx.Err()
		default:
		}
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	if wait {
		return time.Since(start), nil
	}
	return 0, nil
}

func (p *Pool) put(pc *poolConn, forceClose bool) error {
	p.mu.Lock()
	if !p.closed && !forceClose {
		pc.t = nowFunc()
		p.idle.pushFront(pc)
		if p.idle.count > p.maxIdle {
			pc = p.idle.back
			p.idle.popBack()
		} else {
			pc = nil
		}
	}

	var err error
	if pc != nil {
		p.mu.Unlock()
		err = pc.conn.Close()
		p.mu.Lock()
		p.active--
	}

	if p.ch != nil && !p.closed {
		p.ch <- struct{}{}
	}
	p.mu.Unlock()
	return err
}

var (
	sentinel     []byte
	sentinelOnce sync.Once
)

func initSentinel() {
	p := make([]byte, 64)
	if _, err := rand.Read(p); err == nil {
		sentinel = p
	} else {
		h := sha1.New()
		io.WriteString(h, "Oops, rand failed. Use time instead.")       // nolint: errcheck
		io.WriteString(h, strconv.FormatInt(time.Now().UnixNano(), 10)) // nolint: errcheck
		sentinel = h.Sum(nil)
	}
}

func (pc *PoolConn) activeClose() error {
	pconn := pc.pc
	if pc.err != nil {
		return pc.err // Likely already closed.
	}
	pc.pc = nil

	defer func() {
		pc.err = errConnClosed
	}()

	if pconn.conn.state != 0 {
		// Reset the connection state.
		if err := pconn.conn.reset(); err != nil {
			// Force close the connection.
			pconn.p.put(pconn, true) //nolint: errcheck
			return fmt.Errorf("reset: %w", err)
		}
	}

	// Return the connection to the pool.
	return pconn.p.put(pconn, pconn.conn.Err() != nil)
}

func (pc *PoolConn) activeErr() error {
	if pc.err != nil {
		return pc.err
	}
	return pc.pc.conn.Err()
}

func (pc *PoolConn) activeDo(commandName string, args ...interface{}) (reply interface{}, err error) {
	return pc.DoContext(context.Background(), commandName, args...)
}

func (pc *PoolConn) activeDoContext(ctx context.Context, commandName string, args ...interface{}) (reply interface{}, err error) {
	if pc.err != nil {
		return nil, pc.err
	}

	return pc.pc.conn.DoContext(ctx, commandName, args...)
}

func (pc *PoolConn) activeSend(commandName string, args ...interface{}) error {
	if pc.err != nil {
		return pc.err
	}

	return pc.pc.conn.Send(commandName, args...)
}

func (pc *PoolConn) activeFlush() error {
	if pc.err != nil {
		return pc.err
	}

	return pc.pc.conn.Flush()
}

func (pc *PoolConn) activeReceive() (reply interface{}, err error) {
	return pc.activeReceiveContext(context.Background())
}

func (pc *PoolConn) activeReceiveContext(ctx context.Context) (reply interface{}, err error) {
	if pc.err != nil {
		return nil, pc.err
	}
	return pc.pc.conn.ReceiveContext(ctx)
}

type PoolConn struct {
	// pc is the underling pool connection which will be returned to the pool
	// when the connection is closed.
	pc *poolConn

	// err is the error returned if we failed to acquire the connection.
	err error

	// Base / chained connection methods.
	do                 func(string, ...interface{}) (interface{}, error)
	doContext          func(context.Context, string, ...interface{}) (interface{}, error)
	send               func(string, ...interface{}) error
	error              func() error
	close              func() error
	flush              func() error
	receive            func() (interface{}, error)
	receiveContext     func(context.Context) (interface{}, error)
	receiveWithTimeout func(time.Duration) (interface{}, error)
}

func newPoolConn(pool *Pool, conn *Conn, created time.Time) *poolConn {
	return &poolConn{
		p:       pool,
		conn:    conn,
		created: created,
	}
}

func (p *PoolConn) Do(commandName string, args ...interface{}) (interface{}, error) {
	return p.do(commandName, args...)
}

func (p *PoolConn) DoContext(ctx context.Context, commandName string, args ...interface{}) (interface{}, error) {
	return p.doContext(ctx, commandName, args...)
}

func (p *PoolConn) Send(commandName string, args ...interface{}) error {
	return p.send(commandName, args...)
}

func (p *PoolConn) Err() error {
	return p.error()
}

func (p *PoolConn) Close() error {
	return p.close()
}

func (p *PoolConn) Flush() error {
	return p.flush()
}

func (p *PoolConn) Receive() (interface{}, error) {
	return p.receive()
}

func (p *PoolConn) ReceiveContext(ctx context.Context) (interface{}, error) {
	return p.receiveContext(ctx)
}

func (p *PoolConn) ReceiveWithTimeout(timeout time.Duration) (interface{}, error) {
	return p.receiveWithTimeout(timeout)
}

type poolConn struct {
	p          *Pool
	conn       *Conn
	t          time.Time
	created    time.Time
	next, prev *poolConn
}

func newActiveConn(pc *poolConn) *PoolConn {
	c := &PoolConn{
		pc: pc,
	}

	c.close = c.activeClose
	c.do = c.activeDo
	c.doContext = c.activeDoContext
	c.error = c.activeErr
	c.flush = c.activeFlush
	c.receive = c.activeReceive
	c.receiveContext = c.activeReceiveContext
	c.send = c.activeSend

	return c
}

func newErrorConn(err error) *PoolConn {
	pc := &PoolConn{
		err: err,
	}

	pc.close = pc.errClose
	pc.do = pc.errDo
	pc.doContext = pc.errDoContext
	pc.error = pc.errErr
	pc.flush = pc.errFlush
	pc.receive = pc.errReceive
	pc.receiveContext = pc.errReceiveContext
	pc.send = pc.errSend

	return pc
}

func (pc *PoolConn) errDo(string, ...interface{}) (interface{}, error) {
	return nil, pc.err
}

func (pc *PoolConn) errDoContext(context.Context, string, ...interface{}) (interface{}, error) {
	return nil, pc.err
}

func (pc *PoolConn) errSend(string, ...interface{}) error {
	return pc.err
}

func (pc *PoolConn) errErr() error {
	return pc.err
}

func (pc *PoolConn) errClose() error {
	return nil
}

func (pc *PoolConn) errFlush() error {
	return pc.err
}

func (pc *PoolConn) errReceive() (interface{}, error) {
	return nil, pc.err
}

func (pc *PoolConn) errReceiveContext(context.Context) (interface{}, error) {
	return nil, pc.err
}

type idleList struct {
	count       int
	front, back *poolConn
}

func (l *idleList) pushFront(pc *poolConn) {
	pc.next = l.front
	pc.prev = nil
	if l.count == 0 {
		l.back = pc
	} else {
		l.front.prev = pc
	}
	l.front = pc
	l.count++
}

func (l *idleList) popFront() {
	pc := l.front
	l.count--
	if l.count == 0 {
		l.front, l.back = nil, nil
	} else {
		pc.next.prev = nil
		l.front = pc.next
	}
	pc.next, pc.prev = nil, nil
}

func (l *idleList) popBack() {
	pc := l.back
	l.count--
	if l.count == 0 {
		l.front, l.back = nil, nil
	} else {
		pc.prev.next = nil
		l.back = pc.prev
	}
	pc.next, pc.prev = nil, nil
}
