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
	"bytes"
	"container/list"
	"crypto/rand"
	"crypto/sha1"
	"errors"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/internal"
)

var nowFunc = time.Now // for testing

// ErrPoolExhausted is returned from a pool connection method (Do, Send,
// Receive, Flush, Err) when the maximum number of database connections in the
// pool has been reached.
var ErrPoolExhausted = errors.New("redigo: connection pool exhausted")

var (
	errPoolClosed = errors.New("redigo: connection pool closed")
	errConnClosed = errors.New("redigo: connection closed")
)

// Pool maintains a pool of connections. The application calls the Get method
// to get a connection from the pool and the connection's Close method to
// return the connection's resources to the pool.
//
// The following example shows how to use a pool in a web application. The
// application creates a pool at application startup and makes it available to
// request handlers using a global variable.
//
//  func newPool(server, password string) *redis.Pool {
//      return &redis.Pool{
//          MaxIdle: 3,
//          IdleTimeout: 240 * time.Second,
//          Dial: func () (redis.Conn, error) {
//              c, err := redis.Dial("tcp", server)
//              if err != nil {
//                  return nil, err
//              }
//              if _, err := c.Do("AUTH", password); err != nil {
//                  c.Close()
//                  return nil, err
//              }
//              return c, err
//          },
//          TestOnBorrow: func(c redis.Conn, t time.Time) error {
//              _, err := c.Do("PING")
//              return err
//          },
//      }
//  }
//
//  var (
//      pool *redis.Pool
//      redisServer = flag.String("redisServer", ":6379", "")
//      redisPassword = flag.String("redisPassword", "", "")
//  )
//
//  func main() {
//      flag.Parse()
//      pool = newPool(*redisServer, *redisPassword)
//      ...
//  }
//
// A request handler gets a connection from the pool and closes the connection
// when the handler is done:
//
//  func serveHome(w http.ResponseWriter, r *http.Request) {
//      conn := pool.Get()
//      defer conn.Close()
//      ....
//  }
//
type Pool struct {

	// Dial is an application supplied function for creating and configuring a
	// connection
	Dial func() (Conn, error)

	// TestOnBorrow is an optional application supplied function for checking
	// the health of an idle connection before the connection is used again by
	// the application. Argument t is the time that the connection was returned
	// to the pool. If the function returns an error, then the connection is
	// closed.
	TestOnBorrow func(c Conn, t time.Time) error

	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	// If Wait is true and the pool is at the MaxIdle limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	Wait bool

	// mu protects fields defined below.
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	active int

	// Stack of idleConn with most recently used at the front.
	idle list.List
}

type idleConn struct {
	c Conn
	t time.Time
}

// NewPool creates a new pool. This function is deprecated. Applications should
// initialize the Pool fields directly as shown in example.
func NewPool(newFn func() (Conn, error), maxIdle int) *Pool {
	return &Pool{Dial: newFn, MaxIdle: maxIdle}
}

// Get gets a connection. The application must close the returned connection.
// This method always returns a valid connection so that applications can defer
// error handling to the first use of the connection. If there is an error
// getting an underlying connection, then the connection Err, Do, Send, Flush
// and Receive methods return that error.
func (p *Pool) Get() Conn {
	c, err := p.get()
	if err != nil {
		return errorConnection{err}
	}
	return &pooledConnection{p: p, c: c}
}

// ActiveCount returns the number of active connections in the pool.
func (p *Pool) ActiveCount() int {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()
	return active
}

// Close releases the resources used by the pool.
func (p *Pool) Close() error {
	p.closed = true
	p.closeAll()
	return nil
}

// Close all connections.
func (p *Pool) closeAll() {
	p.mu.Lock()
	idle := p.idle
	p.idle.Init()
	p.active -= idle.Len()
	if p.cond != nil {
		p.cond.Broadcast()
	}
	p.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(idleConn).c.Close()
	}
}

// release decrements the active count and signals waiters. The caller must
// hold p.mu during the call.
func (p *Pool) release() {
	p.active -= 1
	if p.cond != nil {
		p.cond.Signal()
	}
}

// get prunes stale connections and returns a connection from the idle list or
// creates a new connection.
func (p *Pool) get() (Conn, error) {
	p.mu.Lock()

	// Prune stale connections.

	if timeout := p.IdleTimeout; timeout > 0 {
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Back()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			if ic.t.Add(timeout).After(nowFunc()) {
				break
			}
			p.idle.Remove(e)
			p.release()
			p.mu.Unlock()
			ic.c.Close()
			p.mu.Lock()
		}
	}

	for {

		// Get idle connection.

		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Front()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			p.idle.Remove(e)
			test := p.TestOnBorrow
			p.mu.Unlock()
			if test == nil || test(ic.c, ic.t) == nil {
				return ic.c, nil
			}
			ic.c.Close()
			p.mu.Lock()
			p.release()
		}

		// Check for pool closed before dialing a new connection.

		if p.closed {
			p.mu.Unlock()
			return nil, errors.New("redigo: get on closed pool")
		}

		// Dial new connection if under limit.

		if p.MaxActive == 0 || p.active < p.MaxActive {
			dial := p.Dial
			p.active += 1
			p.mu.Unlock()
			c, err := dial()
			if err != nil {
				p.mu.Lock()
				p.release()
				p.mu.Unlock()
				c = nil
			}
			return c, err
		}

		if !p.Wait {
			p.mu.Unlock()
			return nil, ErrPoolExhausted
		}

		if p.cond == nil {
			p.cond = sync.NewCond(&p.mu)
		}
		p.cond.Wait()
	}
}

func (p *Pool) put(c Conn, forceClose bool) error {
	err := c.Err()
	return p.putCommon(err, c, forceClose)
}

// putCommon contains operations in common with the Pool and the
// SentinelAwarePool.
func (p *Pool) putCommon(err error, c Conn, forceClose bool) error {
	p.mu.Lock()
	if !p.closed && err == nil && !forceClose {
		p.idle.PushFront(idleConn{t: nowFunc(), c: c})
		if p.idle.Len() > p.MaxIdle {
			c = p.idle.Remove(p.idle.Back()).(idleConn).c
		} else {
			c = nil
		}
	}

	if c == nil {
		if p.cond != nil {
			p.cond.Signal()
		}
		p.mu.Unlock()
		return nil
	}

	p.release()
	p.mu.Unlock()
	return c.Close()
}

// The SentinelAwarePool is identical to the normal Pool implementation except
// the configuration of the monitored master is retained. Change in this
// configuration causes a purge of all stored idle connections. The typical
// use of the UpdateMaster method will be in the Dial() method, see the
// example below.
//
// A TestOnReturn function entry point is also supplied. Users are encouraged
// to put a role test in TestOnReturn to prevent connections that persist
// through unexpected role changes without fatal errors from making it back
// into the pool.
//
// Active connections will not be impacted by the purge. The user is expected
// to be aware of this and be able to handle interruptions to the redis
// connection in the event of a failover, which is _not_ seamless.
//
// Example of usage to contact master:
//
//  func newSentinelPool() *redis.SentinelAwarePool {
//	  sentConn, err := redis.NewSentinelClient("tcp",
//      []string{"127.0.0.1:26379", "127.0.0.2:26379", "127.0.0.2.26379"},
//      nil, 100*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond)
//
//	  if err != nil {
//	    log.Println("Failed to connect to redis sentinel:", err)
//	    return nil
//	  }
//
//	  sap := &redis.SentinelAwarePool{
//	    Pool : redis.Pool{
//	      MaxIdle : 10,
//	      IdleTimeout : 240 * time.Second,
//	      TestOnBorrow: func(c redis.Conn, t time.Time) error {
//	        if !redis.TestRole(c, "master") {
//	          return errors.New("Failed role check")
//	        } else {
//	          return nil
//	        }
//	      },
//	    },
//	    TestOnReturn : func(c redis.Conn) error {
//	      if !redis.TestRole(c, "master") {
//	        return errors.New("Failed role check")
//	      } else {
//	        return nil
//	      }
//	    },
//	  }
//
//	  sap.Pool.Dial = func() (redis.Conn, error) {
//	    masterAddr, err := sentConn.QueryConfForMaster("kvmaster")
//	    if err != nil {
//	      return nil, err
//	    }
//	    sap.UpdateMaster(masterAddr)
//
//	    c, err := redis.Dial("tcp", masterAddr)
//	    if err != nil {
//	      return nil, err
//	    }
//	    if !redis.TestRole(c, "master") {
//	      return nil, errors.New("Failed role check")
//	    }
//	    return c, err
//	  }
//
//	  return sap
//	}
//
// The SentinelClient used above will try all connected sentinels when
// querying for the master address. However, if no sentinels can be
// contacted, it may be impossible to dial for some time. The
// SentinelClient should automatically recover once a sentinel returns.
type SentinelAwarePool struct {
	Pool
	// TestOnReturn is a user-supplied function to test a connection before
	// returning it to the pool. Like TestOnBorrow, TestOnReturn will close the
	// connection if an error is observed. It is strongly suggested to test the
	// role of a connection on return, especially if the sentinels in use are
	// older than 2.8.12.
	TestOnReturn func(c Conn) error

	masterAddr string
}

// UpdateMaster updates the internal accounting of the SentinelAwarePool,
// which may trigger a flush of all idle connections if the master
// changes.
func (sap *SentinelAwarePool) UpdateMaster(addr string) {
	if addr != sap.masterAddr {
		sap.masterAddr = addr
		sap.closeAll()
	}
}

// Entrypoint for TestOnReturn, any error here causes the connection to be
// closed instead of being returned to the pool.
func (sap *SentinelAwarePool) testConn(c Conn) error {
	err := c.Err()
	if err != nil {
		return err
	}
	return sap.TestOnReturn(c)
}

func (sap *SentinelAwarePool) put(c Conn, forceClose bool) error {
	err := sap.testConn(c)
	return sap.putCommon(err, c, forceClose)
}

// New interface to allow the pooledConnection to interact with both
// the Pool and the SentinelAwarePool.
type connToPool interface {
	put(Conn, bool) error
}

type pooledConnection struct {
	p     connToPool
	c     Conn
	state int
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
		io.WriteString(h, "Oops, rand failed. Use time instead.")
		io.WriteString(h, strconv.FormatInt(time.Now().UnixNano(), 10))
		sentinel = h.Sum(nil)
	}
}

func (pc *pooledConnection) Close() error {
	c := pc.c
	if _, ok := c.(errorConnection); ok {
		return nil
	}
	pc.c = errorConnection{errConnClosed}

	if pc.state&internal.MultiState != 0 {
		c.Send("DISCARD")
		pc.state &^= (internal.MultiState | internal.WatchState)
	} else if pc.state&internal.WatchState != 0 {
		c.Send("UNWATCH")
		pc.state &^= internal.WatchState
	}
	if pc.state&internal.SubscribeState != 0 {
		c.Send("UNSUBSCRIBE")
		c.Send("PUNSUBSCRIBE")
		// To detect the end of the message stream, ask the server to echo
		// a sentinel value and read until we see that value.
		sentinelOnce.Do(initSentinel)
		c.Send("ECHO", sentinel)
		c.Flush()
		for {
			p, err := c.Receive()
			if err != nil {
				break
			}
			if p, ok := p.([]byte); ok && bytes.Equal(p, sentinel) {
				pc.state &^= internal.SubscribeState
				break
			}
		}
	}
	c.Do("")
	pc.p.put(c, pc.state != 0)
	return nil
}

func (pc *pooledConnection) Err() error {
	return pc.c.Err()
}

func (pc *pooledConnection) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	ci := internal.LookupCommandInfo(commandName)
	pc.state = (pc.state | ci.Set) &^ ci.Clear
	return pc.c.Do(commandName, args...)
}

func (pc *pooledConnection) Send(commandName string, args ...interface{}) error {
	ci := internal.LookupCommandInfo(commandName)
	pc.state = (pc.state | ci.Set) &^ ci.Clear
	return pc.c.Send(commandName, args...)
}

func (pc *pooledConnection) Flush() error {
	return pc.c.Flush()
}

func (pc *pooledConnection) Receive() (reply interface{}, err error) {
	return pc.c.Receive()
}

type errorConnection struct{ err error }

func (ec errorConnection) Do(string, ...interface{}) (interface{}, error) { return nil, ec.err }
func (ec errorConnection) Send(string, ...interface{}) error              { return ec.err }
func (ec errorConnection) Err() error                                     { return ec.err }
func (ec errorConnection) Close() error                                   { return ec.err }
func (ec errorConnection) Flush() error                                   { return ec.err }
func (ec errorConnection) Receive() (interface{}, error)                  { return nil, ec.err }
