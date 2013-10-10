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
	"container/list"
	"errors"
	"sync"
	"time"
)

var nowFunc = time.Now // for testing

// ErrPoolExhausted is returned from pool connection methods when the maximum
// number of database connections in the pool has been reached.
var ErrPoolExhausted = errors.New("redigo: connection pool exhausted")

var errPoolClosed = errors.New("redigo: connection pool closed")

// Pool maintains a pool of connections. The application calls the Get method
// to get a connection from the pool and the connection's Close method to
// return the connection's resources to the pool.
//
// The following example shows how to use a pool in a web application. The
// application creates a pool at application startup and makes it available to
// request handlers, possibly using a global variable:
//
//      var server string           // host:port of server
//      var password string
//      ...
//
//      pool = &redis.Pool{
//              MaxIdle: 3,
//              IdleTimeout: 240 * time.Second,
//              Dial: func () (redis.Conn, error) {
//                  c, err := redis.Dial("tcp", server)
//                  if err != nil {
//                      return nil, err
//                  }
//                  if _, err := c.Do("AUTH", password); err != nil {
//                      c.Close()
//                      return nil, err
//                  }
//                  return c, err
//              },
//				TestOnBorrow: func(c redis.Conn, t time.Time) error {
//				    _, err := c.Do("PING")
//                  return err
//			    },
//          }
//
// This pool has a maximum of three connections to the server specified by the
// variable "server". Each connection is authenticated using a password.
//
// A request handler gets a connection from the pool and closes the connection
// when the handler is done:
//
//  conn := pool.Get()
//  defer conn.Close()
//  // do something with the connection
type Pool struct {

	// Dial is an application supplied function for creating new connections.
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

	// mu protects fields defined below.
	mu     sync.Mutex
	closed bool
	active int

	// Stack of idleConn with most recently used at the front.
	idle list.List
}

type idleConn struct {
	c Conn
	t time.Time
}

// NewPool returns a pool that uses newPool to create connections as needed.
// The pool keeps a maximum of maxIdle idle connections.
func NewPool(newFn func() (Conn, error), maxIdle int) *Pool {
	return &Pool{Dial: newFn, MaxIdle: maxIdle}
}

// Get gets a connection from the pool.
func (p *Pool) Get() Conn {
	return &pooledConnection{p: p}
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
	p.mu.Lock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.active -= idle.Len()
	p.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(idleConn).c.Close()
	}
	return nil
}

// get prunes stale connections and returns a connection from the idle list or
// creates a new connection.
func (p *Pool) get() (Conn, error) {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("redigo: get on closed pool")
	}

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
			p.active -= 1
			p.mu.Unlock()
			ic.c.Close()
			p.mu.Lock()
		}
	}

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
		p.active -= 1
	}

	if p.MaxActive > 0 && p.active >= p.MaxActive {
		p.mu.Unlock()
		return nil, ErrPoolExhausted
	}

	// No idle connection, create new.

	dial := p.Dial
	p.active += 1
	p.mu.Unlock()
	c, err := dial()
	if err != nil {
		p.mu.Lock()
		p.active -= 1
		p.mu.Unlock()
		c = nil
	}
	return c, err
}

func (p *Pool) put(c Conn) error {
	if c.Err() == nil {
		p.mu.Lock()
		if !p.closed {
			p.idle.PushFront(idleConn{t: nowFunc(), c: c})
			if p.idle.Len() > p.MaxIdle {
				c = p.idle.Remove(p.idle.Back()).(idleConn).c
			} else {
				c = nil
			}
		}
		p.mu.Unlock()
	}
	if c != nil {
		p.mu.Lock()
		p.active -= 1
		p.mu.Unlock()
		return c.Close()
	}
	return nil
}

type pooledConnection struct {
	c   Conn
	err error
	p   *Pool
}

func (c *pooledConnection) get() error {
	if c.err == nil && c.c == nil {
		c.c, c.err = c.p.get()
	}
	return c.err
}

func (c *pooledConnection) Close() (err error) {
	if c.c != nil {
		c.c.Do("")
		c.p.put(c.c)
		c.c = nil
		c.err = errPoolClosed
	}
	return err
}

func (c *pooledConnection) Err() error {
	if err := c.get(); err != nil {
		return err
	}
	return c.c.Err()
}

func (c *pooledConnection) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	if err := c.get(); err != nil {
		return nil, err
	}
	return c.c.Do(commandName, args...)
}

func (c *pooledConnection) Send(commandName string, args ...interface{}) error {
	if err := c.get(); err != nil {
		return err
	}
	return c.c.Send(commandName, args...)
}

func (c *pooledConnection) Flush() error {
	if err := c.get(); err != nil {
		return err
	}
	return c.c.Flush()
}

func (c *pooledConnection) Receive() (reply interface{}, err error) {
	if err := c.get(); err != nil {
		return nil, err
	}
	return c.c.Receive()
}
