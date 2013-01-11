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
	"log"
	"sync"
	"time"
)

var nowFunc = time.Now // for testing

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
//                  if err := c.Do("AUTH", password); err != nil {
//                      c.Close()
//                      return nil, err
//                  }
//                  return c, err
//              },
//          }
//
// This pool has a maximum of three connections to the server specified by the
// variable "server". Each connection is authenticated using a password.
//
// A request handler gets a connection from the pool and closes the connection
// when the handler is done:
//
//  conn, err := pool.Get()
//  defer conn.Close()
//  // do something with the connection
type Pool struct {

	// Dial is an application supplied function for creating new connections.
	Dial func() (Conn, error)

	// Test is an application supplied function for testing new connections as they are requested
	Test func(Conn) error

	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	// mu protects fields defined below.
	mu     sync.Mutex
	closed bool

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

// Close releases the resources used by the pool.
func (p *Pool) Close() error {
	p.mu.Lock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(idleConn).c.Close()
	}
	return nil
}

func (p *Pool) get() (c Conn, err error) {
	p.mu.Lock()
	if p.IdleTimeout > 0 {
		for {
			e := p.idle.Back()
			if e == nil || nowFunc().Before(e.Value.(idleConn).t) {
				break
			}
			cStale := e.Value.(idleConn).c
			p.idle.Remove(e)

			// Release the pool lock during the potentially long call to the
			// connection's Close method.
			p.mu.Unlock()
			cStale.Close()
			p.mu.Lock()
		}
	}
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("redigo: get on closed pool")
	}
	if e := p.idle.Front(); e != nil {
		c = e.Value.(idleConn).c
		p.idle.Remove(e)
	}
	p.mu.Unlock()
	if c == nil {
		c, err = p.Dial()
	}
	return c, err
}

func (p *Pool) put(c Conn) error {
	p.mu.Lock()
	if !p.closed {
		p.idle.PushFront(idleConn{t: nowFunc().Add(p.IdleTimeout), c: c})
		if p.idle.Len() > p.MaxIdle {
			c = p.idle.Remove(p.idle.Back()).(idleConn).c
		} else {
			c = nil
		}
	}
	p.mu.Unlock()
	if c != nil {
		return c.Close()
	}
	return nil
}

type pooledConnection struct {
	c   Conn
	err error
	p   *Pool
}

func (c *pooledConnection) remove(c1 Conn) {
	var toRemove *list.Element
	for e := c.p.idle.Front(); e != nil; e = e.Next() {
		log.Printf("enumerating")
		c2 := e.Value.(idleConn).c
		if c1 == c2 {
			toRemove = e
			break
		}
	}
	if toRemove != nil {
		//						c.p.mu.Lock()
		//				c.p.mu.Unlock()
		log.Printf("removing %v", toRemove)
		c.p.idle.Remove(toRemove)
	}
}

func (c *pooledConnection) get() error {
	if c.err == nil && c.c == nil {
		c.c, c.err = c.p.get()
		if c.err == nil {
			log.Printf("c.err == nil")
			if c.p.Test != nil {
				log.Printf("c.p.Test != nil")
				if c.p.Test(c.c) != nil {
					log.Printf("c.p.Test(c.c) != nil")
					// connection acquisition test function error'd
					// kill this connection and get a different one
					c.remove(c.c)
					log.Printf("removed connection, acquiring new one")
					err := c.get()
					log.Printf("get returned [%v]", err)
					return err
				}
			}
		}
	}
	return c.err
}

func (c *pooledConnection) Close() (err error) {
	if c.c != nil {
		c.c.Do("")
		if c.c.Err() != nil {
			err = c.c.Close()
		} else {
			err = c.p.put(c.c)
		}
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
