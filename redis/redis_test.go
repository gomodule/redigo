// Copyright 2017 Gary Burd
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
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

type timeoutTestConn int

func (tc timeoutTestConn) Do(string, ...interface{}) (interface{}, error) {
	return time.Duration(-1), nil
}

func (tc timeoutTestConn) DoWithTimeout(timeout time.Duration, cmd string, args ...interface{}) (interface{}, error) {
	return timeout, nil
}

func (tc timeoutTestConn) Receive() (interface{}, error) {
	return time.Duration(-1), nil
}

func (tc timeoutTestConn) ReceiveWithTimeout(timeout time.Duration) (interface{}, error) {
	return timeout, nil
}

func (tc timeoutTestConn) Send(string, ...interface{}) error { return nil }
func (tc timeoutTestConn) Err() error                        { return nil }
func (tc timeoutTestConn) Close() error                      { return nil }
func (tc timeoutTestConn) Flush() error                      { return nil }

func testTimeout(t *testing.T, c redis.Conn) {
	r, err := c.Do("PING")
	if r != time.Duration(-1) || err != nil {
		t.Errorf("Do() = %v, %v, want %v, %v", r, err, time.Duration(-1), nil)
	}
	r, err = redis.DoWithTimeout(c, time.Minute, "PING")
	if r != time.Minute || err != nil {
		t.Errorf("DoWithTimeout() = %v, %v, want %v, %v", r, err, time.Minute, nil)
	}
	r, err = c.Receive()
	if r != time.Duration(-1) || err != nil {
		t.Errorf("Receive() = %v, %v, want %v, %v", r, err, time.Duration(-1), nil)
	}
	r, err = redis.ReceiveWithTimeout(c, time.Minute)
	if r != time.Minute || err != nil {
		t.Errorf("ReceiveWithTimeout() = %v, %v, want %v, %v", r, err, time.Minute, nil)
	}
}

func TestConnTimeout(t *testing.T) {
	testTimeout(t, timeoutTestConn(0))
}

func TestPoolConnTimeout(t *testing.T) {
	p := &redis.Pool{Dial: func() (redis.Conn, error) { return timeoutTestConn(0), nil }}
	testTimeout(t, p.Get())
}

type contextDeadTestConn int

func (cc contextDeadTestConn) Do(string, ...interface{}) (interface{}, error) {
	return -1, nil
}
func (cc contextDeadTestConn) DoContext(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	return 1, nil
}
func (cc contextDeadTestConn) Receive() (interface{}, error) {
	return -1, nil
}
func (cc contextDeadTestConn) ReceiveContext(ctx context.Context) (interface{}, error) {
	return 1, nil
}
func (cc contextDeadTestConn) Send(string, ...interface{}) error { return nil }
func (cc contextDeadTestConn) Err() error                        { return nil }
func (cc contextDeadTestConn) Close() error                      { return nil }
func (cc contextDeadTestConn) Flush() error                      { return nil }

func testcontext(t *testing.T, c redis.Conn) {
	r, e := c.Do("PING")
	if r != -1 || e != nil {
		t.Errorf("Do() = %v, %v, want %v, %v", r, e, -1, nil)
	}
	ctx, f := context.WithTimeout(context.Background(), time.Minute)
	defer f()
	r, e = redis.DoContext(c, ctx, "PING")
	if r != 1 || e != nil {
		t.Errorf("DoContext() = %v, %v, want %v, %v", r, e, 1, nil)
	}
	r, e = c.Receive()
	if r != -1 || e != nil {
		t.Errorf("Receive() = %v, %v, want %v, %v", r, e, -1, nil)
	}
	r, e = redis.ReceiveContext(c, ctx)
	if r != 1 || e != nil {
		t.Errorf("ReceiveContext() = %v, %v, want %v, %v", r, e, 1, nil)
	}
}

func TestConnContext(t *testing.T) {
	testcontext(t, contextDeadTestConn(0))
}

func TestPoolConnContext(t *testing.T) {
	p := redis.Pool{Dial: func() (redis.Conn, error) { return contextDeadTestConn(0), nil }}
	testcontext(t, p.Get())
}
