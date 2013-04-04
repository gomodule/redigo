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

package redis

import (
	"io"
	"testing"
	"time"
)

type fakeConn struct {
	open *int
	err  error
}

func (c *fakeConn) Close() error { *c.open -= 1; return nil }
func (c *fakeConn) Err() error   { return c.err }

func (c *fakeConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	if commandName == "ERR" {
		c.err = args[0].(error)
	}
	return nil, nil
}

func (c *fakeConn) Send(commandName string, args ...interface{}) error {
	return nil
}

func (c *fakeConn) Flush() error {
	return nil
}

func (c *fakeConn) Receive() (reply interface{}, err error) {
	return nil, nil
}

type dialer struct {
	t            *testing.T
	dialed, open int
}

func (d *dialer) dial() (Conn, error) {
	d.open += 1
	d.dialed += 1
	return &fakeConn{open: &d.open}, nil
}

func (d *dialer) check(message string, p *Pool, dialed, open int) {
	if d.dialed != dialed {
		d.t.Errorf("%s: dialed=%d, want %d", message, d.dialed, dialed)
	}
	if d.open != open {
		d.t.Errorf("%s: open=%d, want %d", message, d.open, open)
	}
	if active := p.ActiveCount(); active != open {
		d.t.Errorf("%s: active=%d, want %d", message, active, open)
	}
}

func TestPoolReuse(t *testing.T) {
	d := dialer{t: t}
	p := &Pool{
		MaxIdle: 2,
		Dial:    d.dial,
	}

	for i := 0; i < 10; i++ {
		c1 := p.Get()
		c1.Do("PING")
		c2 := p.Get()
		c2.Do("PING")
		c1.Close()
		c2.Close()
	}

	d.check("before close", p, 2, 2)
	p.Close()
	d.check("after close", p, 2, 0)
}

func TestPoolMaxIdle(t *testing.T) {
	d := dialer{t: t}
	p := &Pool{
		MaxIdle: 2,
		Dial:    d.dial,
	}
	for i := 0; i < 10; i++ {
		c1 := p.Get()
		c1.Do("PING")
		c2 := p.Get()
		c2.Do("PING")
		c3 := p.Get()
		c3.Do("PING")
		c1.Close()
		c2.Close()
		c3.Close()
	}
	d.check("before close", p, 12, 2)
	p.Close()
	d.check("after close", p, 12, 0)
}

func TestPoolError(t *testing.T) {
	d := dialer{t: t}
	p := &Pool{
		MaxIdle: 2,
		Dial:    d.dial,
	}

	c := p.Get()
	c.Do("ERR", io.EOF)
	if c.Err() == nil {
		t.Errorf("expected c.Err() != nil")
	}
	c.Close()

	c = p.Get()
	c.Do("ERR", io.EOF)
	c.Close()

	d.check(".", p, 2, 0)
}

func TestPoolClose(t *testing.T) {
	d := dialer{t: t}
	p := &Pool{
		MaxIdle: 2,
		Dial:    d.dial,
	}

	c1 := p.Get()
	c1.Do("PING")
	c2 := p.Get()
	c2.Do("PING")
	c3 := p.Get()
	c3.Do("PING")

	c1.Close()
	if _, err := c1.Do("PING"); err == nil {
		t.Errorf("expected error after connection closed")
	}

	c2.Close()

	p.Close()

	d.check("after pool close", p, 3, 1)

	if _, err := c1.Do("PING"); err == nil {
		t.Errorf("expected error after connection and pool closed")
	}

	c3.Close()

	d.check("after channel close", p, 3, 0)

	c1 = p.Get()
	if _, err := c1.Do("PING"); err == nil {
		t.Errorf("expected error after pool closed")
	}
}

func TestPoolTimeout(t *testing.T) {
	d := dialer{t: t}
	p := &Pool{
		MaxIdle:     2,
		IdleTimeout: 300 * time.Second,
		Dial:        d.dial,
	}

	now := time.Now()
	nowFunc = func() time.Time { return now }
	defer func() { nowFunc = time.Now }()

	c := p.Get()
	c.Do("PING")
	c.Close()

	d.check("1", p, 1, 1)

	now = now.Add(p.IdleTimeout)

	c = p.Get()
	c.Do("PING")
	c.Close()

	d.check("2", p, 2, 1)
}

func TestBorrowCheck(t *testing.T) {
	d := dialer{t: t}
	p := &Pool{
		MaxIdle:      2,
		Dial:         d.dial,
		TestOnBorrow: func(Conn, time.Time) error { return Error("BLAH") },
	}

	for i := 0; i < 10; i++ {
		c := p.Get()
		c.Do("PING")
		c.Close()
	}
	d.check("1", p, 10, 1)
}

func TestMaxActive(t *testing.T) {
	d := dialer{t: t}
	p := &Pool{
		MaxIdle:   2,
		MaxActive: 2,
		Dial:      d.dial,
	}
	c1 := p.Get()
	c1.Do("PING")
	c2 := p.Get()
	c2.Do("PING")

	d.check("1", p, 2, 2)

	c3 := p.Get()
	if _, err := c3.Do("PING"); err != ErrPoolExhausted {
		t.Errorf("expected pool exhausted")
	}

	c3.Close()
	d.check("2", p, 2, 2)
	c2.Close()
	d.check("2", p, 2, 2)

	c3 = p.Get()
	if _, err := c3.Do("PING"); err != nil {
		t.Errorf("expected good channel, err=%v", err)
	}
	c3.Close()

	d.check("2", p, 2, 2)
}
