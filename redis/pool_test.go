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
	"log"
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

func TestPoolReuse(t *testing.T) {
	var open, dialed int
	p := &Pool{
		MaxIdle: 2,
		Dial:    func() (Conn, error) { open += 1; dialed += 1; return &fakeConn{open: &open}, nil },
		Test: func(c Conn) error {
			log.Printf("testing connection %v", c)
			if _, err := c.Do("PING"); err != nil {
				log.Printf("Redis Test function caught error %v", err)
				return err
			}
			return nil
		},
	}

	for i := 0; i < 10; i++ {
		c1 := p.Get()
		c1.Do("PING")
		c2 := p.Get()
		c2.Do("PING")
		c1.Close()
		c2.Close()
	}
	if open != 2 || dialed != 2 {
		t.Errorf("want open=2, got %d; want dialed=2, got %d", open, dialed)
	}
}

func TestPoolMaxIdle(t *testing.T) {
	var open, dialed int
	p := &Pool{
		MaxIdle: 2,
		Dial:    func() (Conn, error) { open += 1; dialed += 1; return &fakeConn{open: &open}, nil },
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
	if open != 2 || dialed != 12 {
		t.Errorf("want open=2, got %d; want dialed=12, got %d", open, dialed)
	}
}

func TestPoolError(t *testing.T) {
	var open, dialed int
	p := &Pool{
		MaxIdle: 2,
		Dial:    func() (Conn, error) { open += 1; dialed += 1; return &fakeConn{open: &open}, nil },
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

	if open != 0 || dialed != 2 {
		t.Errorf("want open=0, got %d; want dialed=2, got %d", open, dialed)
	}
}

func TestPoolClose(t *testing.T) {
	var open, dialed int
	p := &Pool{
		MaxIdle: 2,
		Dial:    func() (Conn, error) { open += 1; dialed += 1; return &fakeConn{open: &open}, nil },
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

	if open != 1 {
		t.Errorf("want open=1, got %d", open)
	}

	if _, err := c1.Do("PING"); err == nil {
		t.Errorf("expected error after connection and pool closed")
	}

	c3.Close()
	if open != 0 {
		t.Errorf("want open=0, got %d", open)
	}

	c1 = p.Get()
	if _, err := c1.Do("PING"); err == nil {
		t.Errorf("expected error after pool closed")
	}
}

func TestPoolTimeout(t *testing.T) {
	var open, dialed int
	p := &Pool{
		MaxIdle:     2,
		IdleTimeout: 300 * time.Second,
		Dial:        func() (Conn, error) { open += 1; dialed += 1; return &fakeConn{open: &open}, nil },
	}

	now := time.Now()
	nowFunc = func() time.Time { return now }
	defer func() { nowFunc = time.Now }()

	c := p.Get()
	c.Do("PING")
	c.Close()

	if open != 1 || dialed != 1 {
		t.Errorf("want open=1, got %d; want dialed=1, got %d", open, dialed)
	}

	now = now.Add(p.IdleTimeout)

	c = p.Get()
	c.Do("PING")
	c.Close()

	if open != 1 || dialed != 2 {
		t.Errorf("want open=1, got %d; want dialed=2, got %d", open, dialed)
	}
}
