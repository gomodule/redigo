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
	"reflect"
	"testing"
	"time"
)

type poolTestConn struct {
	d   *poolDialer
	err error
	Conn
}

func (c *poolTestConn) Close() error { c.d.open -= 1; return nil }
func (c *poolTestConn) Err() error   { return c.err }

func (c *poolTestConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	if commandName == "ERR" {
		c.err = args[0].(error)
	}
	if commandName != "" {
		c.d.commands = append(c.d.commands, commandName)
	}
	return c.Conn.Do(commandName, args...)
}

func (c *poolTestConn) Send(commandName string, args ...interface{}) error {
	c.d.commands = append(c.d.commands, commandName)
	return c.Conn.Send(commandName, args...)
}

type poolDialer struct {
	t            *testing.T
	dialed, open int
	commands     []string
}

func (d *poolDialer) dial() (Conn, error) {
	d.open += 1
	d.dialed += 1
	c, err := DialTestDB()
	if err != nil {
		return nil, err
	}
	return &poolTestConn{d: d, Conn: c}, nil
}

func (d *poolDialer) check(message string, p *Pool, dialed, open int) {
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
	d := poolDialer{t: t}
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
	d := poolDialer{t: t}
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
	d := poolDialer{t: t}
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
	d := poolDialer{t: t}
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
	c2.Close()

	p.Close()

	d.check("after pool close", p, 3, 1)

	if _, err := c1.Do("PING"); err == nil {
		t.Errorf("expected error after connection and pool closed")
	}

	c3.Close()

	d.check("after conn close", p, 3, 0)

	c1 = p.Get()
	if _, err := c1.Do("PING"); err == nil {
		t.Errorf("expected error after pool closed")
	}
}

func TestPoolTimeout(t *testing.T) {
	d := poolDialer{t: t}
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

	p.Close()
}

func TestPoolConcurrenSendReceive(t *testing.T) {
	p := &Pool{
		Dial: DialTestDB,
	}
	c := p.Get()
	done := make(chan error, 1)
	go func() {
		_, err := c.Receive()
		done <- err
	}()
	c.Send("PING")
	c.Flush()
	err := <-done
	if err != nil {
		t.Fatalf("Receive() returned error %v", err)
	}
	_, err = c.Do("")
	if err != nil {
		t.Fatalf("Do() returned error %v", err)
	}
	c.Close()
	p.Close()
}

func TestPoolBorrowCheck(t *testing.T) {
	d := poolDialer{t: t}
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
	p.Close()
}

func TestPoolMaxActive(t *testing.T) {
	d := poolDialer{t: t}
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
	d.check("3", p, 2, 2)

	c3 = p.Get()
	if _, err := c3.Do("PING"); err != nil {
		t.Errorf("expected good channel, err=%v", err)
	}
	c3.Close()

	d.check("4", p, 2, 2)
	p.Close()
}

func TestPoolMonitorCleanup(t *testing.T) {
	d := poolDialer{t: t}
	p := &Pool{
		MaxIdle:   2,
		MaxActive: 2,
		Dial:      d.dial,
	}
	c := p.Get()
	c.Send("MONITOR")
	c.Close()

	d.check("", p, 1, 0)
	p.Close()
}

func TestPoolPubSubCleanup(t *testing.T) {
	d := poolDialer{t: t}
	p := &Pool{
		MaxIdle:   2,
		MaxActive: 2,
		Dial:      d.dial,
	}

	c := p.Get()
	c.Send("SUBSCRIBE", "x")
	c.Close()

	want := []string{"SUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE", "ECHO"}
	if !reflect.DeepEqual(d.commands, want) {
		t.Errorf("got commands %v, want %v", d.commands, want)
	}
	d.commands = nil

	c = p.Get()
	c.Send("PSUBSCRIBE", "x*")
	c.Close()

	want = []string{"PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE", "ECHO"}
	if !reflect.DeepEqual(d.commands, want) {
		t.Errorf("got commands %v, want %v", d.commands, want)
	}
	d.commands = nil

	p.Close()
}

func TestPoolTransactionCleanup(t *testing.T) {
	d := poolDialer{t: t}
	p := &Pool{
		MaxIdle:   2,
		MaxActive: 2,
		Dial:      d.dial,
	}

	c := p.Get()
	c.Do("WATCH", "key")
	c.Do("PING")
	c.Close()

	want := []string{"WATCH", "PING", "UNWATCH"}
	if !reflect.DeepEqual(d.commands, want) {
		t.Errorf("got commands %v, want %v", d.commands, want)
	}
	d.commands = nil

	c = p.Get()
	c.Do("WATCH", "key")
	c.Do("UNWATCH")
	c.Do("PING")
	c.Close()

	want = []string{"WATCH", "UNWATCH", "PING"}
	if !reflect.DeepEqual(d.commands, want) {
		t.Errorf("got commands %v, want %v", d.commands, want)
	}
	d.commands = nil

	c = p.Get()
	c.Do("WATCH", "key")
	c.Do("MULTI")
	c.Do("PING")
	c.Close()

	want = []string{"WATCH", "MULTI", "PING", "DISCARD"}
	if !reflect.DeepEqual(d.commands, want) {
		t.Errorf("got commands %v, want %v", d.commands, want)
	}
	d.commands = nil

	c = p.Get()
	c.Do("WATCH", "key")
	c.Do("MULTI")
	c.Do("DISCARD")
	c.Do("PING")
	c.Close()

	want = []string{"WATCH", "MULTI", "DISCARD", "PING"}
	if !reflect.DeepEqual(d.commands, want) {
		t.Errorf("got commands %v, want %v", d.commands, want)
	}
	d.commands = nil

	c = p.Get()
	c.Do("WATCH", "key")
	c.Do("MULTI")
	c.Do("EXEC")
	c.Do("PING")
	c.Close()

	want = []string{"WATCH", "MULTI", "EXEC", "PING"}
	if !reflect.DeepEqual(d.commands, want) {
		t.Errorf("got commands %v, want %v", d.commands, want)
	}
	d.commands = nil

	p.Close()
}

func BenchmarkPoolGet(b *testing.B) {
	b.StopTimer()
	p := Pool{Dial: DialTestDB, MaxIdle: 2}
	c := p.Get()
	if err := c.Err(); err != nil {
		b.Fatal(err)
	}
	c.Close()
	defer p.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c = p.Get()
		c.Close()
	}
}

func BenchmarkPoolGetErr(b *testing.B) {
	b.StopTimer()
	p := Pool{Dial: DialTestDB, MaxIdle: 2}
	c := p.Get()
	if err := c.Err(); err != nil {
		b.Fatal(err)
	}
	c.Close()
	defer p.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c = p.Get()
		if err := c.Err(); err != nil {
			b.Fatal(err)
		}
		c.Close()
	}
}

func BenchmarkPoolGetPing(b *testing.B) {
	b.StopTimer()
	p := Pool{Dial: DialTestDB, MaxIdle: 2}
	c := p.Get()
	if err := c.Err(); err != nil {
		b.Fatal(err)
	}
	c.Close()
	defer p.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c = p.Get()
		if _, err := c.Do("PING"); err != nil {
			b.Fatal(err)
		}
		c.Close()
	}
}
