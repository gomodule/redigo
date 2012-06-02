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
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

// conn is the low-level implementation of Conn
type conn struct {
	rw      bufio.ReadWriter
	conn    net.Conn
	scratch []byte
	pending int
	mu      sync.Mutex
	err     error
}

// Dial connects to the Redis server at the given network and address.
func Dial(network, address string) (Conn, error) {
	netConn, err := net.Dial(network, address)
	if err != nil {
		return nil, errors.New("Could not connect to Redis server: " + err.Error())
	}
	return NewConn(netConn), nil
}

// DialTimeout acts like Dial but takes a timeout. The timeout includes name
// resolution, if required.
func DialTimeout(network, address string, timeout time.Duration) (Conn, error) {
	netConn, err := net.DialTimeout(network, address, timeout)
	if err != nil {
		return nil, errors.New("Could not connect to Redis server: " + err.Error())
	}
	return NewConn(netConn), nil
}

// NewConn returns a new Redigo connection for the given net connection.
func NewConn(netConn net.Conn) Conn {
	return &conn{
		conn: netConn,
		rw: bufio.ReadWriter{
			bufio.NewReader(netConn),
			bufio.NewWriter(netConn),
		},
	}
}

func (c *conn) Close() error {
	err := c.conn.Close()
	if err != nil {
		c.fatal(err)
	} else {
		c.fatal(errors.New("redigo: closed"))
	}
	return err
}

func (c *conn) Err() error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	return err
}

func (c *conn) fatal(err error) error {
	c.mu.Lock()
	if c.err != nil {
		c.err = err
	}
	c.mu.Unlock()
	return err
}

func (c *conn) Do(cmd string, args ...interface{}) (interface{}, error) {
	if err := c.Send(cmd, args...); err != nil {
		return nil, err
	}
	var reply interface{}
	var err = c.err
	for c.pending > 0 && c.err == nil {
		var e error
		reply, e = c.Receive()
		if e != nil && err == nil {
			err = e
		}
	}
	return reply, err
}

func (c *conn) writeN(prefix byte, n int) error {
	c.scratch = append(c.scratch[0:0], prefix)
	c.scratch = strconv.AppendInt(c.scratch, int64(n), 10)
	c.scratch = append(c.scratch, "\r\n"...)
	_, err := c.rw.Write(c.scratch)
	return err
}

func (c *conn) writeString(s string) error {
	if err := c.writeN('$', len(s)); err != nil {
		return err
	}
	if _, err := c.rw.WriteString(s); err != nil {
		return err
	}
	_, err := c.rw.WriteString("\r\n")
	return err
}

func (c *conn) writeBytes(p []byte) error {
	if err := c.writeN('$', len(p)); err != nil {
		return err
	}
	if _, err := c.rw.Write(p); err != nil {
		return err
	}
	_, err := c.rw.WriteString("\r\n")
	return err
}

func (c *conn) readLine() ([]byte, error) {
	p, err := c.rw.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, errors.New("redigo: long response line")
	}
	if err != nil {
		return nil, err
	}
	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		return nil, errors.New("redigo: bad response line terminator")
	}
	return p[:i], nil
}

func (c *conn) parseReply() (interface{}, error) {
	line, err := c.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, errors.New("redigo: short response line")
	}
	switch line[0] {
	case '+':
		return string(line[1:]), nil
	case '-':
		return Error(string(line[1:])), nil
	case ':':
		n, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, err
		}
		return n, nil
	case '$':
		n, err := strconv.Atoi(string(line[1:]))
		if err != nil || n < 0 {
			return nil, err
		}
		p := make([]byte, n)
		_, err = io.ReadFull(c.rw, p)
		if err != nil {
			return nil, err
		}
		line, err := c.readLine()
		if err != nil {
			return nil, err
		}
		if len(line) != 0 {
			return nil, errors.New("redigo: bad bulk format")
		}
		return p, nil
	case '*':
		n, err := strconv.Atoi(string(line[1:]))
		if err != nil || n < 0 {
			return nil, err
		}
		r := make([]interface{}, n)
		for i := range r {
			r[i], err = c.parseReply()
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	}
	return nil, errors.New("redigo: unpexected response line")
}

func (c *conn) Send(cmd string, args ...interface{}) error {
	if err := c.writeN('*', 1+len(args)); err != nil {
		return c.fatal(err)
	}

	if err := c.writeString(cmd); err != nil {
		return c.fatal(err)
	}

	for _, arg := range args {
		var err error
		switch arg := arg.(type) {
		case string:
			err = c.writeString(arg)
		case []byte:
			err = c.writeBytes(arg)
		case nil:
			err = c.writeString("")
		default:
			var buf bytes.Buffer
			fmt.Fprint(&buf, arg)
			err = c.writeBytes(buf.Bytes())
		}
		if err != nil {
			return c.fatal(err)
		}
	}
	c.pending += 1
	return nil
}

func (c *conn) Receive() (interface{}, error) {
	c.pending -= 1
	if err := c.rw.Flush(); err != nil {
		return nil, c.fatal(err)
	}
	v, err := c.parseReply()
	if err == nil {
		if e, ok := v.(Error); ok {
			err = e
		}
	}
	if err != nil {
		return nil, c.fatal(err)
	}
	return v, nil
}
