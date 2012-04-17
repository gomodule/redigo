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

// Package redis is a client for the Redis database.
//
// Package redis only supports the binary-safe Redis protocol, so you can use
// it with any Redis version >= 1.2.0.
package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// Conn represents a connection to a Redis server.
//
// Conn objects are not thread-safe.
//
// The Conn Do and Receive methods return a Redis reply. The following table
// shows how Redis reply types map to Go language types.
// 
//  Redis type          Go type
//  Integer             int64
//  Status              string
//  Bulk                []byte or nil if value not present
//  Multi-Bulk          []interface{} or nil if value not present
//  Error               redis.Error
type Conn struct {
	rw      bufio.ReadWriter
	conn    net.Conn
	err     error
	scratch []byte
}

// Dial connects to the Redis server at the given network and address.
func Dial(network, address string) (*Conn, error) {
	netConn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return newConn(netConn), nil
}

// DialTimeout acts like Dial but takes a timeout. The timeout includes name
// resolution, if required.
func DialTimeout(network, address string, timeout time.Duration) (*Conn, error) {
	netConn, err := net.DialTimeout(network, address, timeout)
	if err != nil {
		return nil, err
	}
	return newConn(netConn), nil
}

func newConn(netConn net.Conn) *Conn {
	return &Conn{
		conn: netConn,
		rw: bufio.ReadWriter{
			bufio.NewReader(netConn),
			bufio.NewWriter(netConn),
		},
	}
}

// Close closes the connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// Err returns the permanent error for this connection.
func (c *Conn) Err() error {
	return c.err
}

// Do sends a command to the server and returns the received reply.
func (c *Conn) Do(cmd string, args ...interface{}) (interface{}, error) {
	if err := c.Send(cmd, args...); err != nil {
		return nil, err
	}
	return c.Receive()
}

func (c *Conn) writeN(prefix byte, n int) error {
	c.scratch = append(c.scratch[0:0], prefix)
	c.scratch = strconv.AppendInt(c.scratch, int64(n), 10)
	c.scratch = append(c.scratch, "\r\n"...)
	_, err := c.rw.Write(c.scratch)
	return err
}

func (c *Conn) writeString(s string) error {
	if err := c.writeN('$', len(s)); err != nil {
		return err
	}
	if _, err := c.rw.WriteString(s); err != nil {
		return err
	}
	_, err := c.rw.WriteString("\r\n")
	return err
}

func (c *Conn) writeBytes(p []byte) error {
	if err := c.writeN('$', len(p)); err != nil {
		return err
	}
	if _, err := c.rw.Write(p); err != nil {
		return err
	}
	_, err := c.rw.WriteString("\r\n")
	return err
}

func (c *Conn) readLine() ([]byte, error) {
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

// Error represets an error returned in a command reply.
type Error string

func (err Error) Error() string { return string(err) }

func (c *Conn) parseReply() (interface{}, error) {
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

// Send sends a command for the server without waiting for a reply.
func (c *Conn) Send(cmd string, args ...interface{}) error {
	if c.err != nil {
		return c.err
	}

	c.err = c.writeN('*', 1+len(args))
	if c.err != nil {
		return c.err
	}

	c.err = c.writeString(cmd)
	if c.err != nil {
		return c.err
	}

	for _, arg := range args {
		switch arg := arg.(type) {
		case string:
			c.err = c.writeString(arg)
		case []byte:
			c.err = c.writeBytes(arg)
		case nil:
			c.err = c.writeString("")
		default:
			var buf bytes.Buffer
			fmt.Fprint(&buf, arg)
			c.err = c.writeBytes(buf.Bytes())
		}
		if c.err != nil {
			return c.err
		}
	}
	return nil
}

// Receive receives a single reply from the server
func (c *Conn) Receive() (interface{}, error) {
	c.err = c.rw.Flush()
	if c.err != nil {
		return nil, c.err
	}
	v, err := c.parseReply()
	if err != nil {
		c.err = err
	} else if e, ok := v.(Error); ok {
		err = e
		v = nil
	}
	return v, err
}
