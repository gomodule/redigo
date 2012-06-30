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
	conn net.Conn

	// Read
	readTimeout time.Duration
	br          *bufio.Reader
	scratch     []byte

	// Write
	writeTimeout time.Duration
	bw           *bufio.Writer

	// Shared
	mu      sync.Mutex
	pending int
	err     error
}

// Dial connects to the Redis server at the given network and address.
func Dial(network, address string) (Conn, error) {
	c, err := net.Dial(network, address)
	if err != nil {
		return nil, errors.New("Could not connect to Redis server: " + err.Error())
	}
	return NewConn(c, 0, 0), nil
}

// DialTimeout acts like Dial but takes timeouts for establishing the
// connection to the server, write a command and reading a reply.
func DialTimeout(network, address string, connectTimeout, readTimeout, writeTimeout time.Duration) (Conn, error) {
	var c net.Conn
	var err error
	if connectTimeout > 0 {
		c, err = net.DialTimeout(network, address, connectTimeout)
	} else {
		c, err = net.Dial(network, address)
	}
	if err != nil {
		return nil, errors.New("Could not connect to Redis server: " + err.Error())
	}
	return NewConn(c, readTimeout, writeTimeout), nil
}

// NewConn returns a new Redigo connection for the given net connection.
func NewConn(netConn net.Conn, readTimeout, writeTimeout time.Duration) Conn {
	return &conn{
		conn:         netConn,
		bw:           bufio.NewWriter(netConn),
		br:           bufio.NewReader(netConn),
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
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

func (c *conn) fatal(err error) error {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.mu.Unlock()
	return err
}

func (c *conn) Err() error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	return err
}

func (c *conn) writeN(prefix byte, n int) error {
	c.scratch = append(c.scratch[0:0], prefix)
	c.scratch = strconv.AppendInt(c.scratch, int64(n), 10)
	c.scratch = append(c.scratch, "\r\n"...)
	_, err := c.bw.Write(c.scratch)
	return err
}

func (c *conn) writeString(s string) error {
	c.writeN('$', len(s))
	c.bw.WriteString(s)
	_, err := c.bw.WriteString("\r\n")
	return err
}

func (c *conn) writeBytes(p []byte) error {
	c.writeN('$', len(p))
	c.bw.Write(p)
	_, err := c.bw.WriteString("\r\n")
	return err
}

func (c *conn) writeCommand(cmd string, args []interface{}) (err error) {
	c.writeN('*', 1+len(args))
	err = c.writeString(cmd)
	for _, arg := range args {
		if err != nil {
			break
		}
		switch arg := arg.(type) {
		case string:
			err = c.writeString(arg)
		case []byte:
			err = c.writeBytes(arg)
		case bool:
			if arg {
				err = c.writeString("1")
			} else {
				err = c.writeString("0")
			}
		case nil:
			err = c.writeString("")
		default:
			var buf bytes.Buffer
			fmt.Fprint(&buf, arg)
			err = c.writeBytes(buf.Bytes())
		}
	}
	return err
}

func (c *conn) readLine() ([]byte, error) {
	p, err := c.br.ReadSlice('\n')
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

func (c *conn) readReply() (interface{}, error) {
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
		_, err = io.ReadFull(c.br, p)
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
			r[i], err = c.readReply()
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	}
	return nil, errors.New("redigo: unpexected response line")
}

func (c *conn) Send(cmd string, args ...interface{}) error {
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	if err := c.writeCommand(cmd, args); err != nil {
		return c.fatal(err)
	}
	c.mu.Lock()
	c.pending += 1
	c.mu.Unlock()
	return nil
}

func (c *conn) Flush() error {
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	if err := c.bw.Flush(); err != nil {
		return c.fatal(err)
	}
	return nil
}

func (c *conn) Receive() (reply interface{}, err error) {
	c.mu.Lock()
	// There can be more receives than sends when using pub/sub. To allow
	// normal use of the connection after unsubscribe from all channels, do not
	// decrement pending to a negative value.
	if c.pending > 0 {
		c.pending -= 1
	}
	c.mu.Unlock()
	if c.readTimeout != 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}
	if reply, err = c.readReply(); err != nil {
		return nil, c.fatal(err)
	}
	if err, ok := reply.(Error); ok {
		return nil, err
	}
	return
}

func (c *conn) Do(cmd string, args ...interface{}) (reply interface{}, err error) {
	// Send
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	c.writeCommand(cmd, args)
	if err = c.bw.Flush(); err != nil {
		return nil, c.fatal(err)
	}

	c.mu.Lock()
	pending := c.pending
	c.pending = 0
	c.mu.Unlock()

	// Receive
	for ; pending >= 0; pending-- {
		var e error
		if reply, e = c.readReply(); e != nil {
			return nil, c.fatal(e)
		}
		if e, ok := reply.(Error); ok && err == nil {
			err = e
		}
	}
	return
}
