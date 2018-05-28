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
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"sync"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/trace"

	"github.com/gomodule/redigo/internal/observability"
)

var (
	_ ConnWithTimeout = (*conn)(nil)
)

// conn is the low-level implementation of Conn
type conn struct {
	// Shared
	mu      sync.Mutex
	pending int
	err     error
	conn    net.Conn

	// Read
	readTimeout time.Duration
	br          *bufio.Reader

	// Write
	writeTimeout time.Duration
	bw           *bufio.Writer

	// Scratch space for formatting argument length.
	// '*' or '$', length, "\r\n"
	lenScratch [32]byte

	// Scratch space for formatting integers and floats.
	numScratch [40]byte
}

// DialTimeout acts like Dial but takes timeouts for establishing the
// connection to the server, writing a command and reading a reply.
//
// Deprecated: Use Dial with options instead.
func DialTimeout(network, address string, connectTimeout, readTimeout, writeTimeout time.Duration) (Conn, error) {
	return Dial(network, address,
		DialConnectTimeout(connectTimeout),
		DialReadTimeout(readTimeout),
		DialWriteTimeout(writeTimeout))
}

// DialOption specifies an option for dialing a Redis server.
type DialOption struct {
	f func(*dialOptions)
}

type dialOptions struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
	dialer       *net.Dialer
	dial         func(network, addr string) (net.Conn, error)
	db           int
	password     string
	useTLS       bool
	skipVerify   bool
	tlsConfig    *tls.Config
}

// DialReadTimeout specifies the timeout for reading a single command reply.
func DialReadTimeout(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.readTimeout = d
	}}
}

// DialWriteTimeout specifies the timeout for writing a single command.
func DialWriteTimeout(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.writeTimeout = d
	}}
}

// DialConnectTimeout specifies the timeout for connecting to the Redis server when
// no DialNetDial option is specified.
func DialConnectTimeout(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dialer.Timeout = d
	}}
}

// DialKeepAlive specifies the keep-alive period for TCP connections to the Redis server
// when no DialNetDial option is specified.
// If zero, keep-alives are not enabled. If no DialKeepAlive option is specified then
// the default of 5 minutes is used to ensure that half-closed TCP sessions are detected.
func DialKeepAlive(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dialer.KeepAlive = d
	}}
}

// DialNetDial specifies a custom dial function for creating TCP
// connections, otherwise a net.Dialer customized via the other options is used.
// DialNetDial overrides DialConnectTimeout and DialKeepAlive.
func DialNetDial(dial func(network, addr string) (net.Conn, error)) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dial = dial
	}}
}

// DialDatabase specifies the database to select when dialing a connection.
func DialDatabase(db int) DialOption {
	return DialOption{func(do *dialOptions) {
		do.db = db
	}}
}

// DialPassword specifies the password to use when connecting to
// the Redis server.
func DialPassword(password string) DialOption {
	return DialOption{func(do *dialOptions) {
		do.password = password
	}}
}

// DialTLSConfig specifies the config to use when a TLS connection is dialed.
// Has no effect when not dialing a TLS connection.
func DialTLSConfig(c *tls.Config) DialOption {
	return DialOption{func(do *dialOptions) {
		do.tlsConfig = c
	}}
}

// DialTLSSkipVerify disables server name verification when connecting over
// TLS. Has no effect when not dialing a TLS connection.
func DialTLSSkipVerify(skip bool) DialOption {
	return DialOption{func(do *dialOptions) {
		do.skipVerify = skip
	}}
}

// DialUseTLS specifies whether TLS should be used when connecting to the
// server. This option is ignore by DialURL.
func DialUseTLS(useTLS bool) DialOption {
	return DialOption{func(do *dialOptions) {
		do.useTLS = useTLS
	}}
}

// Dial connects to the Redis server at the given network and
// address using the specified options.
func Dial(network, address string, options ...DialOption) (Conn, error) {
	return DialWithContext(context.Background(), network, address, options...)
}

func DialWithContext(ctx context.Context, network, address string, options ...DialOption) (Conn, error) {
	startTime := time.Now()
	conn, err := doDial(network, address, options...)
	measures := []stats.Measurement{observability.MDials.M(1), observability.MDialLatencySeconds.M(time.Since(startTime).Seconds())}
	if err != nil {
		measures = append(measures, observability.MDialErrors.M(1))
	}
	stats.Record(ctx, measures...)
	return conn, err
}

func doDial(network, address string, options ...DialOption) (Conn, error) {
	do := dialOptions{
		dialer: &net.Dialer{
			KeepAlive: time.Minute * 5,
		},
	}
	for _, option := range options {
		option.f(&do)
	}
	if do.dial == nil {
		do.dial = do.dialer.Dial
	}

	netConn, err := do.dial(network, address)
	if err != nil {
		return nil, err
	}

	if do.useTLS {
		var tlsConfig *tls.Config
		if do.tlsConfig == nil {
			tlsConfig = &tls.Config{InsecureSkipVerify: do.skipVerify}
		} else {
			tlsConfig = cloneTLSConfig(do.tlsConfig)
		}
		if tlsConfig.ServerName == "" {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				netConn.Close()
				return nil, err
			}
			tlsConfig.ServerName = host
		}

		tlsConn := tls.Client(netConn, tlsConfig)
		if err := tlsConn.Handshake(); err != nil {
			netConn.Close()
			return nil, err
		}
		netConn = tlsConn
	}

	c := &conn{
		conn:         netConn,
		bw:           bufio.NewWriter(netConn),
		br:           bufio.NewReader(netConn),
		readTimeout:  do.readTimeout,
		writeTimeout: do.writeTimeout,
	}

	if do.password != "" {
		if _, err := c.Do("AUTH", do.password); err != nil {
			netConn.Close()
			return nil, err
		}
	}

	if do.db != 0 {
		if _, err := c.Do("SELECT", do.db); err != nil {
			netConn.Close()
			return nil, err
		}
	}

	return c, nil
}

var pathDBRegexp = regexp.MustCompile(`/(\d*)\z`)

// DialURL connects to a Redis server at the given URL using the Redis
// URI scheme. URLs should follow the draft IANA specification for the
// scheme (https://www.iana.org/assignments/uri-schemes/prov/redis).
func DialURL(rawurl string, options ...DialOption) (Conn, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "redis" && u.Scheme != "rediss" {
		return nil, fmt.Errorf("invalid redis URL scheme: %s", u.Scheme)
	}

	// As per the IANA draft spec, the host defaults to localhost and
	// the port defaults to 6379.
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		// assume port is missing
		host = u.Host
		port = "6379"
	}
	if host == "" {
		host = "localhost"
	}
	address := net.JoinHostPort(host, port)

	if u.User != nil {
		password, isSet := u.User.Password()
		if isSet {
			options = append(options, DialPassword(password))
		}
	}

	match := pathDBRegexp.FindStringSubmatch(u.Path)
	if len(match) == 2 {
		db := 0
		if len(match[1]) > 0 {
			db, err = strconv.Atoi(match[1])
			if err != nil {
				return nil, fmt.Errorf("invalid database: %s", u.Path[1:])
			}
		}
		if db != 0 {
			options = append(options, DialDatabase(db))
		}
	} else if u.Path != "" {
		return nil, fmt.Errorf("invalid database: %s", u.Path[1:])
	}

	options = append(options, DialUseTLS(u.Scheme == "rediss"))

	return Dial("tcp", address, options...)
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
	c.mu.Lock()
	err := c.err
	if c.err == nil {
		c.err = errors.New("redigo: closed")
		err = c.conn.Close()
	}
	c.mu.Unlock()
	return err
}

func (c *conn) fatal(err error) error {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
		// Close connection to force errors on subsequent calls and to unblock
		// other reader or writer.
		c.conn.Close()
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

func (c *conn) writeLen(prefix byte, n int) error {
	c.lenScratch[len(c.lenScratch)-1] = '\n'
	c.lenScratch[len(c.lenScratch)-2] = '\r'
	i := len(c.lenScratch) - 3
	for {
		c.lenScratch[i] = byte('0' + n%10)
		i -= 1
		n = n / 10
		if n == 0 {
			break
		}
	}
	c.lenScratch[i] = prefix
	_, err := c.bw.Write(c.lenScratch[i:])
	return err
}

func (c *conn) writeString(s string) (int, error) {
	c.writeLen('$', len(s))
	n, _ := c.bw.WriteString(s)
	nr, err := c.bw.WriteString("\r\n")
	n += nr
	return n, err
}

func (c *conn) writeBytes(p []byte) (int, error) {
	c.writeLen('$', len(p))
	c.bw.Write(p)
	return c.bw.WriteString("\r\n")
}

func (c *conn) writeInt64(n int64) (int, error) {
	return c.writeBytes(strconv.AppendInt(c.numScratch[:0], n, 10))
}

func (c *conn) writeFloat64(n float64) (int, error) {
	return c.writeBytes(strconv.AppendFloat(c.numScratch[:0], n, 'g', -1, 64))
}

func (c *conn) writeCommand(ctx context.Context, cmd string, args []interface{}) (int64, error) {
	ctx, span := trace.StartSpan(ctx, "redis.(*Conn).writeCommand")
	defer span.End()

	c.writeLen('*', 1+len(args))
	n := int64(0)
	ns, err := c.writeString(cmd)
	n += int64(ns)
	if err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		span.End()
		return n, err
	}
	for _, arg := range args {
		ni, err := c.writeArg(arg, true)
		if err != nil {
			span.End()
			return n, err
		}
		n += int64(ni)
	}
	span.Annotatef([]trace.Attribute{trace.Int64Attribute("bytes_written", n)}, "Wrote bytes")
	return n, nil
}

func (c *conn) writeArg(arg interface{}, argumentTypeOK bool) (int, error) {
	switch arg := arg.(type) {
	case string:
		return c.writeString(arg)
	case []byte:
		return c.writeBytes(arg)
	case int:
		return c.writeInt64(int64(arg))
	case int64:
		return c.writeInt64(arg)
	case float64:
		return c.writeFloat64(arg)
	case bool:
		if arg {
			return c.writeString("1")
		} else {
			return c.writeString("0")
		}
	case nil:
		return c.writeString("")
	case Argument:
		if argumentTypeOK {
			return c.writeArg(arg.RedisArg(), false)
		}
		// See comment in default clause below.
		var buf bytes.Buffer
		fmt.Fprint(&buf, arg)
		return c.writeBytes(buf.Bytes())
	default:
		// This default clause is intended to handle builtin numeric types.
		// The function should return an error for other types, but this is not
		// done for compatibility with previous versions of the package.
		var buf bytes.Buffer
		fmt.Fprint(&buf, arg)
		return c.writeBytes(buf.Bytes())
	}
}

type protocolError string

func (pe protocolError) Error() string {
	return fmt.Sprintf("redigo: %s (possible server error or unsupported concurrent read by application)", string(pe))
}

func (c *conn) readLine() ([]byte, int, error) {
	p, err := c.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, len(p), protocolError("long response line")
	}
	if err != nil {
		return nil, len(p), err
	}
	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		return nil, len(p), protocolError("bad response line terminator")
	}
	return p[:i], len(p), nil
}

// parseLen parses bulk string and array lengths.
func parseLen(p []byte) (int, error) {
	if len(p) == 0 {
		return -1, protocolError("malformed length")
	}

	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		// handle $-1 and $-1 null replies.
		return -1, nil
	}

	var n int
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, protocolError("illegal bytes in length")
		}
		n += int(b - '0')
	}

	return n, nil
}

// parseInt parses an integer reply.
func parseInt(p []byte) (interface{}, int, error) {
	if len(p) == 0 {
		return 0, 0, protocolError("malformed integer")
	}

	var negate bool
	if p[0] == '-' {
		negate = true
		p = p[1:]
		if len(p) == 0 {
			return 0, 0, protocolError("malformed integer")
		}
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return 0, len(p), protocolError("illegal bytes in length")
		}
		n += int64(b - '0')
	}

	if negate {
		n = -n
	}
	return n, len(p), nil
}

var (
	okReply   string = "OK"
	pongReply string = "PONG"
)

func (c *conn) readReply() (interface{}, int, error) {
	line, n, err := c.readLine()
	if err != nil {
		return nil, n, err
	}
	if len(line) == 0 {
		return nil, n, protocolError("short response line")
	}
	switch line[0] {
	case '+':
		switch {
		case len(line) == 3 && line[1] == 'O' && line[2] == 'K':
			// Avoid allocation for frequent "+OK" response.
			return okReply, n, nil
		case len(line) == 5 && line[1] == 'P' && line[2] == 'O' && line[3] == 'N' && line[4] == 'G':
			// Avoid allocation in PING command benchmarks :)
			return pongReply, n, nil
		default:
			return string(line[1:]), n, nil
		}
	case '-':
		return Error(string(line[1:])), n, nil
	case ':':
		return parseInt(line[1:])
	case '$':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, n, err
		}
		p := make([]byte, n)
		ni, err := io.ReadFull(c.br, p)
		ni += n
		if err != nil {
			return nil, ni, err
		}
		if line, nii, err := c.readLine(); err != nil {
			return nil, ni + nii, err
		} else if len(line) != 0 {
			return nil, ni, protocolError("bad bulk string format")
		}
		return p, ni, nil
	case '*':
		ni, err := parseLen(line[1:])
		if ni < 0 || err != nil {
			return nil, n, err
		}
		r := make([]interface{}, ni)
		var nir int
		for i := range r {
			r[i], nir, err = c.readReply()
			n += nir
			if err != nil {
				return nil, n, err
			}
		}
		return r, n, nil
	}
	return nil, n, protocolError("unexpected response line")
}

func (c *conn) Send(cmd string, args ...interface{}) error {
	return c.SendWithContext(context.Background(), cmd, args...)
}

func (c *conn) SendWithContext(ctx context.Context, cmd string, args ...interface{}) error {
	c.mu.Lock()
	c.pending += 1
	c.mu.Unlock()
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	if _, err := c.writeCommand(ctx, cmd, args); err != nil {
		return c.fatal(err)
	}
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

func (c *conn) Receive() (interface{}, error) {
	return c.ReceiveWithTimeout(c.readTimeout)
}

func (c *conn) ReceiveWithTimeout(timeout time.Duration) (reply interface{}, err error) {
	var deadline time.Time
	if timeout != 0 {
		deadline = time.Now().Add(timeout)
	}
	c.conn.SetReadDeadline(deadline)

	if reply, _, err = c.readReply(); err != nil {
		return nil, c.fatal(err)
	}
	// When using pub/sub, the number of receives can be greater than the
	// number of sends. To enable normal use of the connection after
	// unsubscribing from all channels, we do not decrement pending to a
	// negative value.
	//
	// The pending field is decremented after the reply is read to handle the
	// case where Receive is called before Send.
	c.mu.Lock()
	if c.pending > 0 {
		c.pending -= 1
	}
	c.mu.Unlock()
	if err, ok := reply.(Error); ok {
		return nil, err
	}
	return
}

func (c *conn) Do(cmd string, args ...interface{}) (interface{}, error) {
	return c.DoWithTimeout(c.readTimeout, cmd, args...)
}

func (c *conn) DoWithTimeout(readTimeout time.Duration, cmd string, args ...interface{}) (interface{}, error) {
	return c.do(context.Background(), readTimeout, cmd, args...)
}

func (c *conn) DoWithContext(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	return c.do(ctx, c.readTimeout, cmd, args...)
}

func (c *conn) do(ctx context.Context, readTimeout time.Duration, cmd string, args ...interface{}) (interface{}, error) {
	c.mu.Lock()
	pending := c.pending
	c.pending = 0
	c.mu.Unlock()

	if cmd == "" && pending == 0 {
		return nil, nil
	}

	spanName := cmd
	if spanName == "" {
		spanName = "do"
	}

	ctx, _ = observability.TagKeyValuesIntoContext(ctx, observability.KeyCommandName, spanName)
	ctx, span := trace.StartSpan(ctx, "redis.(*Conn)."+spanName)
	startTime := time.Now()
	defer func() {
		// At the very end we need to record the overall latency
		span.End()
		stats.Record(ctx, observability.MRoundtripLatencySeconds.M(time.Since(startTime).Seconds()))
	}()

	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		span.Annotatef([]trace.Attribute{
			trace.Int64Attribute("timeout_ns", c.writeTimeout.Nanoseconds()),
		}, "Set connection writeTimeout")
	}

	if cmd != "" {
		nw, err := c.writeCommand(ctx, cmd, args)
		stats.Record(ctx, observability.MBytesWritten.M(nw), observability.MWrites.M(1))
		if err != nil {
			stats.Record(ctx, observability.MWriteErrors.M(1))
			span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
			return nil, c.fatal(err)
		}
	}

	if err := c.bw.Flush(); err != nil {
		span.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
		return nil, c.fatal(err)
	}

	var deadline time.Time
	if readTimeout != 0 {
		deadline = time.Now().Add(readTimeout)
		span.Annotatef([]trace.Attribute{
			trace.Int64Attribute("timeout_ns", readTimeout.Nanoseconds()),
		}, "Set connection readTimeout")
	}
	c.conn.SetReadDeadline(deadline)

	var nread int64
	defer func() {
		// At the end record the number of bytes read and increment the number of reads.
		stats.Record(ctx, observability.MBytesRead.M(nread), observability.MReads.M(1))
	}()

	_, readSpan := trace.StartSpan(ctx, "redis.(*Conn).readReplies")
	defer readSpan.End()

	if cmd == "" {
		reply := make([]interface{}, pending)
		for i := range reply {
			r, nir, e := c.readReply()
			nread += int64(nir)
			if e != nil {
				readSpan.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: e.Error()})
				stats.Record(ctx, observability.MReadErrors.M(1))
				return nil, c.fatal(e)
			}
			reply[i] = r
		}
		return reply, nil
	}

	var err error
	var reply interface{}
	for i := 0; i <= pending; i++ {
		var e error
		var nir int
		reply, nir, e = c.readReply()
		nread += int64(nir)
		if e != nil {
			readSpan.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: e.Error()})
			stats.Record(ctx, observability.MReadErrors.M(1))
			return nil, c.fatal(e)
		}
		if e, ok := reply.(Error); ok && err == nil {
			err = e
		}
	}
	if err != nil {
		stats.Record(ctx, observability.MReadErrors.M(1))
		readSpan.SetStatus(trace.Status{Code: int32(trace.StatusCodeInternal), Message: err.Error()})
	} else {
		span.Annotatef([]trace.Attribute{trace.Int64Attribute("bytes_read", nread)}, "Read bytes")
	}
	return reply, err
}
