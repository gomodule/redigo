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
)

// conn is the low-level implementation of Conn
type Conn struct {
	// Handlers
	close     func() error
	doContext func(ctx context.Context, commandName string, args ...interface{}) (interface{}, error)
	error     func() error
	send      func(commandName string, args ...interface{}) error

	// Shared fields.
	conn net.Conn

	// Mutex protected fields.
	mu      sync.Mutex
	err     error
	pending int
	state   connectionState

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

// DialOption specifies an option for dialing a Redis server.
type DialOption func(*dialOptions)

type dialOptions struct {
	readTimeout         time.Duration
	writeTimeout        time.Duration
	tlsHandshakeTimeout time.Duration
	dialer              *net.Dialer
	dialContext         func(ctx context.Context, network, addr string) (net.Conn, error)
	connOptions         []ConnOption
	db                  int
	username            string
	password            string
	clientName          string
	useTLS              bool
	skipVerify          bool
	tlsConfig           *tls.Config
}

// DialTLSHandshakeTimeout specifies the maximum amount of time waiting to
// wait for a TLS handshake. Zero means no timeout.
// If no DialTLSHandshakeTimeout option is specified then the default is 30 seconds.
func DialTLSHandshakeTimeout(d time.Duration) DialOption {
	return func(do *dialOptions) {
		do.tlsHandshakeTimeout = d
	}
}

// DialReadTimeout specifies the timeout for reading a single command reply.
func DialReadTimeout(d time.Duration) DialOption {
	return func(do *dialOptions) {
		do.readTimeout = d
	}
}

// DialWriteTimeout specifies the timeout for writing a single command.
func DialWriteTimeout(d time.Duration) DialOption {
	return func(do *dialOptions) {
		do.writeTimeout = d
	}
}

// DialConnectTimeout specifies the timeout for connecting to the Redis server when
// no DialNetDial option is specified.
// If no DialConnectTimeout option is specified then the default is 30 seconds.
func DialConnectTimeout(d time.Duration) DialOption {
	return func(do *dialOptions) {
		do.dialer.Timeout = d
	}
}

// DialKeepAlive specifies the keep-alive period for TCP connections to the Redis server
// when no DialNetDial option is specified.
// If zero, keep-alives are not enabled. If no DialKeepAlive option is specified then
// the default of 5 minutes is used to ensure that half-closed TCP sessions are detected.
func DialKeepAlive(d time.Duration) DialOption {
	return func(do *dialOptions) {
		do.dialer.KeepAlive = d
	}
}

// DialNetDial specifies a custom dial function for creating TCP
// connections, otherwise a net.Dialer customized via the other options is used.
// DialNetDial overrides DialConnectTimeout and DialKeepAlive.
func DialNetDial(dial func(network, addr string) (net.Conn, error)) DialOption {
	return func(do *dialOptions) {
		do.dialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dial(network, addr)
		}
	}
}

// DialContextFunc specifies a custom dial function with context for creating TCP
// connections, otherwise a net.Dialer customized via the other options is used.
// DialContextFunc overrides DialConnectTimeout and DialKeepAlive.
func DialContextFunc(f func(ctx context.Context, network, addr string) (net.Conn, error)) DialOption {
	return func(do *dialOptions) {
		do.dialContext = f
	}
}

// DialDatabase specifies the database to select when dialing a connection.
func DialDatabase(db int) DialOption {
	return func(do *dialOptions) {
		do.db = db
	}
}

// DialPassword specifies the password to use when connecting to
// the Redis server.
func DialPassword(password string) DialOption {
	return func(do *dialOptions) {
		do.password = password
	}
}

// DialUsername specifies the username to use when connecting to
// the Redis server when Redis ACLs are used.
// A DialPassword must also be passed otherwise this option will have no effect.
func DialUsername(username string) DialOption {
	return func(do *dialOptions) {
		do.username = username
	}
}

// DialClientName specifies a client name to be used
// by the Redis server connection.
func DialClientName(name string) DialOption {
	return func(do *dialOptions) {
		do.clientName = name
	}
}

// DialTLSConfig specifies the config to use when a TLS connection is dialed.
// Has no effect when not dialing a TLS connection.
func DialTLSConfig(c *tls.Config) DialOption {
	return func(do *dialOptions) {
		do.tlsConfig = c
	}
}

// DialTLSSkipVerify disables server name verification when connecting over
// TLS. Has no effect when not dialing a TLS connection.
func DialTLSSkipVerify(skip bool) DialOption {
	return func(do *dialOptions) {
		do.skipVerify = skip
	}
}

// DialUseTLS specifies whether TLS should be used when connecting to the
// server. This option is ignore by DialURL.
func DialUseTLS(useTLS bool) DialOption {
	return func(do *dialOptions) {
		do.useTLS = useTLS
	}
}

func DialConnOptions(options ...ConnOption) DialOption {
	return func(do *dialOptions) {
		do.connOptions = options
	}
}

// Dial connects to the Redis server at the given network and
// address using the specified options.
func Dial(network, address string, options ...DialOption) (*Conn, error) {
	return DialContext(context.Background(), network, address, options...)
}

type tlsHandshakeTimeoutError struct{}

func (tlsHandshakeTimeoutError) Timeout() bool   { return true }
func (tlsHandshakeTimeoutError) Temporary() bool { return true }
func (tlsHandshakeTimeoutError) Error() string   { return "TLS handshake timeout" }

// DialContext connects to the Redis server at the given network and
// address using the specified options and context.
func DialContext(ctx context.Context, network, address string, options ...DialOption) (*Conn, error) {
	do := dialOptions{
		dialer: &net.Dialer{
			Timeout:   time.Second * 30,
			KeepAlive: time.Minute * 5,
		},
		tlsHandshakeTimeout: time.Second * 10,
	}
	for _, option := range options {
		option(&do)
	}
	if do.dialContext == nil {
		do.dialContext = do.dialer.DialContext
	}

	netConn, err := do.dialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}

	if do.useTLS {
		var tlsConfig *tls.Config
		if do.tlsConfig == nil {
			tlsConfig = &tls.Config{InsecureSkipVerify: do.skipVerify}
		} else {
			tlsConfig = do.tlsConfig.Clone()
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
		errc := make(chan error, 2) // buffered so we don't block timeout or Handshake
		if d := do.tlsHandshakeTimeout; d != 0 {
			timer := time.AfterFunc(d, func() {
				errc <- tlsHandshakeTimeoutError{}
			})
			defer timer.Stop()
		}
		go func() {
			errc <- tlsConn.Handshake()
		}()
		if err := <-errc; err != nil {
			// Timeout or Handshake error.
			netConn.Close() // nolint: errcheck
			return nil, err
		}

		netConn = tlsConn
	}

	c := NewConn(netConn, do.readTimeout, do.writeTimeout, do.connOptions...)

	if do.password != "" {
		authArgs := make([]interface{}, 0, 2)
		if do.username != "" {
			authArgs = append(authArgs, do.username)
		}
		authArgs = append(authArgs, do.password)
		if _, err := c.DoContext(ctx, "AUTH", authArgs...); err != nil {
			netConn.Close()
			return nil, err
		}
	}

	if do.clientName != "" {
		if _, err := c.DoContext(ctx, "CLIENT", "SETNAME", do.clientName); err != nil {
			netConn.Close()
			return nil, err
		}
	}

	if do.db != 0 {
		if _, err := c.DoContext(ctx, "SELECT", do.db); err != nil {
			netConn.Close()
			return nil, err
		}
	}

	return c, nil
}

var pathDBRegexp = regexp.MustCompile(`/(\d*)\z`)

// DialURL wraps DialURLContext using context.Background.
func DialURL(rawurl string, options ...DialOption) (*Conn, error) {
	ctx := context.Background()

	return DialURLContext(ctx, rawurl, options...)
}

// DialURLContext connects to a Redis server at the given URL using the Redis
// URI scheme. URLs should follow the draft IANA specification for the
// scheme (https://www.iana.org/assignments/uri-schemes/prov/redis).
func DialURLContext(ctx context.Context, rawurl string, options ...DialOption) (*Conn, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "redis" && u.Scheme != "rediss" {
		return nil, fmt.Errorf("invalid redis URL scheme: %s", u.Scheme)
	}

	if u.Opaque != "" {
		return nil, fmt.Errorf("invalid redis URL, url is opaque: %s", rawurl)
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
		username := u.User.Username()
		if isSet {
			if username != "" {
				// ACL
				options = append(options, DialUsername(username), DialPassword(password))
			} else {
				// requirepass - user-info username:password with blank username
				options = append(options, DialPassword(password))
			}
		} else if username != "" {
			// requirepass - redis-cli compatibility which treats as single arg in user-info as a password
			options = append(options, DialPassword(username))
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

	return DialContext(ctx, "tcp", address, options...)
}

// NewConn returns a new Redigo connection for the given net connection.
func NewConn(netConn net.Conn, readTimeout, writeTimeout time.Duration, options ...ConnOption) *Conn {
	c := &Conn{
		conn:         netConn,
		bw:           bufio.NewWriter(netConn),
		br:           bufio.NewReader(netConn),
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}

	c.close = c.baseClose
	c.doContext = c.baseDoContext
	c.error = c.baseErr
	c.send = c.baseSend

	for _, option := range options {
		option(c)
	}

	return c
}

func (c *Conn) Close() error {
	return c.close()
}

func (c *Conn) baseClose() error {
	c.mu.Lock()
	err := c.err
	if c.err == nil {
		c.err = errors.New("redigo: closed")
		err = c.conn.Close()
	}
	c.mu.Unlock()
	return err
}

func (c *Conn) fatal(err error) error {
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

func (c *Conn) Err() error {
	return c.error()
}

func (c *Conn) baseErr() error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	return err
}

func (c *Conn) writeLen(prefix byte, n int) error {
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

func (c *Conn) writeString(s string) error {
	if err := c.writeLen('$', len(s)); err != nil {
		return err
	}
	if _, err := c.bw.WriteString(s); err != nil {
		return err
	}
	_, err := c.bw.WriteString("\r\n")
	return err
}

func (c *Conn) writeBytes(p []byte) error {
	if err := c.writeLen('$', len(p)); err != nil {
		return err
	}
	if _, err := c.bw.Write(p); err != nil {
		return err
	}
	_, err := c.bw.WriteString("\r\n")
	return err
}

func (c *Conn) writeInt64(n int64) error {
	return c.writeBytes(strconv.AppendInt(c.numScratch[:0], n, 10))
}

func (c *Conn) writeFloat64(n float64) error {
	return c.writeBytes(strconv.AppendFloat(c.numScratch[:0], n, 'g', -1, 64))
}

func (c *Conn) writeCommand(cmd string, args []interface{}) error {
	if err := c.writeLen('*', 1+len(args)); err != nil {
		return err
	}
	if err := c.writeString(cmd); err != nil {
		return err
	}
	for _, arg := range args {
		if err := c.writeArg(arg, true); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) writeArg(arg interface{}, argumentTypeOK bool) (err error) {
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

// readLine reads a line of input from the RESP stream.
func (c *Conn) readLine() ([]byte, error) {
	// To avoid allocations, attempt to read the line using ReadSlice. This
	// call typically succeeds. The known case where the call fails is when
	// reading the output from the MONITOR command.
	p, err := c.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		// The line does not fit in the bufio.Reader's buffer. Fall back to
		// allocating a buffer for the line.
		buf := append([]byte{}, p...)
		for err == bufio.ErrBufferFull {
			p, err = c.br.ReadSlice('\n')
			buf = append(buf, p...)
		}
		p = buf
	}
	if err != nil {
		return nil, err
	}
	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		return nil, protocolError("bad response line terminator")
	}
	return p[:i], nil
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
func parseInt(p []byte) (interface{}, error) {
	if len(p) == 0 {
		return 0, protocolError("malformed integer")
	}

	var negate bool
	if p[0] == '-' {
		negate = true
		p = p[1:]
		if len(p) == 0 {
			return 0, protocolError("malformed integer")
		}
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return 0, protocolError("illegal bytes in length")
		}
		n += int64(b - '0')
	}

	if negate {
		n = -n
	}
	return n, nil
}

var (
	okReply   interface{} = "OK"
	pongReply interface{} = "PONG"
)

func (c *Conn) readReply() (interface{}, error) {
	line, err := c.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, protocolError("short response line")
	}
	switch line[0] {
	case '+':
		switch string(line[1:]) {
		case "OK":
			// Avoid allocation for frequent "+OK" response.
			return okReply, nil
		case "PONG":
			// Avoid allocation in PING command benchmarks :)
			return pongReply, nil
		default:
			return string(line[1:]), nil
		}
	case '-':
		return Error(line[1:]), nil
	case ':':
		return parseInt(line[1:])
	case '$':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		p := make([]byte, n)
		_, err = io.ReadFull(c.br, p)
		if err != nil {
			return nil, err
		}
		if line, err := c.readLine(); err != nil {
			return nil, err
		} else if len(line) != 0 {
			return nil, protocolError("bad bulk string format")
		}
		return p, nil
	case '*':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
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
	return nil, protocolError("unexpected response line")
}

func (c *Conn) stateUpdate(cmd string, args ...interface{}) {
	c.state.update(connActions, true, cmd, args...)
}

func (c *Conn) Send(cmd string, args ...interface{}) error {
	return c.send(cmd, args...)
}

func (c *Conn) baseSend(cmd string, args ...interface{}) error {
	c.mu.Lock()
	c.stateUpdate(cmd, args...)
	if c.state&(stateClientReplyOff|stateClientReplySkipNext|stateClientReplySkip) == 0 {
		c.pending += 1
	}
	c.mu.Unlock()

	if c.writeTimeout != 0 {
		if err := c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
			return c.fatal(err)
		}
	}
	if err := c.writeCommand(cmd, args); err != nil {
		return c.fatal(err)
	}
	return nil
}

func (c *Conn) Flush() error {
	if c.writeTimeout != 0 {
		if err := c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
			return c.fatal(err)
		}
	}
	if err := c.bw.Flush(); err != nil {
		return c.fatal(err)
	}
	return nil
}

func (c *Conn) Receive() (interface{}, error) {
	return c.ReceiveWithTimeout(c.readTimeout)
}

func (c *Conn) ReceiveContext(ctx context.Context) (interface{}, error) {
	var realTimeout time.Duration
	if dl, ok := ctx.Deadline(); ok {
		timeout := time.Until(dl)
		if timeout >= c.readTimeout && c.readTimeout != 0 {
			realTimeout = c.readTimeout
		} else if timeout <= 0 {
			return nil, c.fatal(context.DeadlineExceeded)
		} else {
			realTimeout = timeout
		}
	} else {
		realTimeout = c.readTimeout
	}
	endch := make(chan struct{})
	var r interface{}
	var e error
	go func() {
		defer close(endch)

		r, e = c.ReceiveWithTimeout(realTimeout)
	}()
	select {
	case <-ctx.Done():
		return nil, c.fatal(ctx.Err())
	case <-endch:
		return r, e
	}
}

func (c *Conn) ReceiveWithTimeout(timeout time.Duration) (reply interface{}, err error) {
	var deadline time.Time
	if timeout != 0 {
		deadline = time.Now().Add(timeout)
	}
	if err = c.conn.SetReadDeadline(deadline); err != nil {
		return nil, c.fatal(err)
	}

	if reply, err = c.readReply(); err != nil {
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

	return reply, nil
}

func (c *Conn) Do(cmd string, args ...interface{}) (interface{}, error) {
	return c.DoWithTimeout(c.readTimeout, cmd, args...)
}

func (c *Conn) DoContext(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	return c.doContext(ctx, cmd, args...)
}

func (c *Conn) baseDoContext(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	var realTimeout time.Duration
	if dl, ok := ctx.Deadline(); ok {
		timeout := time.Until(dl)
		if timeout >= c.readTimeout && c.readTimeout != 0 {
			realTimeout = c.readTimeout
		} else if timeout <= 0 {
			return nil, c.fatal(context.DeadlineExceeded)
		} else {
			realTimeout = timeout
		}
	} else {
		realTimeout = c.readTimeout
	}
	endch := make(chan struct{})
	var r interface{}
	var e error
	go func() {
		defer close(endch)

		r, e = c.DoWithTimeout(realTimeout, cmd, args...)
	}()
	select {
	case <-ctx.Done():
		return nil, c.fatal(ctx.Err())
	case <-endch:
		return r, e
	}
}

func (c *Conn) DoWithTimeout(readTimeout time.Duration, cmd string, args ...interface{}) (interface{}, error) {
	var doPending int
	c.mu.Lock()
	c.stateUpdate(cmd, args...)
	pending := c.pending
	c.pending = 0
	if cmd != "" && c.state&(stateClientReplyOff|stateClientReplySkipNext|stateClientReplySkip) == 0 {
		// Do is expecting a reply.
		doPending = 1
	}
	c.mu.Unlock()

	if cmd == "" && pending == 0 {
		return nil, nil
	}

	if c.writeTimeout != 0 {
		if err := c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
			return nil, c.fatal(err)
		}
	}

	if cmd != "" {
		if err := c.writeCommand(cmd, args); err != nil {
			return nil, c.fatal(err)
		}
	}

	if err := c.bw.Flush(); err != nil {
		return nil, c.fatal(err)
	}

	var deadline time.Time
	if readTimeout != 0 {
		deadline = time.Now().Add(readTimeout)
	}
	if err := c.conn.SetReadDeadline(deadline); err != nil {
		return nil, c.fatal(err)
	}

	if cmd == "" {
		// Empty command indicates we want to ensure unread replies from Send are read.
		reply := make([]interface{}, pending)
		for i := range reply {
			r, e := c.readReply()
			if e != nil {
				return nil, c.fatal(e)
			}
			reply[i] = r
		}
		return reply, nil
	}

	// Read all pending replies, from previous Send and the current Do.
	var err error
	var reply interface{}
	pending += doPending
	for i := 0; i < pending; i++ {
		var e error
		if reply, e = c.readReply(); e != nil {
			return nil, c.fatal(e)
		}
		if e, ok := reply.(Error); ok && err == nil {
			// First non-nil error is the error we will return.
			err = e
		}
	}

	return reply, err
}

// reset resets the connection to a clean state.
func (c *Conn) reset() error {
	if c.state&stateMonitor != 0 {
		// RESET is the only way to clear MONITOR and that resets
		// all state including authentication and DB.
		// Since we can't detect if AUTH or SELECT commands were
		// issued during dial, we have to force close the connection.
		return errMonitorEnabled
	}

	// DISCARD first to ensure subsequent commands are processed.
	if c.state&stateMulti != 0 {
		if err := c.Send("DISCARD"); err != nil {
			return fmt.Errorf("discard: %w", err)
		}
	}

	if c.state&stateClientNoEvict != 0 {
		if err := c.Send("CLIENT", "NO-EVICT", "OFF"); err != nil {
			return fmt.Errorf("client no-evict off: %w", err)
		}
	}

	if c.state&stateClientNoTouch != 0 {
		if err := c.Send("CLIENT", "NO-TOUCH", "OFF"); err != nil {
			return fmt.Errorf("client no-touch off: %w", err)
		}
	}

	if c.state&(stateClientReplyOff|stateClientReplySkipNext|stateClientReplySkip) != 0 {
		if err := c.Send("CLIENT", "REPLY", "ON"); err != nil {
			return fmt.Errorf("client reply on: %w", err)
		}
	}

	if c.state&stateClientTracking != 0 {
		if err := c.Send("CLIENT", "TRACKING", "OFF"); err != nil {
			return fmt.Errorf("client tracking off: %w", err)
		}
	}

	if c.state&statePsubscribe != 0 {
		if err := c.Send("PUNSUBSCRIBE"); err != nil {
			return fmt.Errorf("punsubscribe: %w", err)
		}
	}

	if c.state&stateReadOnly != 0 {
		if err := c.Send("READWRITE"); err != nil {
			return fmt.Errorf("readwrite: %w", err)
		}
	}

	if c.state&stateSsubscribe != 0 {
		if err := c.Send("SUNSUBSCRIBE"); err != nil {
			return fmt.Errorf("sunsubscribe: %w", err)
		}
	}

	if c.state&stateSubscribe != 0 {
		if err := c.Send("UNSUBSCRIBE"); err != nil {
			return fmt.Errorf("unsubscribe: %w", err)
		}
	}

	if c.state&stateWatch != 0 {
		if err := c.Send("UNWATCH"); err != nil {
			return fmt.Errorf("unwatch: %w", err)
		}
	}

	if c.state&(stateSubscribe|statePsubscribe|stateSsubscribe) != 0 {
		// Drain subscribed messages.
		// To detect the end of the message stream, ask the server to echo
		// a sentinel value and read until we see that value.
		sentinelOnce.Do(initSentinel)
		if err := c.Send("ECHO", sentinel); err != nil {
			return fmt.Errorf("echo sentinel: %w", err)
		}

		if err := c.Flush(); err != nil {
			return fmt.Errorf("flush: %w", err)
		}

		for {
			p, err := c.Receive()
			if err != nil {
				return fmt.Errorf("receive: %w", err)
			}

			if p, ok := p.([]byte); ok && bytes.Equal(p, sentinel) {
				c.state &^= stateSubscribe | statePsubscribe | stateSsubscribe
				return nil // End of message stream.
			}
		}
	}

	// Ensure any pending reads have completed.
	_, err := c.Do("")
	return err
}
