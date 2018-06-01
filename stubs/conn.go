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

package stubs

import "errors"

// ErrNotImplemented is the default error returned when a
// method is invoked without a stub func defined.
var ErrNotImplemented = errors.New("stub: not implemented")

// StubConn is a redis.Conn that helps consumers of redigo with testing
type Conn struct {
	OnClose   func() error
	OnErr     func() error
	OnDo      func(commandName string, args ...interface{}) (reply interface{}, err error)
	OnSend    func(commandName string, args ...interface{}) error
	OnFlush   func() error
	OnReceive func() (reply interface{}, err error)
}

// Close conforms to the redis.Conn interface.
// "Close closes the connection."
func (s *Conn) Close() error {
	if s.OnClose == nil {
		return ErrNotImplemented
	}
	return s.OnClose()
}

// Err conforms to the redis.Conn interface.
// "Err returns a non-nil value when the connection is not usable."
func (s *Conn) Err() error {
	if s.OnErr == nil {
		return ErrNotImplemented
	}
	return s.OnErr()
}

// Do conforms to the redis.Conn interface.
// "Do sends a command to the server and returns the received reply."
func (s *Conn) Do(cmd string, args ...interface{}) (reply interface{}, err error) {
	if s.OnDo == nil {
		return nil, ErrNotImplemented
	}
	return s.OnDo(cmd, args...)
}

// Send conforms to the redis.Conn interface.
// "Send writes the command to the client's output buffer."
func (s *Conn) Send(cmd string, args ...interface{}) error {
	if s.OnSend == nil {
		return ErrNotImplemented
	}
	return s.OnSend(cmd, args...)
}

// Flush conforms to the redis.Conn interface.
// "Flush flushes the output buffer to the Redis server."
func (s *Conn) Flush() error {
	if s.OnFlush == nil {
		return ErrNotImplemented
	}
	return s.OnFlush()
}

// Receive conforms to the redis.Conn interface.
// "Receive receives a single reply from the Redis server"
func (s *Conn) Receive() (reply interface{}, err error) {
	if s.OnReceive == nil {
		return nil, ErrNotImplemented
	}
	return s.OnReceive()
}
