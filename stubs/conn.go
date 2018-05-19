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

import "fmt"

// MsgNotImplemented is the default error body returned when a
// method is invokes without a stub func defined.
const MsgNotImplemented = "not implemented"

// StubConn is a redis.Conn that helps consumers of redigo with testing
type StubConn struct {
	OnClose   func() error
	OnErr     func() error
	OnDo      func(commandName string, args ...interface{}) (reply interface{}, err error)
	OnSend    func(commandName string, args ...interface{}) error
	OnFlush   func() error
	OnReceive func() (reply interface{}, err error)
}

// Close conforms to the redis.Conn interface.
// "Close closes the connection."
func (s *StubConn) Close() error {
	if s.OnClose == nil {
		return fmt.Errorf(MsgNotImplemented)
	}
	return s.OnClose()
}

// Err conforms to the redis.Conn interface.
// "Err returns a non-nil value when the connection is not usable."
func (s *StubConn) Err() error {
	if s.OnErr == nil {
		return fmt.Errorf(MsgNotImplemented)
	}
	return s.OnErr()
}

// Do conforms to the redis.Conn interface.
// "Do sends a command to the server and returns the received reply."
func (s *StubConn) Do(cmd string, args ...interface{}) (reply interface{}, err error) {
	if s.OnDo == nil {
		return nil, fmt.Errorf(MsgNotImplemented)
	}
	return s.OnDo(cmd, args...)
}

// Send conforms to the redis.Conn interface.
// "Send writes the command to the client's output buffer."
func (s *StubConn) Send(cmd string, args ...interface{}) error {
	if s.OnSend == nil {
		return fmt.Errorf(MsgNotImplemented)
	}
	return s.OnSend(cmd, args...)
}

// Flush conforms to the redis.Conn interface.
// "Flush flushes the output buffer to the Redis server."
func (s *StubConn) Flush() error {
	if s.OnFlush == nil {
		return fmt.Errorf(MsgNotImplemented)
	}
	return s.OnFlush()
}

// Receive conforms to the redis.Conn interface.
// "Receive receives a single reply from the Redis server"
func (s *StubConn) Receive() (reply interface{}, err error) {
	if s.OnReceive == nil {
		return nil, fmt.Errorf(MsgNotImplemented)
	}
	return s.OnReceive()
}
