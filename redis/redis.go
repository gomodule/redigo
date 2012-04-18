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

// Error represets an error returned in a command reply.
type Error string

func (err Error) Error() string { return string(err) }

// Conn represents a connection to a Redis server.
//
// The Do method executes a Redis command. Command arguments of type string and
// []byte are sent to the server as is. All other argument types are formatted
// using the fmt.Fprint() function. Command replies are represented as Go types
// as follows:
// 
//  Redis type          Go type
//  error               redis.Error
//  integer             int64
//  status              string
//  bulk                []byte or nil if value not present.
//  multi-bulk          []interface{} or nil if value not present.
//
// Connections support pipelining using the Send and Receive methods. Send
// formats and buffers outgoing commands. Receive flushes the outgoing command
// buffer and reads an incoming reply. The following example shows a simple
// pipeline:
//
//  c.Send("SET", "foo", "bar")
//  c.Send("GET", "foo")
//  // reply from SET
//  if _, err := c.Receive(); err != nil {
//      return err
//  }
//  // reply from GET
//  v, err := c.Receive()
//  if err != nil {
//      return err
//  }
//  
// This API can be used to implement a blocking subscriber:
//
//  c.Do("SUBSCRIBE", "foo")
//  for {
//      reply, err := c.Receive()
//      if err != nil {
//          // handle error
//      }
//      // consume message
//  }
type Conn interface {
	// Close closes the connection.
	Close() error

	// Err returns the permanent error for this connection.
	Err() error

	// Do sends a command to the server and returns the received reply.
	Do(cmd string, args ...interface{}) (reply interface{}, err error)

	// Send sends a command for the server without waiting for a reply.
	Send(cmd string, args ...interface{}) error

	// Receive receives a single reply from the server
	Receive() (reply interface{}, err error)
}
