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
//
// Connections
//
// The Conn interface is the primary interface for working with Redis.
// Applications create connections by calling the Dial, DialWithTimeout or
// NewConn functions. In the future, functions will be added for creating
// pooled connections and sharded connections.
//
// The application must call the connection Close method when the application
// is done with the connection.
//
// Executing Commands
//
// The Conn interface has a generic method for executing Redis commands:
//
//  Do(commandName string, args ...interface{}) (reply interface{}, err error)
//
// Arguments of type string and []byte are sent to the server as is. All other
// types are formatted using the fmt.Fprint function. Command replies are
// represented using the following Go types:
//
//  Redis type          Go type
//  error               redis.Error
//  integer             int64
//  status              string
//  bulk                []byte or nil if value not present.
//  multi-bulk          []interface{} or nil if value not present.
// 
// Applications can use type assertions or type switches to determine the type
// of a reply.
//
// Pipelining
//
// Connections support pipelining using the Send and Receive methods. 
//
//  Send(commandName string, args ...interface{}) error
//  Receive() (reply interface{}, err error)
//
// Send writes the command to the connection's output buffer. Receive flushes
// the output buffer to the server and reads a single reply. The following
// example shows a simple pipeline:
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
// The Do method is implemented with the Send and Receive methods. The method
// starts by sending the command. Next, the method receives all unconsumed
// replies including the reply for the command just sent by Do. If any of the
// received replies is an error, then Do returns the error. If there are no
// errors, then Do returns the last reply.
//
// The Send and Do methods can be used together to implement pipelined
// transactions:
//
//  c.Send("MULTI")
//  c.Send("INCR", "foo")
//  c.Send("INCR", "bar")
//  r, err := c.Do("EXEC")
//  fmt.Println(r) // prints [1, 1]
//
// Publish and Subscribe
//  
// The connection Receive method is used to implement blocking subscribers: 
//
//  c.Send("SUBSCRIBE", "foo")
//  for {
//      reply, err := c.Receive()
//      if err != nil {
//          return err
//      }
//      // consume message
//  }
//
// Thread Safety
//
// The Send method cannot be called concurrently with other calls to Send. The
// Receive method cannot be called concurrently  with other calls to Receive.
// Because the Do method invokes Send and Receive, the Do method cannot be
// called concurrently  with Send, Receive or Do. All other concurrent access is
// allowed.
package redis

import ()

// Error represents an error returned in a command reply.
type Error string

func (err Error) Error() string { return string(err) }

// Conn represents a connection to a Redis server.
type Conn interface {
	// Close closes the connection.
	Close() error

	// Err returns the permanent error for this connection.
	Err() error

	// Do sends a command to the server and returns the received reply.
	Do(commandName string, args ...interface{}) (reply interface{}, err error)

	// Send sends a command for the server without waiting for a reply.
	Send(commandName string, args ...interface{}) error

	// Receive receives a single reply from the server
	Receive() (reply interface{}, err error)
}
