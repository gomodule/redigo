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
// shareded and other types of connections.
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
// Arguments of type string and []byte are sent to the server as is. The value
// false is converted to "0" and the value true is converted to "1". The value
// nil is converted to "". All other values are converted to a string using the
// fmt.Fprint function. Command replies are represented using the following Go
// types:
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
// Connections support pipelining using the Send, Flush and Receive methods. 
//
//  Send(commandName string, args ...interface{}) error
//  Flush() error
//  Receive() (reply interface{}, err error)
//
// Send writes the command to the connection's output buffer. Flush flushes the
// connection's output buffer to the server. Receive reads a single reply from
// the server. The following example shows a simple pipeline.
//
//  c.Send("SET", "foo", "bar")
//  c.Send("GET", "foo")
//  c.Flush()
//  c.Receive() // reply from SET
//  v, err = c.Receive() // reply from GET
//
// The Do method combines the functionality of the Send, Flush and Receive
// methods. The Do method starts by writing the command and flushing the output
// buffer. Next, the Do method receives all pending replies including the reply
// for the command just sent by Do. If any of the received replies is an error,
// then Do returns the error. If there are no errors, then Do returns the last
// reply.
//
// Use the Send and Do methods to implement pipelined transactions.
//
//  c.Send("MULTI")
//  c.Send("INCR", "foo")
//  c.Send("INCR", "bar")
//  r, err := c.Do("EXEC")
//  fmt.Println(r) // prints [1, 1]
//
// Thread Safety
//
// The connection Send and Flush methods cannot be called concurrently with
// other calls to these methods. The connection Receive method cannot be called
// concurrently  with other calls to Receive. Because the connection Do method
// uses Send, Flush and Receive, the Do method cannot be called concurrently
// with Send, Flush, Receive or Do. Unless stated otherwise, all other
// concurrent access is allowed.
//
// Publish and Subscribe
//  
// Use the Send, Flush and Receive methods to implement Pub/Sub subscribers.
//
//  c.Send("SUBSCRIBE", "example")
//  c.Flush()
//  for {
//      reply, err := c.Receive()
//      if err != nil {
//          return err
//      }
//      // process pushed message
//  }
// 
// The PubSubConn type wraps a Conn with convenience methods for implementing
// subscribers. The Subscribe, PSubscribe, Unsubscribe and PUnsubscribe methods
// send and flush a subscription management command. The receive method
// converts a pushed message to convenient types for use in a type switch.
//
//  psc := PubSubConn{c}
//  psc.Subscribe("example")
//  for {
//      switch v := psc.Receive().(type) {
//      case redis.Message:
//          fmt.Printf("%s: message: %s\n", v.Channel, v.Data)
//      case redis.Subscription:
//          fmt.Printf("%s: %s %d\n", v.Channel, v.Kind, v.Count)
//      case error:
//          return v
//  }
//
// Reply Helpers
//
// The Bool, Int, Bytes, String and MultiBulk functions convert a reply to a
// value of a specific type. To allow convenient wrapping of calls to the
// connection Do and Receive methods, the functions take a second argument of
// type error. If the error is non-nil, then the helper function returns the
// error. If the error is nil, the function converts the reply to the specified
// type:
//
//  exists, err := redis.Bool(c.Do("EXISTS", "foo"))
//  if err != nil {
//      // handle error return from c.Do or type conversion error.
//  }
package redis
