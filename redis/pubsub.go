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
	"bytes"
)

// Subscribe represents a subscribe or unsubscribe notification.
type Subscription struct {

	// Kind is "subscribe", "unsubscribe", "psubscribe" or "punsubscribe"
	Kind string

	// The channel that was changed.
	Channel string

	// The current number of subscriptions for connection.
	Count int
}

// Message represents a message notification.
type Message struct {

	// The originating channel.
	Channel string

	// The message data.
	Data []byte
}

// PubSubConn wraps a Conn with convenience methods for subscribers.
type PubSubConn struct {
	Conn Conn
}

// Close closes the connection.
func (c PubSubConn) Close() error {
	return c.Conn.Close()
}

// Subscribe subscribes the connection to the specified channels.
func (c PubSubConn) Subscribe(channel ...interface{}) error {
	c.Conn.Send("SUBSCRIBE", channel...)
	return c.Conn.Flush()
}

// PSubscribe subscribes the connection to the given patterns.
func (c PubSubConn) PSubscribe(channel ...interface{}) error {
	c.Conn.Send("PSUBSCRIBE", channel...)
	return c.Conn.Flush()
}

// Unsubscribe unsubscribes the connection from the given channels, or from all
// of them if none is given.
func (c PubSubConn) Unsubscribe(channel ...interface{}) error {
	c.Conn.Send("UNSUBSCRIBE", channel...)
	return c.Conn.Flush()
}

// PUnsubscribe unsubscribes the connection from the given patterns, or from all
// of them if none is given.
func (c PubSubConn) PUnsubscribe(channel ...interface{}) error {
	c.Conn.Send("PUNSUBSCRIBE", channel...)
	return c.Conn.Flush()
}

var messageBytes = []byte("message")

// Receive returns a pushed message as a Subscription, Message or error. The
// return value is intended to be used directly in a type switch as illustrated
// in the PubSubConn example.
func (c PubSubConn) Receive() interface{} {
	multiBulk, err := MultiBulk(c.Conn.Receive())
	if err != nil {
		return err
	}

	var kind []byte
	var channel string
	multiBulk, err = Values(multiBulk, &kind, &channel)
	if err != nil {
		return err
	}

	if bytes.Equal(kind, messageBytes) {
		var data []byte
		if _, err := Values(multiBulk, &data); err != nil {
			return err
		}
		return Message{channel, data}
	}

	var count int
	if _, err := Values(multiBulk, &count); err != nil {
		return err
	}
	return Subscription{string(kind), channel, count}
}
