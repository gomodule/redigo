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
	"errors"
	"strconv"
)

var (
	errUnexpectedResultType = errors.New("redigo: unexpected result type")
)

// Int is a helper that wraps a call to the Conn Do and Receive methods and
// returns the result as an integer. If the result is an integer, then Int
// returns the integer. If the result is a bulk response, then Int parses the
// result as a signed decimal value.  Otherwise, Int returns an error.
func Int(v interface{}, err error) (int, error) {
	if err != nil {
		return 0, err
	}
	switch v := v.(type) {
	case int64:
		return int(v), nil
	case []byte:
		n, err := strconv.ParseInt(string(v), 10, 0)
		return int(n), err
	case Error:
		return 0, v
	}
	return 0, errUnexpectedResultType
}

// String is a helper that wraps a call to the Conn Do and Receive methods and
// returns the result as a string. If the result is a bulk response, then
// String returns the bytes converted to a string. If the result is an integer,
// then String formats the result as a decimal string. Otherwise, String returns
// an error.
func String(v interface{}, err error) (string, error) {
	if err != nil {
		return "", err
	}
	switch v := v.(type) {
	case int64:
		return strconv.FormatInt(v, 10), nil
	case []byte:
		return string(v), nil
	case Error:
		return "", v
	}
	return "", errUnexpectedResultType
}

// Bytes is a helper that wraps a call to the Conn Do or Receive methods and
// returns the result as a []byte. If the result is a bulk response, then Bytes
// returns the result as is.  If the result is an integer, then Bytes formats
// the result as a decimal string. Otherwise, Bytes returns an error.
func Bytes(v interface{}, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	switch v := v.(type) {
	case int64:
		return strconv.AppendInt(nil, v, 10), nil
	case []byte:
		return v, nil
	case Error:
		return nil, v
	}
	return nil, errUnexpectedResultType
}

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

// Notification returns the result from the Conn Receive method as a
// Subscription or a Message.
func Notification(v interface{}, err error) (interface{}, error) {
	if err != nil {
		return nil, err
	}
	err = errUnexpectedResultType
	s, ok := v.([]interface{})
	if !ok || len(s) != 3 {
		return nil, errUnexpectedResultType
	}
	b, ok := s[0].([]byte)
	if !ok {
		return nil, errUnexpectedResultType
	}
	kind := string(b)

	b, ok = s[1].([]byte)
	if !ok {
		return nil, errUnexpectedResultType
	}
	channel := string(b)

	if kind == "message" {
		data, ok := s[2].([]byte)
		if !ok {
			return nil, errUnexpectedResultType
		}
		return Message{channel, data}, nil
	}

	count, ok := s[2].(int64)
	if !ok {
		return nil, errUnexpectedResultType
	}

	return Subscription{kind, channel, int(count)}, nil
}
