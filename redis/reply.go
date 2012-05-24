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
	errUnexpectedReplyType = errors.New("redigo: unexpected reply type")
)

// Int is a helper that converts a Redis reply to an int. Integer replies are
// returned directly. Bulk replies are interpreted as signed decimal strings.
// If err is not equal to nil or the reply is not an integer or bulk value,
// then Int returns an error.
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
	return 0, errUnexpectedReplyType
}

// String is a helper that converts a Redis reply to a string. Bulk replies are
// returned as a string. Integer replies are formatted as as a signed decimal
// string. If err is not equal to nil or the reply is not an integer or bulk
// value, then Int returns an error.
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
	return "", errUnexpectedReplyType
}

// Bytes is a helper that converts a Redis reply to slice of bytes.  Bulk
// replies are returned as is. Integer replies are formatted as as a signed
// decimal string. If err is not equal to nil or the reply is not an integer
// or bulk value, then Int returns an error.
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
	return nil, errUnexpectedReplyType
}

// Bool is a helper that converts a Redis reply eo a bool. Bool returns true if
// the reply is the integer 1 and false if the reply is the integer 0.  If err
// is not equal to nil or the reply is not the integer 0 or 1, then Bool
// returns an error.
func Bool(v interface{}, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	switch v := v.(type) {
	case int64:
		switch v {
		case 0:
			return false, nil
		case 1:
			return true, nil
		}
	case Error:
		return false, v
	}
	return false, errUnexpectedReplyType
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

// Notification is a helper that returns a pub/sub notification as a
// Subscription or a Message.
func Notification(v interface{}, err error) (interface{}, error) {
	if err != nil {
		return nil, err
	}
	err = errUnexpectedReplyType
	s, ok := v.([]interface{})
	if !ok || len(s) != 3 {
		return nil, errUnexpectedReplyType
	}
	b, ok := s[0].([]byte)
	if !ok {
		return nil, errUnexpectedReplyType
	}
	kind := string(b)

	b, ok = s[1].([]byte)
	if !ok {
		return nil, errUnexpectedReplyType
	}
	channel := string(b)

	if kind == "message" {
		data, ok := s[2].([]byte)
		if !ok {
			return nil, errUnexpectedReplyType
		}
		return Message{channel, data}, nil
	}

	count, ok := s[2].(int64)
	if !ok {
		return nil, errUnexpectedReplyType
	}

	return Subscription{kind, channel, int(count)}, nil
}
