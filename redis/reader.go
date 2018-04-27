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
	"io"
)

// NewSubscriptionReader builds a subscription to the given channels, and
// returns a io.ReadCloser, where each call to the Read function
// pulls an individual messages.
func NewSubscriptionReader(conn Conn, channel ...interface{}) (io.ReadCloser, error) {
	psc := PubSubConn{Conn: conn}
	err := psc.Subscribe(channel...)
	if err != nil {
		return nil, err
	}
	return &subsReader{psc: psc}, nil
}

type subsReader struct {
	psc    PubSubConn
	closed bool
}

// Read waits for a message body to come off the subscription channel(s),
// and returns the content of the message data. If the supplied buffer
// is smaller than what came off the wire, p is populated with as much data
// as possible and an io.ErrShortBuffer is returned.
func (sr *subsReader) Read(p []byte) (n int, err error) {
	for {
		if sr.closed {
			return 0, io.ErrClosedPipe
		}
		switch msg := sr.psc.Receive().(type) {
		case Message:
			n = copy(p, msg.Data)
			if n < len(msg.Data) {
				err = io.ErrShortWrite
			}
			return n, err
		case Subscription:
			if msg.Count == 0 {
				return 0, io.EOF
			}
		case Pong:
			// ignored
		case error:
			return 0, msg
		default:
			return 0, errors.New("redigo: subscription reader: unexpected notification")
		}
	}
}

// WriteTo gets used in an io.Copy if it is available, to avoid a copy
// to an intermediate buffer
func (sr *subsReader) WriteTo(w io.Writer) (n int64, err error) {
	for {
		if sr.closed {
			return 0, io.ErrClosedPipe
		}
		switch msg := sr.psc.Receive().(type) {
		case Message:
			size, err := w.Write(msg.Data)
			return int64(size), err
		case Subscription:
			if msg.Count == 0 {
				return 0, io.EOF
			}
		case Pong:
			// ignored
		case error:
			return 0, msg
		default:
			return 0, errors.New("redigo: subscription reader: unexpected notification")
		}
	}
}

// Close attempts to kill the current subscription, but does NOT close
// the underlying Conn
func (sr *subsReader) Close() error {
	if sr.closed {
		return nil
	}
	sr.closed = true
	return sr.psc.Unsubscribe()
}
