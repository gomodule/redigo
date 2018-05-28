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

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

func TestStub(t *testing.T) {
	alt := fmt.Sprintf("alternate message %d", time.Now().UnixNano())
	testCases := []struct {
		name string
		conn redis.Conn
		xErr string
	}{
		{
			"undefined funcs",
			&StubConn{},
			MsgNotImplemented,
		},
		{
			"provided funcs",
			&StubConn{
				OnClose: func() error {
					return fmt.Errorf(alt)
				},
				OnErr: func() error {
					return fmt.Errorf(alt)
				},
				OnDo: func(_ string, _ ...interface{}) (interface{}, error) {
					return nil, fmt.Errorf(alt)
				},
				OnSend: func(_ string, _ ...interface{}) error {
					return fmt.Errorf(alt)
				},
				OnFlush: func() error {
					return fmt.Errorf(alt)
				},
				OnReceive: func() (interface{}, error) {
					return nil, fmt.Errorf(alt)
				},
			},
			alt,
		},
	}

	for _, tc := range testCases {
		// Test Close()
		err := tc.conn.Close()
		if err == nil {
			t.Errorf("Close(): expected error\n")
		}
		if !strings.Contains(err.Error(), tc.xErr) {
			t.Errorf("Close(): expected %q; got %q\n", tc.xErr, err.Error())
		}

		// Test Err()
		err = tc.conn.Err()
		if err == nil {
			t.Errorf("Err(): expected error\n")
		}
		if !strings.Contains(err.Error(), tc.xErr) {
			t.Errorf("Err(): expected %q; got %q\n", tc.xErr, err.Error())
		}

		// Test Do(...)
		_, err = tc.conn.Do("SET", "key", "value")
		if err == nil {
			t.Errorf("Do(...): expected error\n")
		}
		if !strings.Contains(err.Error(), tc.xErr) {
			t.Errorf("Do(...): expected %q; got %q\n", tc.xErr, err.Error())
		}

		// Test Send(...)
		err = tc.conn.Send("SET", "key", "value")
		if err == nil {
			t.Errorf("Send(...): expected error\n")
		}
		if !strings.Contains(err.Error(), tc.xErr) {
			t.Errorf("Send(...): expected %q; got %q\n", tc.xErr, err.Error())
		}

		// Test Flush()
		err = tc.conn.Flush()
		if err == nil {
			t.Errorf("Flush(): expected error\n")
		}
		if !strings.Contains(err.Error(), tc.xErr) {
			t.Errorf("Flush(): expected %q; got %q\n", tc.xErr, err.Error())
		}

		// Test Receive()
		_, err = tc.conn.Receive()
		if err == nil {
			t.Errorf("Receive(): expected error\n")
		}
		if !strings.Contains(err.Error(), tc.xErr) {
			t.Errorf("Receive(): expected %q; got %q\n", tc.xErr, err.Error())
		}
	}
}
