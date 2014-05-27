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
	"bufio"
	"errors"
	"net"
	"time"
)

type testConn struct {
	Conn
}

func (t testConn) Close() error {
	_, err := t.Conn.Do("SELECT", "9")
	if err != nil {
		return nil
	}
	_, err = t.Conn.Do("FLUSHDB")
	if err != nil {
		return err
	}
	return t.Conn.Close()
}

// DialTestDB dials the local Redis server and selects database 9. To prevent
// stomping on real data, DialTestDB fails if database 9 contains data. The
// returned connection flushes database 9 on close.
func DialTestDB() (Conn, error) {
	c, err := DialTimeout("tcp", ":6379", 0, 1*time.Second, 1*time.Second)
	if err != nil {
		return nil, err
	}

	_, err = c.Do("SELECT", "9")
	if err != nil {
		return nil, err
	}

	n, err := Int(c.Do("DBSIZE"))
	if err != nil {
		return nil, err
	}

	if n != 0 {
		return nil, errors.New("database #9 is not empty, test can not continue")
	}

	return testConn{c}, nil
}

type dummyClose struct{ net.Conn }

func (dummyClose) Close() error { return nil }

// NewConnBufio is a hook for tests.
func NewConnBufio(rw bufio.ReadWriter) Conn {
	return &conn{br: rw.Reader, bw: rw.Writer, conn: dummyClose{}}
}

var (
	ErrNegativeInt = errNegativeInt
)
