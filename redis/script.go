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
	"crypto/sha1"
	"encoding/hex"
	"io"
	"strings"
)

// Script encapsulates the source, hash and key count for a Lua script. See
// http://redis.io/commands/eval for information on scripts in Redis.
type Script struct {
	keyCount int
	src      string
	hash     string
}

// NewScript returns a new script object initialized with the specified number
// of keys and source code.
func NewScript(keyCount int, src string) *Script {
	h := sha1.New()
	io.WriteString(h, src)
	return &Script{keyCount, src, hex.EncodeToString(h.Sum(nil))}
}

func (s *Script) args(spec string, keysAndArgs []interface{}) []interface{} {
	args := make([]interface{}, 2+len(keysAndArgs))
	args[0] = spec
	args[1] = s.keyCount
	copy(args[2:], keysAndArgs)
	return args
}

// Do evaluates the script and returns the result. Under the covers, Do
// attempts to evaluate the script using the EVALSHA command. If the command
// fails because the script is not loaded, then Do evaluates the script using
// the EVAL command (thus causing the script to load).
func (s *Script) Do(c Conn, keysAndArgs ...interface{}) (interface{}, error) {
	v, err := c.Do("EVALSHA", s.args(s.hash, keysAndArgs)...)
	if e, ok := err.(Error); ok && strings.HasPrefix(string(e), "NOSCRIPT ") {
		v, err = c.Do("EVAL", s.args(s.src, keysAndArgs)...)
	}
	return v, err
}

// SendHash evaluates the script without waiting for the result. The script is
// evaluated with the EVALSHA command. The application must ensure that the
// script is loaded by a previous call to Send, Do or Load methods.
func (s *Script) SendHash(c Conn, keysAndArgs ...interface{}) error {
	return c.Send("EVALSHA", s.args(s.hash, keysAndArgs)...)
}

// Send evaluates the script without waiting for the result. 
func (s *Script) Send(c Conn, keysAndArgs ...interface{}) error {
	return c.Send("EVAL", s.args(s.src, keysAndArgs)...)
}

// Load loads the script without evaluating it.
func (s *Script) Load(c Conn) error {
	_, err := c.Do("SCRIPT", "LOAD", s.src)
	return err
}
