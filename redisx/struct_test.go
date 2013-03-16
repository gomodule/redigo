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

package redisx_test

import (
	"github.com/garyburd/redigo/redisx"
	"reflect"
	"testing"
)

var scanStructTests = []struct {
	title string
	reply []string
	value interface{}
}{
	{"basic",
		[]string{"i", "-1234", "u", "5678", "s", "hello", "p", "world", "b", "", "Bt", "1", "Bf", "0"},
		&struct {
			I  int    `redis:"i"`
			U  uint   `redis:"u"`
			S  string `redis:"s"`
			P  []byte `redis:"p"`
			B  bool   `redis:"b"`
			Bt bool
			Bf bool
		}{
			-1234, 5678, "hello", []byte("world"), false, true, false,
		},
	},
}

func TestScanStruct(t *testing.T) {
	for _, tt := range scanStructTests {

		var reply []interface{}
		for _, v := range tt.reply {
			reply = append(reply, []byte(v))
		}

		value := reflect.New(reflect.ValueOf(tt.value).Type().Elem())

		if err := redisx.ScanStruct(reply, value.Interface()); err != nil {
			t.Fatalf("ScanStruct(%s) returned error %v", tt.title, err)
		}

		if !reflect.DeepEqual(value.Interface(), tt.value) {
			t.Fatalf("ScanStruct(%s) returned %v, want %v", tt.title, value.Interface(), tt.value)
		}
	}
}

var formatStructTests = []struct {
	title string
	args  []interface{}
	value interface{}
}{
	{"basic",
		[]interface{}{"i", int(-1234), "u", uint(5678), "s", "hello", "p", []byte("world"), "Bt", true, "Bf", false},
		&struct {
			I  int    `redis:"i"`
			U  uint   `redis:"u"`
			S  string `redis:"s"`
			P  []byte `redis:"p"`
			Bt bool
			Bf bool
		}{
			-1234, 5678, "hello", []byte("world"), true, false,
		},
	},
}

func TestFormatStruct(t *testing.T) {
	for _, tt := range formatStructTests {
		args := redisx.AppendStruct(nil, tt.value)
		if !reflect.DeepEqual(args, tt.args) {
			t.Fatalf("FormatStruct(%s) returned %v, want %v", tt.title, args, tt.args)
		}
	}
}
