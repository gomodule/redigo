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

package redis_test

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"math"
	"reflect"
	"testing"
)

var scanConversionTests = []struct {
	src  interface{}
	dest interface{}
}{
	{[]byte("-inf"), math.Inf(-1)},
	{[]byte("+inf"), math.Inf(1)},
	{[]byte("0"), float64(0)},
	{[]byte("3.14159"), float64(3.14159)},
	{[]byte("3.14"), float32(3.14)},
	{[]byte("-100"), int(-100)},
	{[]byte("101"), int(101)},
	{int64(102), int(102)},
	{[]byte("103"), uint(103)},
	{int64(104), uint(104)},
	{[]byte("105"), int8(105)},
	{int64(106), int8(106)},
	{[]byte("107"), uint8(107)},
	{int64(108), uint8(108)},
	{[]byte("0"), false},
	{int64(0), false},
	{[]byte("f"), false},
	{[]byte("1"), true},
	{int64(1), true},
	{[]byte("t"), true},
	{[]byte("hello"), "hello"},
	{[]byte("world"), []byte("world")},
	{[]interface{}{[]byte("foo")}, []string{"foo"}},
	{[]interface{}{[]byte("bar")}, [][]byte{[]byte("bar")}},
	{[]interface{}{[]byte("1")}, []int{1}},
	{[]interface{}{[]byte("1"), []byte("2")}, []int{1, 2}},
	{[]interface{}{[]byte("1")}, []byte{1}},
	{[]interface{}{[]byte("1")}, []bool{true}},
}

var scanConversionErrorTests = []struct {
	src  interface{}
	dest interface{}
}{
	{[]byte("1234"), byte(0)},
	{int64(1234), byte(0)},
	{[]byte("-1"), byte(0)},
	{int64(-1), byte(0)},
	{[]byte("junk"), false},
	{redis.Error("blah"), false},
}

func TestScanConversion(t *testing.T) {
	for _, tt := range scanConversionTests {
		values := []interface{}{tt.src}
		dest := reflect.New(reflect.TypeOf(tt.dest))
		values, err := redis.Scan(values, dest.Interface())
		if err != nil {
			t.Errorf("Scan(%v) returned error %v", tt, err)
			continue
		}
		if !reflect.DeepEqual(tt.dest, dest.Elem().Interface()) {
			t.Errorf("Scan(%v) returned %v, want %v", tt, dest.Elem().Interface(), tt.dest)
		}
	}
}

func TestScanConversionError(t *testing.T) {
	for _, tt := range scanConversionErrorTests {
		values := []interface{}{tt.src}
		dest := reflect.New(reflect.TypeOf(tt.dest))
		values, err := redis.Scan(values, dest.Interface())
		if err == nil {
			t.Errorf("Scan(%v) did not return error", tt)
		}
	}
}

func ExampleScan() {
	c, err := dial()
	if err != nil {
		panic(err)
	}
	defer c.Close()

	c.Send("HMSET", "album:1", "title", "Red", "rating", 5)
	c.Send("HMSET", "album:2", "title", "Earthbound", "rating", 1)
	c.Send("HMSET", "album:3", "title", "Beat")
	c.Send("LPUSH", "albums", "1")
	c.Send("LPUSH", "albums", "2")
	c.Send("LPUSH", "albums", "3")
	values, err := redis.Values(c.Do("SORT", "albums",
		"BY", "album:*->rating",
		"GET", "album:*->title",
		"GET", "album:*->rating"))
	if err != nil {
		panic(err)
	}

	for len(values) > 0 {
		var title string
		rating := -1 // initialize to illegal value to detect nil.
		values, err = redis.Scan(values, &title, &rating)
		if err != nil {
			panic(err)
		}
		if rating == -1 {
			fmt.Println(title, "not-rated")
		} else {
			fmt.Println(title, rating)
		}
	}
	// Output:
	// Beat not-rated
	// Earthbound 1
	// Red 5
}

type s0 struct {
	X  int
	Y  int `redis:"y"`
	Bt bool
}

type s1 struct {
	X  int    `redis:"-"`
	I  int    `redis:"i"`
	U  uint   `redis:"u"`
	S  string `redis:"s"`
	P  []byte `redis:"p"`
	B  bool   `redis:"b"`
	Bt bool
	Bf bool
	s0
}

var scanStructTests = []struct {
	title string
	reply []string
	value interface{}
}{
	{"basic",
		[]string{"i", "-1234", "u", "5678", "s", "hello", "p", "world", "b", "t", "Bt", "1", "Bf", "0", "X", "123", "y", "456"},
		&s1{I: -1234, U: 5678, S: "hello", P: []byte("world"), B: true, Bt: true, Bf: false, s0: s0{X: 123, Y: 456}},
	},
}

func TestScanStruct(t *testing.T) {
	for _, tt := range scanStructTests {

		var reply []interface{}
		for _, v := range tt.reply {
			reply = append(reply, []byte(v))
		}

		value := reflect.New(reflect.ValueOf(tt.value).Type().Elem())

		if err := redis.ScanStruct(reply, value.Interface()); err != nil {
			t.Fatalf("ScanStruct(%s) returned error %v", tt.title, err)
		}

		if !reflect.DeepEqual(value.Interface(), tt.value) {
			t.Fatalf("ScanStruct(%s) returned %v, want %v", tt.title, value.Interface(), tt.value)
		}
	}
}
