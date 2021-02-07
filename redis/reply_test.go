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
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

var (
	maxUint64Str = strconv.FormatUint(math.MaxUint64, 10)
)

type valueError struct {
	v   interface{}
	err error
}

func ve(v interface{}, err error) valueError {
	return valueError{v, err}
}

var replyTests = []struct {
	name     interface{}
	actual   valueError
	expected valueError
}{
	{
		"ints([[]byte, []byte])",
		ve(redis.Ints([]interface{}{[]byte("4"), []byte("5")}, nil)),
		ve([]int{4, 5}, nil),
	},
	{
		"ints([nt64, int64])",
		ve(redis.Ints([]interface{}{int64(4), int64(5)}, nil)),
		ve([]int{4, 5}, nil),
	},
	{
		"ints([[]byte, nil, []byte])",
		ve(redis.Ints([]interface{}{[]byte("4"), nil, []byte("5")}, nil)),
		ve([]int{4, 0, 5}, nil),
	},
	{
		"ints(nil)",
		ve(redis.Ints(nil, nil)),
		ve([]int(nil), redis.ErrNil),
	},
	{
		"int64s([[]byte, []byte])",
		ve(redis.Int64s([]interface{}{[]byte("4"), []byte("5")}, nil)),
		ve([]int64{4, 5}, nil),
	},
	{
		"int64s([int64, int64])",
		ve(redis.Int64s([]interface{}{int64(4), int64(5)}, nil)),
		ve([]int64{4, 5}, nil),
	},
	{
		"uint64s([[]byte, []byte])",
		ve(redis.Uint64s([]interface{}{[]byte(maxUint64Str), []byte("5")}, nil)),
		ve([]uint64{math.MaxUint64, 5}, nil),
	},
	{
		"Uint64Map([[]byte, []byte])",
		ve(redis.Uint64Map([]interface{}{[]byte("key1"), []byte(maxUint64Str), []byte("key2"), []byte("5")}, nil)),
		ve(map[string]uint64{"key1": math.MaxUint64, "key2": 5}, nil),
	},
	{
		"strings([[]byte, []byte])",
		ve(redis.Strings([]interface{}{[]byte("v1"), []byte("v2")}, nil)),
		ve([]string{"v1", "v2"}, nil),
	},
	{
		"strings([string, string])",
		ve(redis.Strings([]interface{}{"v1", "v2"}, nil)),
		ve([]string{"v1", "v2"}, nil),
	},
	{
		"byteslices([v1, v2])",
		ve(redis.ByteSlices([]interface{}{[]byte("v1"), []byte("v2")}, nil)),
		ve([][]byte{[]byte("v1"), []byte("v2")}, nil),
	},
	{
		"float64s([v1, v2])",
		ve(redis.Float64s([]interface{}{[]byte("1.234"), []byte("5.678")}, nil)),
		ve([]float64{1.234, 5.678}, nil),
	},
	{
		"values([v1, v2])",
		ve(redis.Values([]interface{}{[]byte("v1"), []byte("v2")}, nil)),
		ve([]interface{}{[]byte("v1"), []byte("v2")}, nil),
	},
	{
		"values(nil)",
		ve(redis.Values(nil, nil)),
		ve([]interface{}(nil), redis.ErrNil),
	},
	{
		"float64(1.0)",
		ve(redis.Float64([]byte("1.0"), nil)),
		ve(float64(1.0), nil),
	},
	{
		"float64(nil)",
		ve(redis.Float64(nil, nil)),
		ve(float64(0.0), redis.ErrNil),
	},
	{
		"uint64(1)",
		ve(redis.Uint64(int64(1), nil)),
		ve(uint64(1), nil),
	},
	{
		"uint64(-1)",
		ve(redis.Uint64(int64(-1), nil)),
		ve(uint64(0), redis.ErrNegativeInt(-1)),
	},
	{
		"positions([[1, 2], nil, [3, 4]])",
		ve(redis.Positions([]interface{}{[]interface{}{[]byte("1"), []byte("2")}, nil, []interface{}{[]byte("3"), []byte("4")}}, nil)),
		ve([]*[2]float64{{1.0, 2.0}, nil, {3.0, 4.0}}, nil),
	},
	{
		"SlowLogs(1, 1579625870, 3, {set, x, y}, localhost:1234, testClient",
		ve(getSlowLog()),
		ve(redis.SlowLog{ID: 1, Time: time.Unix(1579625870, 0), ExecutionTime: time.Duration(3) * time.Microsecond, Args: []string{"set", "x", "y"}, ClientAddr: "localhost:1234", ClientName: "testClient"}, nil),
	},
}

func getSlowLog() (redis.SlowLog, error) {
	slowLogs, _ := redis.SlowLogs([]interface{}{[]interface{}{int64(1), int64(1579625870), int64(3), []interface{}{"set", "x", "y"}, "localhost:1234", "testClient"}}, nil)
	if err != nil {
		return redis.SlowLog{}, err
	}
	return slowLogs[0], nil
}

func TestReply(t *testing.T) {
	for _, rt := range replyTests {
		if rt.actual.err != rt.expected.err && rt.actual.err.Error() != rt.expected.err.Error() {
			t.Errorf("%s returned err %v, want %v", rt.name, rt.actual.err, rt.expected.err)
			continue
		}
		if !reflect.DeepEqual(rt.actual.v, rt.expected.v) {
			t.Errorf("%s=%+v, want %+v", rt.name, rt.actual.v, rt.expected.v)
		}
	}
}

func TestSlowLog(t *testing.T) {
	c, err := dial()
	if err != nil {
		t.Errorf("TestSlowLog failed during dial with error " + err.Error())
		return
	}
	defer c.Close()

	resultStr, err := redis.Strings(c.Do("CONFIG", "GET", "slowlog-log-slower-than"))
	if err != nil {
		t.Errorf("TestSlowLog failed during CONFIG GET slowlog-log-slower-than with error " + err.Error())
		return
	}
	// in case of older verion < 2.2.12 where SLOWLOG command is not supported
	// don't run the test
	if len(resultStr) == 0 {
		return
	}
	slowLogSlowerThanOldCfg, err := strconv.Atoi(resultStr[1])
	if err != nil {
		t.Errorf("TestSlowLog failed during strconv.Atoi with error " + err.Error())
		return
	}
	result, err := c.Do("CONFIG", "SET", "slowlog-log-slower-than", "0")
	if err != nil && result != "OK" {
		t.Errorf("TestSlowLog failed during CONFIG SET with error " + err.Error())
		return
	}
	result, err = c.Do("SLOWLOG", "GET")
	if err != nil {
		t.Errorf("TestSlowLog failed during SLOWLOG GET with error " + err.Error())
		return
	}
	slowLogs, err := redis.SlowLogs(result, err)
	if err != nil {
		t.Errorf("TestSlowLog failed during redis.SlowLogs with error " + err.Error())
		return
	}
	slowLog := slowLogs[0]
	if slowLog.Args[0] != "CONFIG" ||
		slowLog.Args[1] != "SET" ||
		slowLog.Args[2] != "slowlog-log-slower-than" ||
		slowLog.Args[3] != "0" {
		t.Errorf("%s=%+v, want %+v", "TestSlowLog test failed : ",
			slowLog.Args[0]+" "+slowLog.Args[1]+" "+slowLog.Args[2]+" "+
				slowLog.Args[3], "CONFIG SET slowlog-log-slower-than 0")
	}
	// reset the old configuration after test
	result, err = c.Do("CONFIG", "SET", "slowlog-log-slower-than", slowLogSlowerThanOldCfg)
	if err != nil && result != "OK" {
		t.Errorf("TestSlowLog failed during CONFIG SET with error " + err.Error())
		return
	}
}

// dial wraps DialDefaultServer() with a more suitable function name for examples.
func dial() (redis.Conn, error) {
	return redis.DialDefaultServer()
}

// serverAddr wraps DefaultServerAddr() with a more suitable function name for examples.
func serverAddr() (string, error) {
	return redis.DefaultServerAddr()
}

func ExampleBool() {
	c, err := dial()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.Close()

	if _, err = c.Do("SET", "foo", 1); err != nil {
		fmt.Println(err)
		return
	}
	exists, err := redis.Bool(c.Do("EXISTS", "foo"))
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("%#v\n", exists)
	// Output:
	// true
}

func ExampleInt() {
	c, err := dial()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.Close()

	_, err = c.Do("SET", "k1", 1)
	if err != nil {
		fmt.Println(err)
		return
	}
	n, err := redis.Int(c.Do("GET", "k1"))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%#v\n", n)
	n, err = redis.Int(c.Do("INCR", "k1"))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%#v\n", n)
	// Output:
	// 1
	// 2
}

func ExampleInts() {
	c, err := dial()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.Close()

	_, err = c.Do("SADD", "set_with_integers", 4, 5, 6)
	if err != nil {
		fmt.Println(err)
		return
	}
	ints, err := redis.Ints(c.Do("SMEMBERS", "set_with_integers"))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%#v\n", ints)
	// Output:
	// []int{4, 5, 6}
}

func ExampleString() {
	c, err := dial()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.Close()

	_, err = c.Do("SET", "hello", "world")
	if err != nil {
		fmt.Println(err)
		return
	}
	s, err := redis.String(c.Do("GET", "hello"))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%#v\n", s)
	// Output:
	// "world"
}
