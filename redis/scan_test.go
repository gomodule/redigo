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
	"github.com/stretchr/testify/require"
)

type durationScan struct {
	time.Duration `redis:"sd"`
}

func (t *durationScan) RedisScan(src interface{}) (err error) {
	if t == nil {
		return fmt.Errorf("nil pointer")
	}
	switch src := src.(type) {
	case string:
		t.Duration, err = time.ParseDuration(src)
	case []byte:
		t.Duration, err = time.ParseDuration(string(src))
	case int64:
		t.Duration = time.Duration(src)
	default:
		err = fmt.Errorf("cannot convert from %T to %T", src, t)
	}
	return err
}

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
	{"hello", "hello"},
	{[]byte("hello"), "hello"},
	{[]byte("world"), []byte("world")},
	{nil, ""},
	{nil, []byte(nil)},

	{[]interface{}{[]byte("b1")}, []interface{}{[]byte("b1")}},
	{[]interface{}{[]byte("b2")}, []string{"b2"}},
	{[]interface{}{[]byte("b3"), []byte("b4")}, []string{"b3", "b4"}},
	{[]interface{}{[]byte("b5")}, [][]byte{[]byte("b5")}},
	{[]interface{}{[]byte("1")}, []int{1}},
	{[]interface{}{[]byte("1"), []byte("2")}, []int{1, 2}},
	{[]interface{}{[]byte("1"), []byte("2")}, []float64{1, 2}},
	{[]interface{}{[]byte("1")}, []byte{1}},
	{[]interface{}{[]byte("1")}, []bool{true}},

	{[]interface{}{"s1"}, []interface{}{"s1"}},
	{[]interface{}{"s2"}, [][]byte{[]byte("s2")}},
	{[]interface{}{"s3", "s4"}, []string{"s3", "s4"}},
	{[]interface{}{"s5"}, [][]byte{[]byte("s5")}},
	{[]interface{}{"1"}, []int{1}},
	{[]interface{}{"1", "2"}, []int{1, 2}},
	{[]interface{}{"1", "2"}, []float64{1, 2}},
	{[]interface{}{"1"}, []byte{1}},
	{[]interface{}{"1"}, []bool{true}},

	{[]interface{}{nil, "2"}, []interface{}{nil, "2"}},
	{[]interface{}{nil, []byte("2")}, [][]byte{nil, []byte("2")}},

	{[]interface{}{redis.Error("e1")}, []interface{}{redis.Error("e1")}},
	{[]interface{}{redis.Error("e2")}, [][]byte{[]byte("e2")}},
	{[]interface{}{redis.Error("e3")}, []string{"e3"}},

	{"1m", durationScan{Duration: time.Minute}},
	{[]byte("1m"), durationScan{Duration: time.Minute}},
	{time.Minute.Nanoseconds(), durationScan{Duration: time.Minute}},
	{[]interface{}{[]byte("1m")}, []durationScan{{Duration: time.Minute}}},
	{[]interface{}{[]byte("1m")}, []*durationScan{{Duration: time.Minute}}},
}

func TestScanConversion(t *testing.T) {
	for _, tt := range scanConversionTests {
		values := []interface{}{tt.src}
		dest := reflect.New(reflect.TypeOf(tt.dest))
		_, err := redis.Scan(values, dest.Interface())
		if err != nil {
			t.Errorf("Scan(%v) returned error %v", tt, err)
			continue
		}
		if !reflect.DeepEqual(tt.dest, dest.Elem().Interface()) {
			t.Errorf("Scan(%v) returned %v, want %v", tt, dest.Elem().Interface(), tt.dest)
		}
	}
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
	{redis.Error("blah"), durationScan{Duration: time.Minute}},
	{"invalid", durationScan{Duration: time.Minute}},
}

func TestScanConversionError(t *testing.T) {
	for _, tt := range scanConversionErrorTests {
		values := []interface{}{tt.src}
		dest := reflect.New(reflect.TypeOf(tt.dest))
		_, err := redis.Scan(values, dest.Interface())
		if err == nil {
			t.Errorf("Scan(%v) did not return error", tt)
		}
	}
}

func ExampleScan() {
	c, err := dial()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.Close()

	if err = c.Send("HMSET", "album:1", "title", "Red", "rating", 5); err != nil {
		fmt.Println(err)
		return
	}
	if err = c.Send("HMSET", "album:2", "title", "Earthbound", "rating", 1); err != nil {
		fmt.Println(err)
		return
	}
	if err = c.Send("HMSET", "album:3", "title", "Beat"); err != nil {
		fmt.Println(err)
		return
	}
	if err = c.Send("LPUSH", "albums", "1"); err != nil {
		fmt.Println(err)
		return
	}
	if err = c.Send("LPUSH", "albums", "2"); err != nil {
		fmt.Println(err)
		return
	}
	if err = c.Send("LPUSH", "albums", "3"); err != nil {
		fmt.Println(err)
		return
	}
	values, err := redis.Values(c.Do("SORT", "albums",
		"BY", "album:*->rating",
		"GET", "album:*->title",
		"GET", "album:*->rating"))
	if err != nil {
		fmt.Println(err)
		return
	}

	for len(values) > 0 {
		var title string
		rating := -1 // initialize to illegal value to detect nil.
		values, err = redis.Scan(values, &title, &rating)
		if err != nil {
			fmt.Println(err)
			return
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
	X    int    `redis:"-"`
	I    int    `redis:"i"`
	U    uint   `redis:"u"`
	S    string `redis:"s"`
	P    []byte `redis:"p"`
	B    bool   `redis:"b"`
	Bt   bool
	Bf   bool
	PtrB *bool
	s0
	Sd  durationScan  `redis:"sd"`
	Sdp *durationScan `redis:"sdp"`
}

var (
	boolTrue = true
	int5     = 5
)

var scanStructTests = []struct {
	name     string
	reply    []string
	expected interface{}
}{
	{"basic",
		[]string{
			"i", "-1234",
			"u", "5678",
			"s", "hello",
			"p", "world",
			"b", "t",
			"Bt", "1",
			"Bf", "0",
			"PtrB", "1",
			"X", "123",
			"y", "456",
			"sd", "1m",
			"sdp", "1m",
		},
		&s1{
			I:    -1234,
			U:    5678,
			S:    "hello",
			P:    []byte("world"),
			B:    true,
			Bt:   true,
			Bf:   false,
			PtrB: &boolTrue,
			s0:   s0{X: 123, Y: 456},
			Sd:   durationScan{Duration: time.Minute},
			Sdp:  &durationScan{Duration: time.Minute},
		},
	},
	{"absent values",
		[]string{},
		&s1{},
	},
	{"struct-anonymous-nil",
		[]string{"edi", "2"},
		&struct {
			Ed
			*Edp
		}{
			Ed: Ed{EdI: 2},
		},
	},
	{"struct-anonymous-multi-nil-early",
		[]string{"edi", "2"},
		&struct {
			Ed
			*Ed2
		}{
			Ed: Ed{EdI: 2},
		},
	},
	{"struct-anonymous-multi-nil-late",
		[]string{"edi", "2", "ed2i", "3", "edp2i", "4"},
		&struct {
			Ed
			*Ed2
		}{
			Ed: Ed{EdI: 2},
			Ed2: &Ed2{
				Ed2I: 3,
				Edp2: &Edp2{
					Edp2I: 4,
				},
			},
		},
	},
}

func TestScanStruct(t *testing.T) {
	for _, tt := range scanStructTests {
		t.Run(tt.name, func(t *testing.T) {
			reply := make([]interface{}, len(tt.reply))
			for i, v := range tt.reply {
				reply[i] = []byte(v)
			}

			value := reflect.New(reflect.ValueOf(tt.expected).Type().Elem()).Interface()
			err := redis.ScanStruct(reply, value)
			require.NoError(t, err)
			require.Equal(t, tt.expected, value)
		})
	}
}

func TestBadScanStructArgs(t *testing.T) {
	x := []interface{}{"A", "b"}
	test := func(v interface{}) {
		if err := redis.ScanStruct(x, v); err == nil {
			t.Errorf("Expect error for ScanStruct(%T, %T)", x, v)
		}
	}

	test(nil)

	var v0 *struct{}
	test(v0)

	var v1 int
	test(&v1)

	x = x[:1]
	v2 := struct{ A string }{}
	test(&v2)
}

type sliceScanner struct {
	Field string
}

func (ss *sliceScanner) RedisScan(s interface{}) error {
	v, ok := s.([]interface{})
	if !ok {
		return fmt.Errorf("invalid type %T", s)
	}
	return redis.ScanStruct(v, ss)
}

var scanSliceTests = []struct {
	name       string
	src        []interface{}
	fieldNames []string
	ok         bool
	dest       interface{}
}{
	{
		"scanner",
		[]interface{}{[]interface{}{[]byte("Field"), []byte("1")}},
		nil,
		true,
		[]*sliceScanner{{"1"}},
	},
	{
		"int",
		[]interface{}{[]byte("1"), nil, []byte("-1")},
		nil,
		true,
		[]int{1, 0, -1},
	},
	{
		"uint",
		[]interface{}{[]byte("1"), nil, []byte("2")},
		nil,
		true,
		[]uint{1, 0, 2},
	},
	{
		"uint-error",
		[]interface{}{[]byte("-1")},
		nil,
		false,
		[]uint{1},
	},
	{
		"[]byte",
		[]interface{}{[]byte("hello"), nil, []byte("world")},
		nil,
		true,
		[][]byte{[]byte("hello"), nil, []byte("world")},
	},
	{
		"string",
		[]interface{}{[]byte("hello"), nil, []byte("world")},
		nil,
		true,
		[]string{"hello", "", "world"},
	},
	{
		"struct",
		[]interface{}{[]byte("a1"), []byte("b1"), []byte("a2"), []byte("b2")},
		nil,
		true,
		[]struct{ A, B string }{{"a1", "b1"}, {"a2", "b2"}},
	},
	{
		"struct-error",
		[]interface{}{[]byte("a1"), []byte("b1")},
		nil,
		false,
		[]struct{ A, B, C string }{{"a1", "b1", ""}},
	},
	{
		"struct-field-names",
		[]interface{}{[]byte("a1"), []byte("b1"), []byte("a2"), []byte("b2")},
		[]string{"A", "B"},
		true,
		[]struct{ A, C, B string }{{"a1", "", "b1"}, {"a2", "", "b2"}},
	},
	{
		"struct-no-fields",
		[]interface{}{[]byte("a1"), []byte("b1"), []byte("a2"), []byte("b2")},
		nil,
		false,
		[]struct{}{},
	},
}

func TestScanSlice(t *testing.T) {
	for _, tt := range scanSliceTests {
		t.Run(tt.name, func(t *testing.T) {
			typ := reflect.ValueOf(tt.dest).Type()
			dest := reflect.New(typ)

			err := redis.ScanSlice(tt.src, dest.Interface(), tt.fieldNames...)
			if tt.ok != (err == nil) {
				t.Fatalf("ScanSlice(%v, []%s, %v) returned error %v", tt.src, typ, tt.fieldNames, err)
			}
			if tt.ok && !reflect.DeepEqual(dest.Elem().Interface(), tt.dest) {
				t.Errorf("ScanSlice(src, []%s) returned %#v, want %#v", typ, dest.Elem().Interface(), tt.dest)
			}
		})
	}
}

func ExampleScanSlice() {
	c, err := dial()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.Close()

	if err = c.Send("HMSET", "album:1", "title", "Red", "rating", 5); err != nil {
		fmt.Println(err)
		return
	}
	if err = c.Send("HMSET", "album:2", "title", "Earthbound", "rating", 1); err != nil {
		fmt.Println(err)
		return
	}
	if err = c.Send("HMSET", "album:3", "title", "Beat", "rating", 4); err != nil {
		fmt.Println(err)
		return
	}
	if err = c.Send("LPUSH", "albums", "1"); err != nil {
		fmt.Println(err)
		return
	}
	if err = c.Send("LPUSH", "albums", "2"); err != nil {
		fmt.Println(err)
		return
	}
	if err = c.Send("LPUSH", "albums", "3"); err != nil {
		fmt.Println(err)
		return
	}
	values, err := redis.Values(c.Do("SORT", "albums",
		"BY", "album:*->rating",
		"GET", "album:*->title",
		"GET", "album:*->rating"))
	if err != nil {
		fmt.Println(err)
		return
	}

	var albums []struct {
		Title  string
		Rating int
	}
	if err := redis.ScanSlice(values, &albums); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%v\n", albums)
	// Output:
	// [{Earthbound 1} {Beat 4} {Red 5}]
}

type Ed struct {
	EdI int `redis:"edi"`
}

type Edp struct {
	EdpI int `redis:"edpi"`
}

type Ed2 struct {
	Ed2I int `redis:"ed2i"`
	*Edp2
}

type Edp2 struct {
	Edp2I int `redis:"edp2i"`
	*Edp
}

type Edpr1 struct {
	Edpr1I int `redis:"edpr1i"`
	*Edpr2
}

type Edpr2 struct {
	Edpr2I int `redis:"edpr2i"`
	*Edpr1
}

var argsTests = []struct {
	title    string
	fn       func() redis.Args
	expected redis.Args
	panics   bool
}{
	{"struct-ptr",
		func() redis.Args {
			return redis.Args{}.AddFlat(&struct {
				I     int               `redis:"i"`
				U     uint              `redis:"u"`
				S     string            `redis:"s"`
				P     []byte            `redis:"p"`
				M     map[string]string `redis:"m"`
				Bt    bool
				Bf    bool
				PtrB  *bool
				PtrI  *int
				PtrI2 *int
			}{
				-1234, 5678, "hello", []byte("world"), map[string]string{"hello": "world"}, true, false, &boolTrue, nil, &int5,
			})
		},
		redis.Args{"i", int(-1234), "u", uint(5678), "s", "hello", "p", []byte("world"), "m", map[string]string{"hello": "world"}, "Bt", true, "Bf", false, "PtrB", true, "PtrI2", 5},
		false,
	},
	{"struct",
		func() redis.Args {
			return redis.Args{}.AddFlat(struct{ I int }{123})
		},
		redis.Args{"I", 123},
		false,
	},
	{"struct-with-RedisArg-direct",
		func() redis.Args {
			return redis.Args{}.AddFlat(struct{ T CustomTime }{CustomTime{Time: time.Unix(1573231058, 0)}})
		},
		redis.Args{"T", int64(1573231058)},
		false,
	},
	{"struct-with-RedisArg-direct-ptr",
		func() redis.Args {
			return redis.Args{}.AddFlat(struct{ T *CustomTime }{&CustomTime{Time: time.Unix(1573231058, 0)}})
		},
		redis.Args{"T", int64(1573231058)},
		false,
	},
	{"struct-with-RedisArg-ptr",
		func() redis.Args {
			return redis.Args{}.AddFlat(struct{ T *CustomTimePtr }{&CustomTimePtr{Time: time.Unix(1573231058, 0)}})
		},
		redis.Args{"T", int64(1573231058)},
		false,
	},
	{"slice",
		func() redis.Args {
			return redis.Args{}.Add(1).AddFlat([]string{"a", "b", "c"}).Add(2)
		},
		redis.Args{1, "a", "b", "c", 2},
		false,
	},
	{"struct-omitempty",
		func() redis.Args {
			return redis.Args{}.AddFlat(&struct {
				Sdp *durationArg `redis:"Sdp,omitempty"`
			}{
				nil,
			})
		},
		redis.Args{},
		false,
	},
	{"struct-anonymous",
		func() redis.Args {
			return redis.Args{}.AddFlat(struct {
				Ed
				*Edp
			}{
				Ed{EdI: 2},
				&Edp{EdpI: 3},
			})
		},
		redis.Args{"edi", 2, "edpi", 3},
		false,
	},
	{"struct-anonymous-nil",
		func() redis.Args {
			return redis.Args{}.AddFlat(struct {
				Ed
				*Edp
			}{
				Ed: Ed{EdI: 2},
			})
		},
		redis.Args{"edi", 2},
		false,
	},
	{"struct-anonymous-multi-nil-early",
		func() redis.Args {
			return redis.Args{}.AddFlat(struct {
				Ed
				*Ed2
			}{
				Ed: Ed{EdI: 2},
			})
		},
		redis.Args{"edi", 2},
		false,
	},
	{"struct-anonymous-multi-nil-late",
		func() redis.Args {
			return redis.Args{}.AddFlat(struct {
				Ed
				*Ed2
			}{
				Ed: Ed{EdI: 2},
				Ed2: &Ed2{
					Ed2I: 3,
					Edp2: &Edp2{
						Edp2I: 4,
					},
				},
			})
		},
		redis.Args{"edi", 2, "ed2i", 3, "edp2i", 4},
		false,
	},
	{"struct-recursive-ptr",
		func() redis.Args {
			return redis.Args{}.AddFlat(struct {
				Edpr1
			}{
				Edpr1: Edpr1{
					Edpr1I: 1,
					Edpr2: &Edpr2{
						Edpr2I: 2,
						Edpr1: &Edpr1{
							Edpr1I: 10,
						},
					},
				},
			})
		},
		redis.Args{},
		true,
	},
}

func TestArgs(t *testing.T) {
	for _, tt := range argsTests {
		t.Run(tt.title, func(t *testing.T) {
			if tt.panics {
				require.Panics(t, func() { tt.fn() })
				return
			}
			require.Equal(t, tt.expected, tt.fn())
		})
	}
}

type CustomTimePtr struct {
	time.Time
}

func (t *CustomTimePtr) RedisArg() interface{} {
	return t.Unix()
}

type CustomTime struct {
	time.Time
}

func (t CustomTime) RedisArg() interface{} {
	return t.Unix()
}

type InnerStruct struct {
	Foo int64
}

func (f *InnerStruct) RedisScan(src interface{}) (err error) {
	switch s := src.(type) {
	case []byte:
		f.Foo, err = strconv.ParseInt(string(s), 10, 64)
	case string:
		f.Foo, err = strconv.ParseInt(s, 10, 64)
	default:
		return fmt.Errorf("invalid type %T", src)
	}
	return err
}

type OuterStruct struct {
	Inner *InnerStruct
}

func TestScanPtrRedisScan(t *testing.T) {
	tests := []struct {
		name     string
		src      []interface{}
		dest     OuterStruct
		expected OuterStruct
	}{
		{
			name:     "value-to-nil",
			src:      []interface{}{[]byte("1234"), nil},
			dest:     OuterStruct{&InnerStruct{}},
			expected: OuterStruct{Inner: &InnerStruct{Foo: 1234}},
		},
		{
			name:     "nil-to-nil",
			src:      []interface{}{[]byte(nil), nil},
			dest:     OuterStruct{},
			expected: OuterStruct{},
		},
		{
			name:     "value-to-value",
			src:      []interface{}{[]byte("1234"), nil},
			dest:     OuterStruct{Inner: &InnerStruct{Foo: 5678}},
			expected: OuterStruct{Inner: &InnerStruct{Foo: 1234}},
		},
		{
			name:     "nil-to-value",
			src:      []interface{}{[]byte(nil), nil},
			dest:     OuterStruct{Inner: &InnerStruct{Foo: 1234}},
			expected: OuterStruct{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := redis.Scan(tc.src, &tc.dest.Inner)
			require.NoError(t, err)
			require.Equal(t, tc.expected, tc.dest)
		})
	}
}

func ExampleArgs() {
	c, err := dial()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.Close()

	var p1, p2 struct {
		Title  string `redis:"title"`
		Author string `redis:"author"`
		Body   string `redis:"body"`
	}

	p1.Title = "Example"
	p1.Author = "Gary"
	p1.Body = "Hello"

	if _, err := c.Do("HMSET", redis.Args{}.Add("id1").AddFlat(&p1)...); err != nil {
		fmt.Println(err)
		return
	}

	m := map[string]string{
		"title":  "Example2",
		"author": "Steve",
		"body":   "Map",
	}

	if _, err := c.Do("HMSET", redis.Args{}.Add("id2").AddFlat(m)...); err != nil {
		fmt.Println(err)
		return
	}

	for _, id := range []string{"id1", "id2"} {

		v, err := redis.Values(c.Do("HGETALL", id))
		if err != nil {
			fmt.Println(err)
			return
		}

		if err := redis.ScanStruct(v, &p2); err != nil {
			fmt.Println(err)
			return
		}

		fmt.Printf("%+v\n", p2)
	}

	// Output:
	// {Title:Example Author:Gary Body:Hello}
	// {Title:Example2 Author:Steve Body:Map}
}
