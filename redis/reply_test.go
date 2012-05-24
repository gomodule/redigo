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
	"net"
	"reflect"
	"testing"
	"time"
)

func ExampleBool() {
	c, err := dial()
	if err != nil {
		return
	}
	defer c.Close()

	c.Do("SET", "foo", 1)
	exists, _ := redis.Bool(c.Do("EXISTS", "foo"))
	fmt.Printf("%#v\n", exists)
	// Output:
	// true
}

func ExampleInt() {
	c, err := dial()
	if err != nil {
		return
	}
	defer c.Close()

	c.Do("SET", "k1", 1)
	n, _ := redis.Int(c.Do("GET", "k1"))
	fmt.Printf("%#v\n", n)
	n, _ = redis.Int(c.Do("INCR", "k1"))
	fmt.Printf("%#v\n", n)
	// Output:
	// 1
	// 2
}

func ExampleString() {
	c, err := dial()
	if err != nil {
		return
	}
	defer c.Close()

	c.Do("SET", "hello", "world")
	s, err := redis.String(c.Do("GET", "hello"))
	fmt.Printf("%#v\n", s)
	// Output:
	// "world"
}

func ExampleNotification(c redis.Conn) {
	c.Send("SUBSCRIBE", "mychannel")
	for {
		n, err := redis.Notification(c.Receive())
		if err != nil {
			break
		}
		switch n := n.(type) {
		case redis.Message:
			fmt.Printf("%s: message: %s", n.Channel, n.Data)
		case redis.Subscription:
			fmt.Printf("%s: %s %d", n.Channel, n.Kind, n.Count)
		default:
			panic("unexpected")
		}
	}
}

func expectNotification(t *testing.T, c redis.Conn, message string, expected interface{}) {
	actual, err := redis.Notification(c.Receive())
	if err != nil {
		t.Errorf("%s returned error %v", message, err)
		return
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("%s = %v, want %v", message, actual, expected)
	}
}

func TestNotification(t *testing.T) {
	pc, err := dial()
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	nc, err := net.Dial("tcp", ":6379")
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	nc.SetReadDeadline(time.Now().Add(4 * time.Second))

	c := redis.NewConn(nc)

	c.Send("SUBSCRIBE", "c1")
	expectNotification(t, c, "Subscribe(c1)", redis.Subscription{"subscribe", "c1", 1})
	c.Send("SUBSCRIBE", "c2")
	expectNotification(t, c, "Subscribe(c2)", redis.Subscription{"subscribe", "c2", 2})
	c.Send("PSUBSCRIBE", "p1")
	expectNotification(t, c, "PSubscribe(p1)", redis.Subscription{"psubscribe", "p1", 3})
	c.Send("PSUBSCRIBE", "p2")
	expectNotification(t, c, "PSubscribe(p2)", redis.Subscription{"psubscribe", "p2", 4})
	c.Send("PUNSUBSCRIBE")
	expectNotification(t, c, "Punsubscribe(p1)", redis.Subscription{"punsubscribe", "p1", 3})
	expectNotification(t, c, "Punsubscribe()", redis.Subscription{"punsubscribe", "p2", 2})

	pc.Do("PUBLISH", "c1", "hello")
	expectNotification(t, c, "PUBLISH c1 hello", redis.Message{"c1", []byte("hello")})
}
