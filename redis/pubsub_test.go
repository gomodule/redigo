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
	"sync"
	"testing"
	"time"
)

func publish(channel, value interface{}) {
	c, err := dial()
	if err != nil {
		panic(err)
	}
	defer c.Close()
	c.Do("PUBLISH", channel, value)
}

// Applications can receive pushed messages from one goroutine and manage subscriptions from another goroutine.
func ExamplePubSubConn() {
	c, err := dial()
	if err != nil {
		panic(err)
	}
	defer c.Close()
	var wg sync.WaitGroup
	wg.Add(2)

	psc := redis.PubSubConn{c}

	// This goroutine receives and prints pushed messages from the server. The
	// goroutine exits when the connection is unsubscribed from all channels or
	// there is an error.
	go func() {
		defer wg.Done()
		for {
			switch n := psc.Receive().(type) {
			case redis.Message:
				fmt.Printf("%s: message: %s\n", n.Channel, n.Data)
			case redis.PMessage:
				fmt.Printf("(%s) %s: message: %s\n", n.Pattern, n.Channel, n.Data)
			case redis.Subscription:
				fmt.Printf("%s: %s %d\n", n.Channel, n.Kind, n.Count)
				if n.Count == 0 {
					return
				}
			case error:
				fmt.Printf("error: %v\n", n)
				return
			}
		}
	}()

	// This goroutine manages subscriptions for the connection. 
	go func() {
		defer wg.Done()

		psc.Subscribe("example")
		psc.PSubscribe("p*")

		// The following function calls publish a message using another
		// connection to the Redis server.
		publish("example", "hello")
		publish("example", "world")
		publish("pexample", "hello")
		publish("pexample", "world")

		// Unsubscribe from all connections. This will cause the receiving
		// goroutine to exit.
		psc.Unsubscribe()
		psc.PUnsubscribe()
	}()

	wg.Wait()

	// Output:
	// example: subscribe 1
	// p*: psubscribe 2
	// example: message: hello
	// example: message: world
	// (p*) pexample: message: hello
	// (p*) pexample: message: world
	// example: unsubscribe 1
	// p*: punsubscribe 0
}

func expectPushed(t *testing.T, c redis.PubSubConn, message string, expected interface{}) {
	actual := c.Receive()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("%s = %v, want %v", message, actual, expected)
	}
}

func TestPushed(t *testing.T) {
	pc := dialt(t)
	defer pc.Close()

	nc, err := net.Dial("tcp", ":6379")
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	nc.SetReadDeadline(time.Now().Add(4 * time.Second))

	c := redis.PubSubConn{redis.NewConn(nc, 0, 0)}

	c.Subscribe("c1")
	expectPushed(t, c, "Subscribe(c1)", redis.Subscription{"subscribe", "c1", 1})
	c.Subscribe("c2")
	expectPushed(t, c, "Subscribe(c2)", redis.Subscription{"subscribe", "c2", 2})
	c.PSubscribe("p1")
	expectPushed(t, c, "PSubscribe(p1)", redis.Subscription{"psubscribe", "p1", 3})
	c.PSubscribe("p2")
	expectPushed(t, c, "PSubscribe(p2)", redis.Subscription{"psubscribe", "p2", 4})
	c.PUnsubscribe()
	expectPushed(t, c, "Punsubscribe(p1)", redis.Subscription{"punsubscribe", "p1", 3})
	expectPushed(t, c, "Punsubscribe()", redis.Subscription{"punsubscribe", "p2", 2})

	pc.Do("PUBLISH", "c1", "hello")
	expectPushed(t, c, "PUBLISH c1 hello", redis.Message{"c1", []byte("hello")})
}
