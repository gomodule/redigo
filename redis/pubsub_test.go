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
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

func expectPushed(t *testing.T, c redis.PubSubConn, message string, expected interface{}) {
	actual := c.Receive()
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("%s = %v, want %v", message, actual, expected)
	}
}

func TestPushed(t *testing.T) {
	pc, err := redis.DialDefaultServer()
	if err != nil {
		t.Fatalf("error connection to database, %v", err)
	}
	defer pc.Close()

	sc, err := redis.DialDefaultServer()
	if err != nil {
		t.Fatalf("error connection to database, %v", err)
	}
	defer sc.Close()

	c := redis.PubSubConn{Conn: sc}

	require.NoError(t, c.Subscribe("c1"))
	expectPushed(t, c, "Subscribe(c1)", redis.Subscription{Kind: "subscribe", Channel: "c1", Count: 1})
	require.NoError(t, c.Subscribe("c2"))
	expectPushed(t, c, "Subscribe(c2)", redis.Subscription{Kind: "subscribe", Channel: "c2", Count: 2})
	require.NoError(t, c.PSubscribe("p1"))
	expectPushed(t, c, "PSubscribe(p1)", redis.Subscription{Kind: "psubscribe", Channel: "p1", Count: 3})
	require.NoError(t, c.PSubscribe("p2"))
	expectPushed(t, c, "PSubscribe(p2)", redis.Subscription{Kind: "psubscribe", Channel: "p2", Count: 4})
	require.NoError(t, c.PUnsubscribe())
	expectPushed(t, c, "Punsubscribe(p1)", redis.Subscription{Kind: "punsubscribe", Channel: "p1", Count: 3})
	expectPushed(t, c, "Punsubscribe()", redis.Subscription{Kind: "punsubscribe", Channel: "p2", Count: 2})

	_, err = pc.Do("PUBLISH", "c1", "hello")
	require.NoError(t, err)
	expectPushed(t, c, "PUBLISH c1 hello", redis.Message{Channel: "c1", Data: []byte("hello")})

	require.NoError(t, c.Ping("hello"))
	expectPushed(t, c, `Ping("hello")`, redis.Pong{Data: "hello"})

	require.NoError(t, c.Conn.Send("PING"))
	c.Conn.Flush()
	expectPushed(t, c, `Send("PING")`, redis.Pong{})

	require.NoError(t, c.Ping("timeout"))
	got := c.ReceiveWithTimeout(time.Minute)
	if want := (redis.Pong{Data: "timeout"}); want != got {
		t.Errorf("recv /w timeout got %v, want %v", got, want)
	}
}

func TestPubSubReceiveContext(t *testing.T) {
	sc, err := redis.DialDefaultServer()
	if err != nil {
		t.Fatalf("error connection to database, %v", err)
	}
	defer sc.Close()

	c := redis.PubSubConn{Conn: sc}

	require.NoError(t, c.Subscribe("c1"))
	expectPushed(t, c, "Subscribe(c1)", redis.Subscription{Kind: "subscribe", Channel: "c1", Count: 1})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := c.ReceiveContext(ctx)
	if err, ok := got.(error); !ok {
		t.Errorf("recv w/canceled expected Canceled got non-error type %T", got)
	} else if !errors.Is(err, context.Canceled) {
		t.Errorf("recv w/canceled expected Canceled got %v", err)
	}
}
