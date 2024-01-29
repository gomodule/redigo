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
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

func TestPushed(t *testing.T) {
	pc, err := redis.DialDefaultServer()
	require.NoError(t, err)
	defer pc.Close()

	sc, err := redis.DialDefaultServer()
	require.NoError(t, err)
	defer sc.Close()

	c := redis.PubSubConn{Conn: sc}

	require.NoError(t, c.Subscribe("c1"))
	require.Equal(t, redis.Subscription{Kind: "subscribe", Channel: "c1", Count: 1}, c.Receive())
	require.NoError(t, c.Subscribe("c2"))
	require.Equal(t, redis.Subscription{Kind: "subscribe", Channel: "c2", Count: 2}, c.Receive())
	require.NoError(t, c.PSubscribe("p1"))
	require.Equal(t, redis.Subscription{Kind: "psubscribe", Channel: "p1", Count: 3}, c.Receive())
	require.NoError(t, c.PSubscribe("p2"))
	require.Equal(t, redis.Subscription{Kind: "psubscribe", Channel: "p2", Count: 4}, c.Receive())
	require.NoError(t, c.PUnsubscribe())

	// Response can return in any order.
	v := c.Receive()
	require.IsType(t, redis.Subscription{}, v)
	u := v.(redis.Subscription)
	expected1 := redis.Subscription{Kind: "punsubscribe", Channel: "p1", Count: 3}
	expected2 := redis.Subscription{Kind: "punsubscribe", Channel: "p2", Count: 2}
	if u.Channel == "p2" {
		// Order reversed.
		expected1.Channel = "p2"
		expected2.Channel = "p1"
	}
	require.Equal(t, expected1, u)
	require.Equal(t, expected2, c.Receive())

	_, err = pc.Do("PUBLISH", "c1", "hello")
	require.NoError(t, err)
	require.Equal(t, redis.Message{Channel: "c1", Data: []byte("hello")}, c.Receive())

	require.NoError(t, c.Ping("hello"))
	require.Equal(t, redis.Pong{Data: "hello"}, c.Receive())

	require.NoError(t, c.Conn.Send("PING"))
	c.Conn.Flush()
	require.Equal(t, redis.Pong{}, c.Receive())

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
	require.Equal(t, redis.Subscription{Kind: "subscribe", Channel: "c1", Count: 1}, c.Receive())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := c.ReceiveContext(ctx)
	if err, ok := got.(error); !ok {
		t.Errorf("recv w/canceled expected Canceled got non-error type %T", got)
	} else if !errors.Is(err, context.Canceled) {
		t.Errorf("recv w/canceled expected Canceled got %v", err)
	}
}
