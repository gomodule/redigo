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
	"reflect"
	"runtime/debug"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

func newCluster(t *testing.T) *redis.Cluster {
	startAddrs, err := redis.FetchClusterNodes()
	if err != nil {
		t.Fatalf("error connection to database, %v", err)
	}
	cluster, err := redis.NewCluster("tcp", startAddrs)
	if err != nil {
		t.Fatal(err)
	}
	return cluster
}

func doSlot(cluster *redis.Cluster, t *testing.T, key string, value int) {
	_, err := cluster.Do("SET", key, value)
	if err != nil {
		t.Fatal(err)
	}
	v, err := redis.Int(cluster.Do("GET", key))
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(v, value) {
		t.Errorf("%s=%+v, want %+v", key, v, value)
	}
	_, err = redis.Int(cluster.Do("DEL", key))
	if err != nil {
		t.Error(err)
	}
	if _, err := cluster.Do("Set"); err == nil {
		t.Errorf("cluster do no key, want key is cannot found.")
	}
}

func TestBadStartNode(t *testing.T) {
	if !redis.IsClusterEnable() {
		return
	}

	_, err := redis.NewCluster("tcp", []string{"aaaa:80"})
	if err == nil {
		t.Errorf("bad start node: err = nil, want err")
	}

	//no cluster server
	_, err = redis.NewCluster("tcp", []string{redis.DefaultServerAddr()})
	if err == nil {
		t.Errorf("bad start node: err = nil, want err")
	}
}

func TestSlots(t *testing.T) {
	if !redis.IsClusterEnable() {
		return
	}

	defer func() {
		if err := recover(); err != nil {
			t.Errorf("TestSlots failed: %v\n, %v", err, string(debug.Stack()))
		}
	}()

	cluster := newCluster(t)
	defer cluster.Close()

	//test ASK command
	key := "foo1"
	slot := redis.HashSlot(key)
	if err := redis.MigrateSlot(slot, false); err != nil {
		t.Fatal(err)
	}
	doSlot(cluster, t, key, 1)

	//test MOVED command
	key = "foo2"
	slot = redis.HashSlot(key)
	if err := redis.MigrateSlot(slot, true); err != nil {
		t.Fatal(err)
	}
	//wait migrate complete
	time.Sleep(time.Second * 1)
	doSlot(cluster, t, key, 2)
}

func TestClose(t *testing.T) {
	if !redis.IsClusterEnable() {
		return
	}

	cluster := newCluster(t)
	cluster.Close()

	_, err := cluster.Do("Set", "foo1", 1)
	if err == nil {
		t.Errorf("cluster is closed: err == nil, want err != nil")
	}
}

func TestMultiKey(t *testing.T) {
	if !redis.IsClusterEnable() {
		return
	}

	cluster := newCluster(t)
	defer cluster.Close()

	_, err := cluster.Do("MGET", "foo1", "foo2")
	if err == nil {
		t.Errorf("call MGET: err == nil, want err CROSSSLOT Keys in request don't hash to the same slot")
	}
}

var testHashSlot = []struct {
	key     string
	hashKey string
}{
	{
		"{user1000}.following",
		"user1000",
	},
	{
		"foo{}{bar}",
		"foo{}{bar}",
	},
	{
		"foo{{bar}}",
		"{bar",
	},
	{
		"foo{bar}{zap}",
		"bar",
	},
	{
		"{}bar",
		"{}bar",
	},
	{
		"foo{bar",
		"foo{bar",
	},
}

func TestHashSlot(t *testing.T) {
	for _, tt := range testHashSlot {
		if redis.HashSlot(tt.key) != redis.HashSlot(tt.hashKey) {
			t.Fatalf("hash slot %v want : %v ", tt.key, tt.hashKey)
		}
	}
}
