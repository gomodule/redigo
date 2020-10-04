package redisx

import (
	"context"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestCacherTracking(t *testing.T) {
	c, cleanup := setupCacher(t)
	defer cleanup()

	conn := c.Get(nil)

	_, err := conn.Do("SET", "k", "v")
	assert.Nil(t, err)

	_, err = conn.Do("GET", "k")
	assert.Nil(t, err)

	value, err := redis.String(conn.Do("GET", "k"))
	assert.Equal(t, "v", value) // Make sure stored value is correct
	assert.Nil(t, err)

	// Should be in cache now
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 1, c.Stats().Entries)

	_, err = conn.Do("SET", "k", "v2")
	assert.Nil(t, err)

	// Value changed. Cache is no longer valid
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 0, c.Stats().Entries)
}

func TestCacherWithoutInvalidationProcess(t *testing.T) {
	c, close := setupCacher(t)
	close()

	assert.Panics(t, func() {
		c.Get(nil)
	}, "should panic")
}

func TestCacherNilGetter(t *testing.T) {
	c := &Cacher{
		Getter: nil,
	}

	assert.Panics(t, func() {
		c.Get(nil)
	})
}

func setupCacher(t *testing.T) (c *Cacher, cleanup func()) {
	getter := ConnGetterFunc(func() redis.Conn {
		conn, err := redis.Dial("tcp", ":6379")
		assert.Nil(t, err)

		return conn
	})

	c = &Cacher{
		Getter:   getter,
		Strategy: Tracking,
	}

	ctx, cleanup := context.WithCancel(context.Background())

	setupDone := make(chan struct{})
	go func() {
		t.Log("Starting invalidator")
		err := c.Run(ctx, setupDone)
		if err != context.Canceled {
			t.Errorf("unexpected error: %v", err)
			panic(123)
		}
	}()
	<-setupDone

	return
}

func TestPrefixMatcher(t *testing.T) {
	m := NewPrefixMatcher([]string{"user:", "object:"})
	assert.True(t, m.Match("user:123"))
	assert.True(t, m.Match("user:"))
	assert.True(t, m.Match("object:blah"))
	assert.False(t, m.Match("admin:666"))
}
