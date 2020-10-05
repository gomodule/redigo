package redisx

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestCacherTrackingStrategy(t *testing.T) {
	c, cleanup := setupCacher(t, Tracking)
	defer cleanup()

	matcher := MatcherFunc(func(key string) bool {
		return key != "nocache"
	})

	cached := hasBeenCachedHelper(t, c)

	assert.True(t, cached(matcher, "key"), "key should be cached")
	assert.False(t, cached(matcher, "nocache"), "key shouldn't be cached because it didn't Match")

	assert.True(t, cached(nil, "anything"), "all keys should be cached when nil matcher is supplied")
}

func TestCacherBroadcastStrategy(t *testing.T) {
	c, cleanup := setupCacher(t, Broadcast)
	defer cleanup()

	cached := hasBeenCachedHelper(t, c)

	t.Run("should panic when Matcher is not prefixMatcher", func(t *testing.T) {
		assert.Panics(t, func() {
			cached(MatcherFunc(func(_ string) bool { return true }), "key")
		})
		assert.Panics(t, func() {
			cached(nil, "key")
		})
	})

	matcher := NewPrefixMatcher([]string{"user:", "object:"})

	assert.True(t, cached(matcher, "user:123"), "key should be in cache")
	assert.True(t, cached(matcher, "object:"), "key should be in cache")

	assert.False(t, cached(matcher, "admin:blah"), "key without needed prefix shouldn't be in cache")
}

func TestCacherWithoutInvalidationProcess(t *testing.T) {
	c, close := setupCacher(t, Tracking)
	close()
	time.Sleep(50 * time.Millisecond) // Wait for goroutine to return

	assert.Panics(t, func() {
		c.Get(nil)
	})

	assert.Panics(t, func() {
		c.Wrap(c.Get(nil), nil)
	})
}

func TestCacherNilGetter(t *testing.T) {
	c := &Cacher{
		Getter: nil,
	}

	assert.Panics(t, func() {
		c.Run(context.Background(), make(chan<- struct{}))
	})

	assert.Panics(t, func() {
		c.Get(nil)
	})
}

func setupCacher(t *testing.T, strategy cachingStrategy) (c *Cacher, cleanup func()) {
	getter := ConnGetterFunc(func() redis.Conn {
		ctx, close := context.WithTimeout(context.Background(), time.Second)
		defer close()

		conn, err := redis.DialContext(ctx, "tcp", ":6379")
		if !assert.Nil(t, err) {
			panic(fmt.Sprintf("Failed to connect to redis: %v", err))
		}

		return conn
	})

	c = &Cacher{
		Getter:   getter,
		Strategy: strategy,
	}

	ctx, cleanup := context.WithCancel(context.Background())

	setupDone := make(chan struct{})
	go func() {
		t.Log("Starting invalidator")
		err := c.Run(ctx, setupDone)
		if err != context.Canceled {
			t.Errorf("unexpected error: %v", err)
		}
	}()
	<-setupDone

	return
}

func hasBeenCachedHelper(t *testing.T, c *Cacher) func(Matcher, string) bool {
	return func(m Matcher, key string) bool {
		conn := c.Get(m)

		beforeCount := c.Stats().Entries

		_, err := conn.Do("SET", key, "value")
		assert.Nil(t, err)

		_, err = conn.Do("GET", key)
		assert.Nil(t, err)

		// Key should be cached by now
		time.Sleep(10 * time.Millisecond)
		afterCount := c.Stats().Entries

		// Make sure stored value is correct
		value, err := redis.String(conn.Do("GET", key))
		assert.Equal(t, "value", value)
		assert.Nil(t, err)

		// Invalidate cache
		_, err = conn.Do("SET", key, "another value")
		assert.Nil(t, err)

		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, beforeCount, c.Stats().Entries)

		return afterCount == beforeCount+1
	}
}

func TestPrefixMatcher(t *testing.T) {
	m := NewPrefixMatcher([]string{"user:", "object:"})
	assert.True(t, m.Match("user:123"))
	assert.True(t, m.Match("user:"))
	assert.True(t, m.Match("object:blah"))
	assert.False(t, m.Match("admin:666"))
}
