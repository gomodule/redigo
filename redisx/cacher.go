package redisx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/gomodule/redigo/redisx/internal"
)

type cachingStrategy int

const (
	// Tracking strategy lets server to keep track of keys request by client
	Tracking cachingStrategy = iota

	// Broadcast strategy allows client to manually subscribe
	// to invalidation broadcasts for interested keys
	// More efficient in terms of Redis memory & cpu usage but requires
	// client to know what keys need to be cached beforehand
	// See https://redis.io/topics/client-side-caching#broadcasting-mode
	Broadcast
)

// Cacher implements client-side caching first appeared in Redis 6.0
type Cacher struct {
	Getter   ConnGetter      // Interface used for obtaining connections
	Strategy cachingStrategy // Caching strategy (see docs)
	MaxSize  int             // Max number of keys in local cache. Must be positive

	mu    sync.Mutex      // Guard for fields below
	cid   int             // Connection ID of invalidator process
	cache *internal.Cache // Actual cache
}

// Matcher interface is used to determine whether key should be cached.
// Allows for custom caching logic.
type Matcher interface {
	Match(key string) bool
}

// MatcherFunc is an adapter type for Matcher interface
type MatcherFunc func(string) bool

// Match just calls underlying function
func (f MatcherFunc) Match(key string) bool {
	return f(key)
}

type prefixMatcher []string

// NewPrefixMatcher is a helper function that matches all keys with
// supplied prefixes
func NewPrefixMatcher(prefixes []string) Matcher {
	return prefixMatcher(prefixes)
}

func (p prefixMatcher) Match(key string) bool {
	for _, prefix := range p {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}

	return false
}

// ConnGetter is interface used to get redis connection for Cacher usage
type ConnGetter interface {
	Get() redis.Conn
}

// ConnGetterFunc is an adapter type for ConnGetter interface
type ConnGetterFunc func() redis.Conn

// Get just calls underlying function
func (f ConnGetterFunc) Get() redis.Conn {
	return f()
}

var errNilGetter = errors.New("redisx cacher: getter is nil")

// Run starts cache invalidation process. Must be called before any other
// method. Closes provided channel when setup is done.
// Blocks caller.
func (c *Cacher) Run(ctx context.Context, setupDoneChan chan<- struct{}) error {
	if c.Getter == nil {
		panic(errNilGetter)
	}

	// Connection used for invalidation
	conn := c.Getter.Get()
	if conn == nil {
		close(setupDoneChan)
		return errors.New("getter returned nil connection")
	}

	id, err := redis.Int(conn.Do("CLIENT", "ID"))
	if err != nil {
		close(setupDoneChan)
		return err
	}

	c.mu.Lock()
	c.cid = id
	c.cache = internal.NewCache(c.MaxSize)
	c.mu.Unlock()

	// Subscribe to revocation channel
	if err = conn.Send("SUBSCRIBE", "__redis__:invalidate"); err != nil {
		close(setupDoneChan)
		return err
	}

	if err = conn.Flush(); err != nil {
		close(setupDoneChan)
		return err
	}

	pingConn := c.Getter.Get()
	if conn == nil {
		close(setupDoneChan)
		return errors.New("getter returned nil connection")
	}

	pingFailures := 0

	pingChan := make(chan error)
	go pinger(ctx, pingConn, pingChan)

	invalidateChan := make(chan []string)
	go invalidator(ctx, conn, invalidateChan)

	close(setupDoneChan)

outer:
	for {
		select {
		case <-ctx.Done():
			break outer

		case err := <-pingChan:
			// Check connection is still active
			// Flush local cache after number of attempts
			if err == nil {
				pingFailures = 0
				continue outer
			}

			pingFailures++

			if pingFailures > 3 {
				c.mu.Lock()
				c.cache.Clear()
				c.mu.Unlock()
			}

		case keys := <-invalidateChan:
			// Remove keys from local cache
			c.mu.Lock()
			for _, key := range keys {
				c.cache.Delete(key)
			}
			c.mu.Unlock()
		}
	}

	c.mu.Lock()
	c.cid = 0
	c.mu.Unlock()

	return ctx.Err()
}

// invalidator polls invalidate messages in separate goroutine since conn.Receive blocks
func invalidator(ctx context.Context, conn redis.Conn, invalidateChan chan<- []string) {
outer:
	for {
		select {
		case <-ctx.Done():
			break outer
		default:
			event, err := redis.Values(conn.Receive())
			if err != nil {
				continue outer
			}

			etype, _ := redis.String(event[0], nil)

			if etype != "message" {
				continue outer
			}

			// Remove revoked keys from local cache
			keys, err := redis.Strings(event[2], nil)
			if err != nil || err == redis.ErrNil {
				continue outer
			}

			invalidateChan <- keys
		}
	}

	close(invalidateChan)
}

// pinger will emit ping result every second
func pinger(ctx context.Context, conn redis.Conn, pingChan chan<- error) {
	ticker := time.NewTicker(time.Second)

outer:
	for {
		select {
		case <-ctx.Done():
			break outer
		case <-ticker.C:
			_, err := conn.Do("PING")
			if err != nil {
				fmt.Println(err)
			}
			pingChan <- err
		}
	}

	ticker.Stop()
	close(pingChan)
}

// Get returns a connection with caching enabled for matching keys.
// If nil Matcher is passed all keys are subjected to cache.
// When using Broadcast strategy Matcher should be created with NewPrefixMatcher.
// Caller is responsible for closing connection.
func (c *Cacher) Get(m Matcher) redis.Conn {
	if c.Getter == nil {
		panic(errNilGetter)
	}
	return c.Wrap(c.Getter.Get(), m)
}

var errNotRunning = errors.New("redisx cacher: invalidation process is not running")
var errPrefixMatcherExpected = errors.New("redisx cacher: NewPrefixMatcher is expected when using Broadcast strategy")

// Wrap allows wrapping existing connection with caching layer
// When using Broadcast strategy Matcher should be created with NewPrefixMatcher.
// Caller is responsible for closing connection.
func (c *Cacher) Wrap(conn redis.Conn, m Matcher) redis.Conn {
	c.mu.Lock()
	cid := c.cid
	c.mu.Unlock()

	if cid == 0 {
		panic(errNotRunning)
	}

	args := []interface{}{"TRACKING", "on", "REDIRECT", cid}

	// When using Broadcast strategy we just pass list of key prefixes
	// we are interested in.
	if c.Strategy == Broadcast {
		prefixes, ok := m.(prefixMatcher)
		if !ok {
			panic(errPrefixMatcherExpected)
		}

		args = append(args, "BCAST")
		for _, prefix := range prefixes {
			args = append(args, "PREFIX", prefix)
		}
	}

	// If matcher if supplied we want to pass OPTIN flag so we can munually
	// decide whether we want to cache certain key
	if c.Strategy == Tracking && m != nil {
		args = append(args, "OPTIN")
	}

	_, err := conn.Do("CLIENT", args...)
	if err != nil {
		return errorConn{err}
	}

	return wrappedConn{c, conn, m}
}

// Stats returns aggregated stats about local cache
func (c *Cacher) Stats() CacheStats {
	size, hits, misses := c.cache.Stats()
	return CacheStats{size, hits, misses}
}

// CacheStats contain statistics about local cache
type CacheStats struct {
	Entries int // Number of keys in the cache
	Hits    int // Number of cache hits
	Misses  int // Number of cache misses
}

type wrappedConn struct {
	c       *Cacher
	conn    redis.Conn
	matcher Matcher
}

func (w wrappedConn) Close() error { return w.conn.Close() }

func (w wrappedConn) Err() error { return w.conn.Err() }

func (w wrappedConn) Do(commandName string, args ...interface{}) (interface{}, error) {
	// Only GET is supported for now
	if commandName != "GET" {
		return w.conn.Do(commandName, args...)
	}

	key, ok := args[0].(string)
	if !ok { // Invalid command
		return w.conn.Do(commandName, args...)
	}

	// Cache is present: just return
	w.c.mu.Lock()
	if entry := w.c.cache.Get(key); entry != nil {
		w.c.mu.Unlock()
		return entry, nil
	}
	w.c.mu.Unlock()

	if w.c.Strategy == Tracking && w.matcher != nil && w.matcher.Match(key) {
		// If matcher is supplied ask redis to remember that we cached next command
		resp, err := w.conn.Do("CLIENT", "CACHING", "YES")
		if err != nil {
			return resp, err
		}
	}

	reply, err := w.conn.Do(commandName, args...)
	if err != nil {
		return reply, err
	}

	// Cache response
	if w.matcher == nil || (w.matcher != nil && w.matcher.Match(key)) {
		ttl := 600 // TODO request TTL
		w.c.mu.Lock()
		w.c.cache.Set(key, reply, ttl)
		w.c.mu.Unlock()
	}
	return reply, nil
}

func (w wrappedConn) Send(commandName string, args ...interface{}) error {
	return w.conn.Send(commandName, args...)
}

func (w wrappedConn) Flush() error { return w.conn.Flush() }

func (w wrappedConn) Receive() (reply interface{}, err error) { return w.conn.Receive() }

//
type errorConn struct{ err error }

func (ec errorConn) Do(string, ...interface{}) (interface{}, error) { return nil, ec.err }
func (ec errorConn) Send(string, ...interface{}) error              { return ec.err }
func (ec errorConn) Err() error                                     { return ec.err }
func (ec errorConn) Close() error                                   { return nil }
func (ec errorConn) Flush() error                                   { return ec.err }
func (ec errorConn) Receive() (interface{}, error)                  { return nil, ec.err }
