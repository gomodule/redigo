package redisx

import (
	"context"
	"strings"
	"sync"

	"github.com/gomodule/redigo/redis"
)

type cachingStrategy int

const (
	// Tracking strategy lets server to keep track of keys request by client
	Tracking cachingStrategy = iota
	// Broadcast strategy allows client to manually subscribe
	// to invalidation broadcasts for interested keys
	Broadcast
)

// Cacher implements client-side caching first appeared in Redis 6.0
type Cacher struct {
	Getter   ConnGetter
	Strategy cachingStrategy

	cache sync.Map // Actual cache. Not yet replaceble

	mu  sync.Mutex // Guard for fields below
	cid int        // Connection ID of invalidator process
}

// Matcher interface is used to determine whether key should be cached.
// Allows for custom caching logic.
type Matcher interface {
	Match(key string) bool
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

// Run starts cache invalidation process. Must be called before any other
// method if Tracking strategy is used. Closes provided channel when setup is done
// Blocks caller.
func (c *Cacher) Run(ctx context.Context, setupDoneChan chan<- struct{}) error {
	// Connection used for invalidation
	conn := c.Getter.Get()

	id, err := redis.Int(conn.Do("CLIENT", "ID"))
	if err != nil {
		close(setupDoneChan)
		return err
	}

	c.mu.Lock()
	c.cid = id
	c.mu.Unlock()

	// Subscribe to revocation channel
	err = conn.Send("SUBSCRIBE", "__redis__:invalidate")
	if err != nil {
		close(setupDoneChan)
		return err
	}

	conn.Flush()
	if err != nil {
		close(setupDoneChan)
		return err
	}

	close(setupDoneChan)

outer:
	for {
		select {
		case <-ctx.Done():
			break outer
		default:
			event, err := redis.Values(conn.Receive())
			if err != nil {
				continue
			}

			evType, _ := redis.String(event[0], nil)

			if evType == "message" {
				// Remove revoked keys from local cache
				keys, err := redis.Strings(event[2], nil)
				if err != nil {
					continue
				}

				for _, key := range keys {
					c.cache.Delete(key)
				}
			}
		}
	}

	c.mu.Lock()
	c.cid = 0
	c.mu.Unlock()

	return ctx.Err()
}

// Get returns a connection with caching enabled for matched keys.
// If nil Matcher is passed all keys are subjected to cache.
// Caller is responsible for closing connection.
func (c *Cacher) Get(m Matcher) redis.Conn {
	if c.Getter == nil {
		panic("redisx cacher: cannot Get: no getter supplied")
	}
	return c.Wrap(c.Getter.Get(), m)
}

// Wrap allows wrapping existing connection with caching layer
func (c *Cacher) Wrap(conn redis.Conn, m Matcher) redis.Conn {
	c.mu.Lock()
	cid := c.cid
	c.mu.Unlock()

	if cid == 0 {
		panic("redisx cacher: invalidation process is not running")
	}

	_, err := conn.Do("CLIENT", "TRACKING", "on", "REDIRECT", cid)
	if err != nil {
		return errorConn{err}
	}

	return wrappedConn{c, conn, m}
}

// Stats return aggregated stats about local cache
func (c *Cacher) Stats() CacheStats {
	entries := 0
	c.cache.Range(func(key, value interface{}) bool {
		entries++
		return true
	})
	return CacheStats{entries}
}

// CacheStats contain statistics about local cache
type CacheStats struct {
	Entries int
}

type wrappedConn struct {
	c       *Cacher
	conn    redis.Conn
	matcher Matcher
}

func (w wrappedConn) Close() error { return w.conn.Close() }

func (w wrappedConn) Err() error { return w.conn.Err() }

func (w wrappedConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	// Only GET is supported for now
	if commandName == "GET" {
		key, ok := args[0].(string)

		if !ok {
			return w.conn.Do(commandName, args...)
		}

		if w.matcher != nil && !w.matcher.Match(key) {
			return w.conn.Do(commandName, args...)
		}

		// Happy path
		if entry, ok := w.c.cache.Load(key); ok {
			return entry, nil
		}

		reply, err := w.conn.Do(commandName, args...)
		if err != nil {
			return reply, err
		}

		// Cache response
		w.c.cache.Store(key, reply)
		return reply, nil
	}

	return w.conn.Do(commandName, args...)
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
