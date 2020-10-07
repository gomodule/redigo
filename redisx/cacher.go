package redisx

import (
	"context"
	"errors"
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

			eventType, _ := redis.String(event[0], nil)

			if eventType == "message" {
				// Remove revoked keys from local cache
				keys, err := redis.Strings(event[2], nil)
				if err != nil || err == redis.ErrNil {
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
	entries := 0
	c.cache.Range(func(key, value interface{}) bool {
		entries++
		return true
	})
	return CacheStats{entries}
}

// CacheStats contain statistics about local cache
type CacheStats struct {
	// Number of cached keys
	Entries int
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
	if entry, ok := w.c.cache.Load(key); ok {
		return entry, nil
	}

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
		w.c.cache.Store(key, reply)
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
