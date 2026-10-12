//go:build !go1.21 || redis_legacy_watch

package redis

import "context"

// Compatibility fallback for the Go 1.17 minimum. The tag permits testing this
// fallback on newer compilers as well.
func watchCommandContext(ctx context.Context, cancelIO func()) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			cancelIO()
		case <-stop:
		}
	}()
	return func() { close(stop); <-done }
}
