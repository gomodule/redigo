//go:build go1.21 && !redis_legacy_watch

package redis

import "context"

func watchCommandContext(ctx context.Context, cancelIO func()) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); cancelIO() })
	return func() {
		if !stop() {
			<-done
		}
	}
}
