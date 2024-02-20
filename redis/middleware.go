package redis

import (
	"context"
	"io"
)

type Cloner interface {
	Clone() interface{}
}

// DoContexter interface is implemented by types which process a DoContext request.
type DoContexter interface {
	DoContext(ctx context.Context, cmd string, args ...interface{}) (interface{}, error)
}

// DoContextFunc is an adapter to allow the use of ordinary functions as DoContext handlers.
type DoContexterFunc func(ctx context.Context, cmd string, args ...interface{}) (interface{}, error)

// DoContext calls f(ctx, cmd, args...).
func (f DoContexterFunc) DoContext(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	return f(ctx, cmd, args...)
}

// TODO: fix description.
// DoContextMiddleware is a function that wraps a DoContexter to provide additional functionality.
type DoContextHandler interface {
	DoContextHandler(DoContexter) DoContexter
}

// CloserFunc is an adapter to allow the use of ordinary functions as io.Closer handlers.
type CloserFunc func() error

// Close calls f().
func (f CloserFunc) Close() error {
	return f()
}

// TODO: fix description.
// CloserMiddleware is a function that wraps a io.Closer to provide additional functionality.
type CloseHandler interface {
	CloseHandler(io.Closer) io.Closer
}

// Sender interface is implemented by types which process a Send request.
type Sender interface {
	Send(commandName string, args ...interface{}) error
}

// SenderFunc is an adapter to allow the use of ordinary functions as Sender handlers.
type SenderFunc func(commandName string, args ...interface{}) error

// Send calls f(commandName, args...).
func (f SenderFunc) Send(commandName string, args ...interface{}) error {
	return f(commandName, args...)
}

// TODO: fix description.
// SenderMiddleware is a function that wraps a Sender to provide additional functionality.
type SendHandler interface {
	SendHandler(Sender) Sender
}

// ErrorReporter interface is implemented by types which process a Err request.
type ErrorReporter interface {
	Err() error
}

// ErrorReporterFunc is an adapter to allow the use of ordinary functions as Errer handlers.
type ErrorReporterFunc func() error

// Err calls f().
func (f ErrorReporterFunc) Err() error {
	return f()
}

// TODO: fix description.
// ErrMiddleware is a function that wraps a Errer to provide additional functionality.
type ErrHandler interface {
	ErrHandler(ErrorReporter) ErrorReporter
}
