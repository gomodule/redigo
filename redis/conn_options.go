package redis

import (
	"io"
)

// ConnOption is a function that configures a PoolConn.
type ConnOption func(*Conn)

// Use sets the middlewares for the PoolConn.
func ConnUse(middlewares ...interface{}) ConnOption {
	return func(c *Conn) {
		var doContextHandler DoContexter
		var closeHandler io.Closer
		var sendHandler Sender
		var errHandler ErrorReporter
		for i := len(middlewares) - 1; i >= 0; i-- {
			m := middlewares[i]
			if c, ok := m.(Cloner); ok {
				m = c.Clone()
			}
			if m, ok := m.(CloseHandler); ok {
				if closeHandler == nil {
					closeHandler = m.CloseHandler(CloserFunc(c.close))
				} else {
					closeHandler = m.CloseHandler(closeHandler)
				}
			}

			if m, ok := m.(DoContextHandler); ok {
				if doContextHandler == nil {
					doContextHandler = m.DoContextHandler(DoContexterFunc(c.doContext))
				} else {
					doContextHandler = m.DoContextHandler(doContextHandler)
				}
			}

			if m, ok := m.(ErrHandler); ok {
				if errHandler == nil {
					errHandler = m.ErrHandler(ErrorReporterFunc(c.error))
				} else {
					errHandler = m.ErrHandler(errHandler)
				}
			}

			if m, ok := m.(SendHandler); ok {
				if sendHandler == nil {
					sendHandler = m.SendHandler(SenderFunc(c.send))
				} else {
					sendHandler = m.SendHandler(sendHandler)
				}
			}

		}

		if closeHandler != nil {
			c.close = closeHandler.Close
		}

		if errHandler != nil {
			c.error = errHandler.Err
		}

		if doContextHandler != nil {
			c.doContext = doContextHandler.DoContext
		}

		if sendHandler != nil {
			c.send = sendHandler.Send
		}
	}
}
