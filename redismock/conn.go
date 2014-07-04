package redismock

import (
	"fmt"
)

// Mock describes a mock and the actions
type Mock interface {
	WithArgs(...interface{}) Mock
	WillReturnError(error) Mock
	WillReturnConnectionError(error) Mock
	WillReturnReply(interface{}) Mock
}

// Conn is a mock redis.Conn to be used in unit tests
type Conn struct {
	// Expectations on this conn
	expectations []expectation
	active       expectation
}

// New returns a new redis.Conn mock
func New() *Conn {
	return &Conn{
		expectations: []expectation{},
	}
}

// will return the next unfulfilled expectation
func (c *Conn) next() expectation {
	for _, e := range c.expectations {
		if !e.fulfilled() {
			return e
		}
	}
	return nil
}

// Close will return an error if there are remaining unfulfilled expectations
//
// When writing unit tests, make sure to always close the Conns and check for errors.
func (c *Conn) Close() (err error) {
	if e := c.next(); e != nil {
		err = fmt.Errorf("there is an unfulfilled expectation %T", e)
	}
	c.expectations = []expectation{}
	c.active = nil
	return err
}

// Err will return an error if the active expectation was set with
// Conn.WillReturnConnectionError()
func (c *Conn) Err() error {
	if c.active == nil {
		return nil
	}
	return c.active.connectionError()
}

// Do acts like a redis.Do and will return errors if expectations are not fulfilled
func (c *Conn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	exp := c.next()
	if exp == nil {
		return nil, fmt.Errorf("all expectations were fulfilled. got extra Do command: %s, with args %+v", commandName, args)
	}
	e, ok := exp.(*doExpectation)
	if !ok {
		return nil, fmt.Errorf("calling Do with command %s and args %+v was not expected. expecting %T with %+v", commandName, args, exp, exp)
	}
	if !e.commandMatches(commandName) {
		return nil, fmt.Errorf("called Do with command %s and args %+v, but expected %s in %+v", commandName, args, e.command(), e)
	}
	if !e.argsMatches(args) {
		return nil, fmt.Errorf("called Do with command %s and args %+v. did not match expected args %+v", commandName, args, e.args())
	}
	e.triggered = true
	return e.reply(), e.error()
}

// Send acts like a redis.Send() and will return errors if expectations are not fulfilled
func (c *Conn) Send(commandName string, args ...interface{}) error {
	exp := c.next()
	if exp == nil {
		return fmt.Errorf("all expectations were fulfilled. got extra Send command: %s, with args %+v", commandName, args)
	}
	e, ok := exp.(*sendExpectation)
	if !ok {
		return fmt.Errorf("calling Send with command %s and args %+v was not expected. expecting %T with %+v", commandName, args, exp, exp)
	}
	if !e.commandMatches(commandName) {
		return fmt.Errorf("called Send with command %s and args %+v, but expected %s in %+v", commandName, args, e.command(), e)
	}
	if !e.argsMatches(args) {
		return fmt.Errorf("called Send with command %s and args %+v. did not match expected args %+v", commandName, args, e.args())
	}
	e.triggered = true
	return e.error()
}

// Flush acts like a redis.Flush() and will return errors if expectations are not fulfilled
func (c *Conn) Flush() error {
	exp := c.next()
	if exp == nil {
		return fmt.Errorf("all expectations were fulfilled. got extra Flush command")
	}
	e, ok := exp.(*flushExpectation)
	if !ok {
		return fmt.Errorf("calling Flush was not expected. expecting %T with %+v", exp, exp)
	}
	e.triggered = true
	return e.error()
}

// Receive acts like a redis.Receive() and will return errors if expectations are not fulfilled
func (c *Conn) Receive() (reply interface{}, err error) {
	exp := c.next()
	if exp == nil {
		return nil, fmt.Errorf("all expectations were fulfilled. got extra Receive command")
	}
	e, ok := exp.(*receiveExpectation)
	if !ok {
		return nil, fmt.Errorf("calling Receive was not expected. expecting %T with %+v", exp, exp)
	}
	e.triggered = true
	return e.reply(), e.error()
}

// ExpectDo tells the mock to expect a Do() with a given command next
//
// You can chain WithArgs(), WillReturnError(), WillReturnConnectionError() and
// WillReturnReply() on a Do expectation
func (c *Conn) ExpectDo(command string) Mock {
	exp := &doExpectation{}
	exp.cmd = command
	c.expectations = append(c.expectations, exp)
	c.active = exp
	return c
}

// ExpectSend tells the mock to expect a Send() with a given command next
//
// You can chain WithArgs(), WillReturnError(), WillReturnConnectionError() on a Send expectation
func (c *Conn) ExpectSend(command string) Mock {
	exp := &sendExpectation{}
	exp.cmd = command
	c.expectations = append(c.expectations, exp)
	c.active = exp
	return c
}

// ExpectReceive tells the mock to expect a Receive() next
//
// You can chain WillReturnError(), WillReturnConnectionError() and
// WillReturnReply() on a Receive expectation
func (c *Conn) ExpectReceive() Mock {
	exp := &receiveExpectation{}
	c.expectations = append(c.expectations, exp)
	c.active = exp
	return c
}

// ExpectFlush tells the mock to expect a Flush()
//
// The Flush() call will return an error if WillReturnError() is set.
func (c *Conn) ExpectFlush() Mock {
	exp := &flushExpectation{}
	c.expectations = append(c.expectations, exp)
	c.active = exp
	return c
}

// WithArgs will expect args
func (c *Conn) WithArgs(args ...interface{}) Mock {
	if c.active == nil {
		panic("no active expectation")
	}
	if exp, ok := c.active.(argsExpecter); !ok {
		panic("current expectation does not support args")
	} else {
		exp.setArgs(args)
	}
	return c
}

// WillReturnError will return an error when the current expectation is executed
func (c *Conn) WillReturnError(err error) Mock {
	if c.active == nil {
		panic("no active expectation")
	}
	c.active.setError(err)
	return c
}

// WillReturnConnectionError sets up the Conn to return an error on
// Conn.Err()
func (c *Conn) WillReturnConnectionError(err error) Mock {
	if c.active == nil {
		panic("no active expectation")
	}
	c.active.setConnectionErr(err)
	return c
}

// WillReturnReply will return the given reply when the current expectation is executed
func (c *Conn) WillReturnReply(reply interface{}) Mock {
	if c.active == nil {
		panic("no active expectation")
	}
	if exp, ok := c.active.(replyExpecter); !ok {
		panic("current expectation does not support a reply")
	} else {
		exp.setReply(reply)
	}
	return c
}
