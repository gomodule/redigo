package redismock

import (
	"reflect"
)

// a general expectation
type expectation interface {
	fulfilled() bool
	setError(err error)
	error() error
	setConnectionErr(err error)
	connectionError() error
}

type commonExpectation struct {
	triggered     bool
	err           error
	connectionErr error
}

func (e *commonExpectation) fulfilled() bool {
	return e.triggered
}

func (e *commonExpectation) setError(err error) {
	e.err = err
}

func (e *commonExpectation) error() error {
	return e.err
}

func (e *commonExpectation) setConnectionErr(err error) {
	e.connectionErr = err
}

func (e *commonExpectation) connectionError() error {
	return e.connectionErr
}

type commandExpecter interface {
	expectation
	command() string
	commandMatches(string) bool
}

// an expectation that can accept a command
type commandExpectation struct {
	cmd string
}

func (c *commandExpectation) command() string {
	return c.cmd
}

func (c *commandExpectation) commandMatches(cmd string) bool {
	return c.cmd == cmd
}

type argsExpecter interface {
	expectation
	setArgs([]interface{})
	args() []interface{}
	argsMatches([]interface{}) bool
}

// any expectation that can accept args
type argsExpectation struct {
	a []interface{}
}

func (a *argsExpectation) setArgs(args []interface{}) {
	a.a = args
}

func (a *argsExpectation) args() []interface{} {
	return a.a
}

func (a *argsExpectation) argsMatches(args []interface{}) bool {
	if a.a == nil {
		return true
	}
	if len(a.a) != len(args) {
		return false
	}
	for k, v := range args {
		val := reflect.ValueOf(v)
		exp := reflect.ValueOf(a.a[k])
		switch val.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return val.Int() == exp.Int()
		case reflect.Float32, reflect.Float64:
			return val.Float() == exp.Float()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return val.Uint() == exp.Uint()
		case reflect.String:
			return val.String() == exp.String()
		default:
			// Others compare by type only
			return val.Kind() == exp.Kind()
		}
	}
	return true
}

type replyExpecter interface {
	expectation
	setReply(interface{})
	reply() interface{}
}

// any expecation that can return a reply
type replyExpectation struct {
	rep interface{}
}

func (r *replyExpectation) setReply(reply interface{}) {
	r.rep = reply
}

func (r *replyExpectation) reply() interface{} {
	return r.rep
}

type doExpectation struct {
	commonExpectation
	commandExpectation
	argsExpectation
	replyExpectation
}

type sendExpectation struct {
	commonExpectation
	commandExpectation
	argsExpectation
}

type flushExpectation struct {
	commonExpectation
}

type receiveExpectation struct {
	commonExpectation
	replyExpectation
}
