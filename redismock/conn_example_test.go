package redismock

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
)

func ExampleConn_Do() {
	// redismock.New()
	c := New()

	// We can return an error
	c.ExpectDo("PING").
		WillReturnError(fmt.Errorf("cannot ping"))
	// Yes, we implement redis.Conn
	var conn redis.Conn
	conn = c
	_, err := conn.Do("PING")
	if err != nil {
		// will print the error "cannot ping"
		fmt.Println(err)
	}

	// reset
	err = c.Close()
	if err != nil {
		fmt.Print(err)
		return
	}

	// SET command
	c.ExpectDo("SET").
		WithArgs("key", Any{}, "NX"). // Args with redismock.Any{} accepts any value
		WillReturnReply("OK")
	reply, err := conn.Do("SET", "key", "value", "NX")
	if err != nil {
		fmt.Print(err)
		return
	}
	// print reply "OK"
	fmt.Println(reply)

	// reset again
	err = c.Close()
	if err != nil {
		fmt.Print(err)
		return
	}

	c.ExpectDo("SET")
	_, err = conn.Do("EXISTS", "A")
	if err != nil {
		// called another command, will fail
		fmt.Println("error on command")
	}

	// reset again
	err = c.Close()
	if err != nil {
		// the conn.Do is unfulfilled
		// we will receive the error "there is an unfulfilled expectation *redismock.doExpectation"
		fmt.Println(err)
	}

	c.ExpectDo("SET").WithArgs("arg")
	_, err = conn.Do("SET", "other")
	if err != nil {
		// called with another arg, will fail
		fmt.Println("error on other arg")
	}

	// Output:
	// cannot ping
	// OK
	// error on command
	// there is an unfulfilled expectation *redismock.doExpectation
	// error on other arg
}

func ExampleConn_Receive() {
	// redismock.New()
	c := New()

	c.ExpectSend("EXISTS").WithArgs("A")
	c.ExpectFlush()
	c.ExpectReceive().WillReturnReply("1")

	// check if it implements redis.Conn
	var conn redis.Conn
	conn = c

	err := conn.Send("EXISTS", "A")
	if err != nil {
		fmt.Println(err)
		return
	}
	err = conn.Flush()
	if err != nil {
		fmt.Println(err)
		return
	}
	ok, err := conn.Receive()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(ok)
	// Output:
	// 1
}
