// +build ignore 

package main

import (
	"github.com/garyburd/redigo/redis"
	"github.com/garyburd/redigo/redisx"
	"log"
)

type MyStruct struct {
	A int
	B string
}

func main() {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		log.Fatal(err)
	}

	v0 := &MyStruct{1, "hello"}

	_, err = c.Do("HMSET", redisx.AppendStruct([]interface{}{"key"}, v0)...)
	if err != nil {
		log.Fatal(err)
	}

	reply, err := c.Do("HGETALL", "key")
	if err != nil {
		log.Fatal(err)
	}

	v1 := &MyStruct{}

	err = redisx.ScanStruct(reply, v1)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("v1=%v", v1)
}
