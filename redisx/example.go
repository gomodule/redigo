// Copyright 2012 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

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
