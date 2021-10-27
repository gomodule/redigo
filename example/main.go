package main

import (
	"encoding/json"
	"fmt"

	"github.com/gomodule/redigo/redis"
)

func main() {
	host := "127.0.0.1" // redis host
	port := "6379"      // redis port

	// set value
	jsonValue := make(map[string]interface{})
	jsonValue["test1"] = "val1"
	jsonValue["test2"] = "val2"
	byteValue, _ := json.Marshal(jsonValue)

	expireSecond := 10 // after 10 second this value removed from redis if set this variable as 0 this value not removed from redis
	SetValue("myKey", string(byteValue), host, port, expireSecond)

	// get value
	myVal := GetValue("myKey", host, port)
	if myVal != "" {
		jsonValue := make(map[string]interface{})
		json.Unmarshal([]byte(myVal), &jsonValue)
		fmt.Println(jsonValue)
	}

	// get all keys in redis
	keysStartedWith := "" // empty string return all keys if you want to get all keys started with hello set this variable with hello value
	keys := GetKeys(keysStartedWith, host, port)
	fmt.Println(keys)
}

func SetValue(key string, data string, host string, port string, expire int) {
	redis_connection_string := fmt.Sprintf("%s:%s", host, port)
	conn, err := redis.Dial("tcp", redis_connection_string)
	if err != nil {
		// fmt.Println("Redis Connection Error", key, err) // handle error
		return
	}
	defer conn.Close()

	_, err = conn.Do(
		"HMSET",
		key,
		"data",
		data,
	)
	if expire > 0 {
		conn.Do("EXPIRE", key, expire)
	}
}

func GetValue(key string, host string, port string) string {
	redis_connection_string := fmt.Sprintf("%s:%s", host, port)
	conn, err := redis.Dial("tcp", redis_connection_string)
	if err != nil {
		// fmt.Println("Redis Connection Error", err) // handle error
		return ""
	}
	defer conn.Close()

	data, err := redis.String(conn.Do("HGET", key, "data"))
	if err != nil {
		// fmt.Println("Redis Connection Error", err) // handle error
		return ""
	}
	return data
}

func GetKeys(key string, host string, port string) []string {
	redis_connection_string := fmt.Sprintf("%s:%s", host, port)
	conn, err := redis.Dial("tcp", redis_connection_string)
	if err != nil {
		fmt.Println("Redis Connection Error") // handle error
		return []string{}
	}
	defer conn.Close()

	data, _ := redis.Strings(conn.Do("KEYS", key))
	return data
}
