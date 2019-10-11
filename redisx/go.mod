module github.com/gomodule/redigo/redisx/v3

go 1.13

require github.com/gomodule/redigo/redis/v3 v3.0.0

// This is needed if you want to use a locally modified version of the github.com/gomodule/redigo/redis package
// replace github.com/gomodule/redigo/redis/v3 => ../redis
