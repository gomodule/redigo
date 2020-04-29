module github.com/gomodule/redigo/redisx

go 1.14

require github.com/gomodule/redigo/redis v1.7.1

// This is needed if you want to use a locally modified version of the github.com/gomodule/redigo/redis package
// replace github.com/gomodule/redigo/redis => ../redis
