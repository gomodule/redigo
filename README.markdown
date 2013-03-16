Redigo
======

Redigo is a [Go](http://golang.org/) client for the [Redis](http://redis.io/) database.

Features
-------

* A [Print-like](http://godoc.org/github.com/garyburd/redigo/redis#hdr-Executing_Commands) API with support for all Redis commands.
* [Pipelining](http://godoc.org/github.com/garyburd/redigo/redis#hdr-Pipelining), including pipelined transactions.
* [Publish/Subscribe](http://godoc.org/github.com/garyburd/redigo/redis#hdr-Publish_and_Subscribe).
* [Connection pooling](http://godoc.org/github.com/garyburd/redigo/redis#Pool).
* [Script helper type](http://godoc.org/github.com/garyburd/redigo/redis#Script) with optimistic use of EVALSHA.
* [Helper functions](http://godoc.org/github.com/garyburd/redigo/redis#hdr-Reply_Helpers) for working with command replies.

Documentation
-------------

The Redigo API reference is available on
[godoc.org](http://godoc.org/github.com/garyburd/redigo/redis).  (The godoc.org
server uses Redigo and Redis for storage).

Installation
------------

Install Redigo using the "go get" command:

    go get github.com/garyburd/redigo/redis

The Go distribution is Redigo's only dependency.

License
-------

Redigo is available under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
