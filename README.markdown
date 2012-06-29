Redigo
======

Redigo is a [Go](http://golang.org/) client for the [Redis](http://redis.io/) database.

Features
-------

* An [fmt.Print-like](http://go.pkgdoc.org/github.com/garyburd/redigo/redis#Executing_Commands) API with support for all Redis commands.
* [Pipelining](http://go.pkgdoc.org/github.com/garyburd/redigo/redis#Pipelining), including pipelined transactions.
* [Publish/Subscribe](http://go.pkgdoc.org/github.com/garyburd/redigo/redis#Publish_and_Subscribe).
* [Connection pooling](http://go.pkgdoc.org/github.com/garyburd/redigo/redis#Pool).
* [Script helper object](http://go.pkgdoc.org/github.com/garyburd/redigo/redis#Script) with optimistic use of EVALSHA.

Documentation
-------------

The Redigo API reference is available on [GoPkgDoc](http://gopkgdoc.appspot.com/pkg/github.com/garyburd/redigo/redis).

Installation
------------

Install Redigo using the "go get" command:

    go get github.com/garyburd/redigo/redis

The Go distribution is Redigo's only dependency.

License
-------

Redigo is available under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
