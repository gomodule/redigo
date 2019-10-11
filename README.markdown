Redigo
======

[![Build Status](https://travis-ci.org/gomodule/redigo.svg?branch=master)](https://travis-ci.org/gomodule/redigo)
[![GoDoc](https://godoc.org/github.com/gomodule/redigo/redis?status.svg)](https://pkg.go.dev/github.com/gomodule/redigo/redis/v3)

Redigo is a [Go](http://golang.org/) client for the [Redis](http://redis.io/) database.

Features
-------

* A [Print-like](http://godoc.org/github.com/gomodule/redigo/redis#hdr-Executing_Commands) API with support for all Redis commands.
* [Pipelining](http://godoc.org/github.com/gomodule/redigo/redis#hdr-Pipelining), including pipelined transactions.
* [Publish/Subscribe](http://godoc.org/github.com/gomodule/redigo/redis#hdr-Publish_and_Subscribe).
* [Connection pooling](http://godoc.org/github.com/gomodule/redigo/redis#Pool).
* [Script helper type](http://godoc.org/github.com/gomodule/redigo/redis#Script) with optimistic use of EVALSHA.
* [Helper functions](http://godoc.org/github.com/gomodule/redigo/redis#hdr-Reply_Helpers) for working with command replies.

Documentation
-------------

- [API Reference](http://godoc.org/github.com/gomodule/redigo/redis)
- [FAQ](https://github.com/gomodule/redigo/wiki/FAQ)
- [Examples](https://godoc.org/github.com/gomodule/redigo/redis#pkg-examples)

Installation
------------

Install Redigo using the "go get" command:

    go get github.com/gomodule/redigo/redis/v3

The Go distribution is Redigo's only dependency.

Updating
--------

If you you were using `redigo` prior to the `v3` tag and are using `go mod` you'll need to update your import paths to add the `/v3` suffix as detailed by [Installation](#installation). A community tool [github.com/marwan-at-work/mod](https://github.com/marwan-at-work/mod) helps automate this procedure. See the [repository](https://github.com/marwan-at-work/mod) or the [community tooling FAQ](https://github.com/golang/go/wiki/Modules#what-community-tooling-exists-for-working-with-modules) below for an overview.

Related Projects
----------------

- [rafaeljusto/redigomock](https://godoc.org/github.com/rafaeljusto/redigomock) - A mock library for Redigo.
- [chasex/redis-go-cluster](https://github.com/chasex/redis-go-cluster) - A Redis cluster client implementation.
- [FZambia/sentinel](https://github.com/FZambia/sentinel) - Redis Sentinel support for Redigo
- [mna/redisc](https://github.com/mna/redisc) - Redis Cluster client built on top of Redigo

Contributing
------------

See [CONTRIBUTING.md](https://github.com/gomodule/redigo/blob/master/.github/CONTRIBUTING.md).

License
-------

Redigo is available under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
