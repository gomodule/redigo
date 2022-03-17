module github.com/gomodule/redigo/redis

go 1.16

require github.com/stretchr/testify v1.7.0

retract [v0.0.0-0, v0.0.1] // Never used only present to fix pkg.go.dev.
