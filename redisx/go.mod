module github.com/gomodule/redigo/redisx

go 1.16

require (
	github.com/gomodule/redigo v1.8.8
	github.com/stretchr/testify v1.7.0
)

retract [v0.0.0-0, v0.0.1] // Never used only present to fix pkg.go.dev.
