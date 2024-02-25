module github.com/gomodule/redigo

go 1.17

require github.com/stretchr/testify v1.8.4

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract (
	v2.0.0+incompatible // Old development version not maintained or published.
	v1.8.10 // Incorrect version tag for feature.
	v0.0.0-do-not-use // Never used only present due to lack of retract.
)
