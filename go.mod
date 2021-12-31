module github.com/gomodule/redigo

go 1.16

require github.com/stretchr/testify v1.7.0

retract (
	v2.0.0+incompatible // Old development version not maintained or published.
	v0.0.0-do-not-use // Never used only present due to lack of retract.
)
