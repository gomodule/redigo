package redismock

import (
	"reflect"
)

type Any struct{}

var anyType = reflect.ValueOf(Any{}).Type()
