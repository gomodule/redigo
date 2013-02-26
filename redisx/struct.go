// Copyright 2012 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redisx

import (
	"errors"
	"reflect"
	"strconv"
)

// ScanStruct is deprecated. Use redis.ScanStruct instead.
//
// ScanStruct scans a reply containing alternating names and values to a
// struct. The HGETALL and CONFIG GET commands return replies in this format.
//
// ScanStruct uses the struct field name to match values in the response. Use
// 'redis' field tag to override the name:
//
//      Field int `redis:"myName"`
//
// Fields with the tag redis:"-" are ignored.
func ScanStruct(reply interface{}, dst interface{}) error {
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return errors.New("redigo: ScanStruct value must be non-nil pointer")
	}
	v = v.Elem()
	ss := structSpecForType(v.Type())

	p, ok := reply.([]interface{})
	if !ok {
		return errors.New("redigo: ScanStruct expectes multibulk reply")
	}
	if len(p)%2 != 0 {
		return errors.New("redigo: ScanStruct expects even number of values in reply")
	}

	for i := 0; i < len(p); i += 2 {
		name, ok := p[i].([]byte)
		if !ok {
			return errors.New("redigo: ScanStruct key not a bulk value")
		}
		value, ok := p[i+1].([]byte)
		if !ok {
			return errors.New("redigo: ScanStruct value not a bulk value")
		}
		fs := ss.fieldSpec(name)
		if fs == nil {
			continue
		}
		fv := v.FieldByIndex(fs.index)
		switch fv.Type().Kind() {
		case reflect.String:
			fv.SetString(string(value))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			x, err := strconv.ParseInt(string(value), 10, fv.Type().Bits())
			if err != nil {
				return err
			}
			fv.SetInt(x)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			x, err := strconv.ParseUint(string(value), 10, fv.Type().Bits())
			if err != nil {
				return err
			}
			fv.SetUint(x)
		case reflect.Float32, reflect.Float64:
			x, err := strconv.ParseFloat(string(value), fv.Type().Bits())
			if err != nil {
				return err
			}
			fv.SetFloat(x)
		case reflect.Bool:
			x := len(value) != 0 && (len(value) != 1 || value[0] != '0')
			fv.SetBool(x)
		case reflect.Slice:
			if fv.Type().Elem().Kind() != reflect.Uint8 {
				// TODO: check field types in structSpec
				panic("redigo: unsuported type for field " + string(name))
			}
			fv.SetBytes(value)
		default:
			// TODO: check field types in structSpec
			panic("redigo: unsuported type for field " + string(name))
		}
	}
	return nil
}

// AppendStruct is deprecated. Use redis.Args{}.AddFlat() instead.
func AppendStruct(args []interface{}, src interface{}) []interface{} {
	v := reflect.ValueOf(src)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			panic("redigo: FormatStruct argument must not be nil")
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		panic("redigo: FormatStruct argument must be a struct or pointer to a struct")
	}
	ss := structSpecForType(v.Type())
	for _, fs := range ss.l {
		fv := v.FieldByIndex(fs.index)
		args = append(args, fs.name, fv.Interface())
	}
	return args
}
