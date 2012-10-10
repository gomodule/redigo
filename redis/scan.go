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

package redis

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

func cannotConvert(d reflect.Value, s interface{}) error {
	return fmt.Errorf("redigo: Scan cannot convert from %s to %s",
		reflect.TypeOf(s), d.Type())
}

func convertAssignBulk(d reflect.Value, s []byte) (err error) {
	switch d.Type().Kind() {
	case reflect.Float32, reflect.Float64:
		var x float64
		x, err = strconv.ParseFloat(string(s), d.Type().Bits())
		d.SetFloat(x)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var x int64
		x, err = strconv.ParseInt(string(s), 10, d.Type().Bits())
		d.SetInt(x)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var x uint64
		x, err = strconv.ParseUint(string(s), 10, d.Type().Bits())
		d.SetUint(x)
	case reflect.Bool:
		var x bool
		x, err = strconv.ParseBool(string(s))
		d.SetBool(x)
	case reflect.String:
		d.SetString(string(s))
	case reflect.Slice:
		if d.Type().Elem().Kind() != reflect.Uint8 {
			err = cannotConvert(d, s)
		} else {
			d.SetBytes(s)
		}
	default:
		err = cannotConvert(d, s)
	}
	return
}

func convertAssignInt(d reflect.Value, s int64) (err error) {
	switch d.Type().Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		d.SetInt(s)
		if d.Int() != s {
			err = strconv.ErrRange
			d.SetInt(0)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if s < 0 {
			err = strconv.ErrRange
		} else {
			x := uint64(s)
			d.SetUint(x)
			if d.Uint() != x {
				err = strconv.ErrRange
				d.SetUint(0)
			}
		}
	case reflect.Bool:
		d.SetBool(s != 0)
	default:
		err = cannotConvert(d, s)
	}
	return
}

// Scan copies from a multi-bulk command reply to the values pointed at by
// dest. 
// 
// The values pointed at by test must be a numeric type, boolean, string or a
// byte slice. Scan uses the standard strconv package to convert bulk values to
// numeric and boolean types. 
//
// If the multi-bulk value is nil, then the corresponding dest value is not
// modified. 
//
// To enable easy use of Scan in a loop, Scan returns the slice following the
// copied values.
func Scan(multiBulk []interface{}, dest ...interface{}) ([]interface{}, error) {

	// We handle the most common dest types using type switches and fallback to
	// reflection for all other types.

	if len(multiBulk) < len(dest) {
		return nil, errors.New("redigo: Scan multibulk short")
	}
	var err error
	for i, d := range dest {
		switch s := multiBulk[i].(type) {
		case nil:
			// ingore
		case []byte:
			switch d := d.(type) {
			case *string:
				*d = string(s)
			case *int:
				*d, err = strconv.Atoi(string(s))
			case *bool:
				*d, err = strconv.ParseBool(string(s))
			case *[]byte:
				*d = s
			default:
				if d := reflect.ValueOf(d); d.Type().Kind() != reflect.Ptr {
					err = cannotConvert(d, s)
				} else {
					err = convertAssignBulk(d.Elem(), s)
				}
			}
		case int64:
			switch d := d.(type) {
			case *int:
				x := int(s)
				if int64(x) != s {
					err = strconv.ErrRange
					x = 0
				}
				*d = x
			case *bool:
				*d = s != 0
			default:
				if d := reflect.ValueOf(d); d.Type().Kind() != reflect.Ptr {
					err = cannotConvert(d, s)
				} else {
					err = convertAssignInt(d.Elem(), s)
				}
			}
		default:
			err = cannotConvert(reflect.ValueOf(d), s)
		}
		if err != nil {
			break
		}
	}
	return multiBulk[len(dest):], err
}
