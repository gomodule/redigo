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
	"strconv"
)

var ErrNil = errors.New("redigo: nil returned")

func Values(multiBulk []interface{}, values ...interface{}) ([]interface{}, error) {
	if len(multiBulk) < len(values) {
		return nil, errors.New("redigo Values: short multibulk")
	}
	var err error
	for i, value := range values {
		bulk := multiBulk[i]
		if bulk != nil {
			switch value := value.(type) {
			case *string:
				*value, err = String(bulk, nil)
			case *int:
				*value, err = Int(bulk, nil)
			case *bool:
				*value, err = Bool(bulk, nil)
			case *[]byte:
				*value, err = Bytes(bulk, nil)
			default:
				panic("Value type not supported")
			}
			if err != nil {
				break
			}
		}
	}
	return multiBulk[len(values):], err
}

// Int is a helper that converts a Redis reply to an int. 
//
//  Reply type    Result
//  integer       return reply as int
//  bulk          parse decimal integer from reply
//  nil           return error ErrNil
//  other         return error
//  
func Int(v interface{}, err error) (int, error) {
	if err != nil {
		return 0, err
	}
	switch v := v.(type) {
	case int64:
		return int(v), nil
	case []byte:
		n, err := strconv.ParseInt(string(v), 10, 0)
		return int(n), err
	case nil:
		return 0, ErrNil
	case Error:
		return 0, v
	}
	return 0, fmt.Errorf("redigo: unexpected type for Int, got type %T", v)
}

// String is a helper that converts a Redis reply to a string. 
//
//  Reply type      Result
//  integer         format as decimal string
//  bulk            return reply as string
//  string          return as is
//  nil             return error ErrNil
//  other           return error
func String(v interface{}, err error) (string, error) {
	if err != nil {
		return "", err
	}
	switch v := v.(type) {
	case []byte:
		return string(v), nil
	case string:
		return v, nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case nil:
		return "", ErrNil
	case Error:
		return "", v
	}
	return "", fmt.Errorf("redigo: unexpected type for String, got type %T", v)
}

// Bytes is a helper that converts a Redis reply to slice of bytes. 
//
//  Reply type      Result
//  integer         format as decimal string
//  bulk            return reply as slice of bytes
//  string          return reply as slice of bytes
//  nil             return error ErrNil
//  other           return error
func Bytes(v interface{}, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	switch v := v.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	case int64:
		return strconv.AppendInt(nil, v, 10), nil
	case nil:
		return nil, ErrNil
	case Error:
		return nil, v
	}
	return nil, fmt.Errorf("redigo: unexpected type for Bytes, got type %T", v)
}

// Bool is a helper that converts a Redis reply to a bool. 
//
//  Reply type      Result
//  integer         return value != 0
//  bulk            return false if reply is "" or "0", otherwise return true.
//  nil             return error ErrNil
//  other           return error
func Bool(v interface{}, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	switch v := v.(type) {
	case int64:
		return v != 0, nil
	case []byte:
		if len(v) == 0 || (len(v) == 1 && v[0] == '0') {
			return false, nil
		}
		return true, nil
	case nil:
		return false, ErrNil
	case Error:
		return false, v
	}
	return false, fmt.Errorf("redigo: unexpected type for Bool, got type %T", v)
}

// MultiBulk is a helper that converts a Redis reply to a []interface{}. 
//
//  Reply type      Result
//  multi-bulk      return []interface{}
//  nil             return error ErrNil
//  other           return error
func MultiBulk(v interface{}, err error) ([]interface{}, error) {
	if err != nil {
		return nil, err
	}
	switch v := v.(type) {
	case []interface{}:
		return v, nil
	case nil:
		return nil, ErrNil
	case Error:
		return nil, v
	}
	return nil, fmt.Errorf("redigo: unexpected type for MultiBulk, got type %T", v)
}
