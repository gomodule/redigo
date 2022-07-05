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
	"strings"
	"sync"
)

var (
	scannerType = reflect.TypeOf((*Scanner)(nil)).Elem()
)

func ensureLen(d reflect.Value, n int) {
	if n > d.Cap() {
		d.Set(reflect.MakeSlice(d.Type(), n, n))
	} else {
		d.SetLen(n)
	}
}

func cannotConvert(d reflect.Value, s interface{}) error {
	var sname string
	switch s.(type) {
	case string:
		sname = "Redis simple string"
	case Error:
		sname = "Redis error"
	case int64:
		sname = "Redis integer"
	case []byte:
		sname = "Redis bulk string"
	case []interface{}:
		sname = "Redis array"
	case nil:
		sname = "Redis nil"
	default:
		sname = reflect.TypeOf(s).String()
	}
	return fmt.Errorf("cannot convert from %s to %s", sname, d.Type())
}

func convertAssignNil(d reflect.Value) (err error) {
	switch d.Type().Kind() {
	case reflect.Slice, reflect.Interface:
		d.Set(reflect.Zero(d.Type()))
	default:
		err = cannotConvert(d, nil)
	}
	return err
}

func convertAssignError(d reflect.Value, s Error) (err error) {
	if d.Kind() == reflect.String {
		d.SetString(string(s))
	} else if d.Kind() == reflect.Slice && d.Type().Elem().Kind() == reflect.Uint8 {
		d.SetBytes([]byte(s))
	} else {
		err = cannotConvert(d, s)
	}
	return
}

func convertAssignString(d reflect.Value, s string) (err error) {
	switch d.Type().Kind() {
	case reflect.Float32, reflect.Float64:
		var x float64
		x, err = strconv.ParseFloat(s, d.Type().Bits())
		d.SetFloat(x)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var x int64
		x, err = strconv.ParseInt(s, 10, d.Type().Bits())
		d.SetInt(x)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var x uint64
		x, err = strconv.ParseUint(s, 10, d.Type().Bits())
		d.SetUint(x)
	case reflect.Bool:
		var x bool
		x, err = strconv.ParseBool(s)
		d.SetBool(x)
	case reflect.String:
		d.SetString(s)
	case reflect.Slice:
		if d.Type().Elem().Kind() == reflect.Uint8 {
			d.SetBytes([]byte(s))
		} else {
			err = cannotConvert(d, s)
		}
	case reflect.Ptr:
		err = convertAssignString(d.Elem(), s)
	default:
		err = cannotConvert(d, s)
	}
	return
}

func convertAssignBulkString(d reflect.Value, s []byte) (err error) {
	switch d.Type().Kind() {
	case reflect.Slice:
		// Handle []byte destination here to avoid unnecessary
		// []byte -> string -> []byte converion.
		if d.Type().Elem().Kind() == reflect.Uint8 {
			d.SetBytes(s)
		} else {
			err = cannotConvert(d, s)
		}
	case reflect.Ptr:
		if d.CanInterface() && d.CanSet() {
			if s == nil {
				if d.IsNil() {
					return nil
				}

				d.Set(reflect.Zero(d.Type()))
				return nil
			}

			if d.IsNil() {
				d.Set(reflect.New(d.Type().Elem()))
			}

			if sc, ok := d.Interface().(Scanner); ok {
				return sc.RedisScan(s)
			}
		}
		err = convertAssignString(d, string(s))
	default:
		err = convertAssignString(d, string(s))
	}
	return err
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

func convertAssignValue(d reflect.Value, s interface{}) (err error) {
	if d.Kind() != reflect.Ptr {
		if d.CanAddr() {
			d2 := d.Addr()
			if d2.CanInterface() {
				if scanner, ok := d2.Interface().(Scanner); ok {
					return scanner.RedisScan(s)
				}
			}
		}
	} else if d.CanInterface() {
		// Already a reflect.Ptr
		if d.IsNil() {
			d.Set(reflect.New(d.Type().Elem()))
		}
		if scanner, ok := d.Interface().(Scanner); ok {
			return scanner.RedisScan(s)
		}
	}

	switch s := s.(type) {
	case nil:
		err = convertAssignNil(d)
	case []byte:
		err = convertAssignBulkString(d, s)
	case int64:
		err = convertAssignInt(d, s)
	case string:
		err = convertAssignString(d, s)
	case Error:
		err = convertAssignError(d, s)
	default:
		err = cannotConvert(d, s)
	}
	return err
}

func convertAssignArray(d reflect.Value, s []interface{}) error {
	if d.Type().Kind() != reflect.Slice {
		return cannotConvert(d, s)
	}
	ensureLen(d, len(s))
	for i := 0; i < len(s); i++ {
		if err := convertAssignValue(d.Index(i), s[i]); err != nil {
			return err
		}
	}
	return nil
}

func convertAssign(d interface{}, s interface{}) (err error) {
	if scanner, ok := d.(Scanner); ok {
		return scanner.RedisScan(s)
	}

	// Handle the most common destination types using type switches and
	// fall back to reflection for all other types.
	switch s := s.(type) {
	case nil:
		// ignore
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
		case *interface{}:
			*d = s
		case nil:
			// skip value
		default:
			if d := reflect.ValueOf(d); d.Type().Kind() != reflect.Ptr {
				err = cannotConvert(d, s)
			} else {
				err = convertAssignBulkString(d.Elem(), s)
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
		case *interface{}:
			*d = s
		case nil:
			// skip value
		default:
			if d := reflect.ValueOf(d); d.Type().Kind() != reflect.Ptr {
				err = cannotConvert(d, s)
			} else {
				err = convertAssignInt(d.Elem(), s)
			}
		}
	case string:
		switch d := d.(type) {
		case *string:
			*d = s
		case *interface{}:
			*d = s
		case nil:
			// skip value
		default:
			err = cannotConvert(reflect.ValueOf(d), s)
		}
	case []interface{}:
		switch d := d.(type) {
		case *[]interface{}:
			*d = s
		case *interface{}:
			*d = s
		case nil:
			// skip value
		default:
			if d := reflect.ValueOf(d); d.Type().Kind() != reflect.Ptr {
				err = cannotConvert(d, s)
			} else {
				err = convertAssignArray(d.Elem(), s)
			}
		}
	case Error:
		err = s
	default:
		err = cannotConvert(reflect.ValueOf(d), s)
	}
	return
}

// Scan copies from src to the values pointed at by dest.
//
// Scan uses RedisScan if available otherwise:
//
// The values pointed at by dest must be an integer, float, boolean, string,
// []byte, interface{} or slices of these types. Scan uses the standard strconv
// package to convert bulk strings to numeric and boolean types.
//
// If a dest value is nil, then the corresponding src value is skipped.
//
// If a src element is nil, then the corresponding dest value is not modified.
//
// To enable easy use of Scan in a loop, Scan returns the slice of src
// following the copied values.
func Scan(src []interface{}, dest ...interface{}) ([]interface{}, error) {
	if len(src) < len(dest) {
		return nil, errors.New("redigo.Scan: array short")
	}
	var err error
	for i, d := range dest {
		err = convertAssign(d, src[i])
		if err != nil {
			err = fmt.Errorf("redigo.Scan: cannot assign to dest %d: %v", i, err)
			break
		}
	}
	return src[len(dest):], err
}

type fieldSpec struct {
	name      string
	index     []int
	omitEmpty bool
}

type structSpec struct {
	m map[string]*fieldSpec
	l []*fieldSpec
}

func (ss *structSpec) fieldSpec(name []byte) *fieldSpec {
	return ss.m[string(name)]
}

func compileStructSpec(t reflect.Type, depth map[string]int, index []int, ss *structSpec, seen map[reflect.Type]struct{}) error {
	if _, ok := seen[t]; ok {
		// Protect against infinite recursion.
		return fmt.Errorf("recursive struct definition for %v", t)
	}

	seen[t] = struct{}{}
LOOP:
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		switch {
		case f.PkgPath != "" && !f.Anonymous:
			// Ignore unexported fields.
		case f.Anonymous:
			switch f.Type.Kind() {
			case reflect.Struct:
				if err := compileStructSpec(f.Type, depth, append(index, i), ss, seen); err != nil {
					return err
				}
			case reflect.Ptr:
				if f.Type.Elem().Kind() == reflect.Struct {
					if err := compileStructSpec(f.Type.Elem(), depth, append(index, i), ss, seen); err != nil {
						return err
					}
				}
			}
		default:
			fs := &fieldSpec{name: f.Name}
			tag := f.Tag.Get("redis")

			var p string
			first := true
			for len(tag) > 0 {
				i := strings.IndexByte(tag, ',')
				if i < 0 {
					p, tag = tag, ""
				} else {
					p, tag = tag[:i], tag[i+1:]
				}
				if p == "-" {
					continue LOOP
				}
				if first && len(p) > 0 {
					fs.name = p
					first = false
				} else {
					switch p {
					case "omitempty":
						fs.omitEmpty = true
					default:
						panic(fmt.Errorf("redigo: unknown field tag %s for type %s", p, t.Name()))
					}
				}
			}

			d, found := depth[fs.name]
			if !found {
				d = 1 << 30
			}

			switch {
			case len(index) == d:
				// At same depth, remove from result.
				delete(ss.m, fs.name)
				j := 0
				for i := 0; i < len(ss.l); i++ {
					if fs.name != ss.l[i].name {
						ss.l[j] = ss.l[i]
						j += 1
					}
				}
				ss.l = ss.l[:j]
			case len(index) < d:
				fs.index = make([]int, len(index)+1)
				copy(fs.index, index)
				fs.index[len(index)] = i
				depth[fs.name] = len(index)
				ss.m[fs.name] = fs
				ss.l = append(ss.l, fs)
			}
		}
	}

	return nil
}

var (
	structSpecMutex sync.RWMutex
	structSpecCache = make(map[reflect.Type]*structSpec)
)

func structSpecForType(t reflect.Type) (*structSpec, error) {
	structSpecMutex.RLock()
	ss, found := structSpecCache[t]
	structSpecMutex.RUnlock()
	if found {
		return ss, nil
	}

	structSpecMutex.Lock()
	defer structSpecMutex.Unlock()
	ss, found = structSpecCache[t]
	if found {
		return ss, nil
	}

	ss = &structSpec{m: make(map[string]*fieldSpec)}
	if err := compileStructSpec(t, make(map[string]int), nil, ss, make(map[reflect.Type]struct{})); err != nil {
		return nil, fmt.Errorf("compile struct: %s: %w", t, err)
	}
	structSpecCache[t] = ss
	return ss, nil
}

var errScanStructValue = errors.New("redigo.ScanStruct: value must be non-nil pointer to a struct")

// ScanStruct scans alternating names and values from src to a struct. The
// HGETALL and CONFIG GET commands return replies in this format.
//
// ScanStruct uses exported field names to match values in the response. Use
// 'redis' field tag to override the name:
//
//      Field int `redis:"myName"`
//
// Fields with the tag redis:"-" are ignored.
//
// Each field uses RedisScan if available otherwise:
// Integer, float, boolean, string and []byte fields are supported. Scan uses the
// standard strconv package to convert bulk string values to numeric and
// boolean types.
//
// If a src element is nil, then the corresponding field is not modified.
func ScanStruct(src []interface{}, dest interface{}) error {
	d := reflect.ValueOf(dest)
	if d.Kind() != reflect.Ptr || d.IsNil() {
		return errScanStructValue
	}

	d = d.Elem()
	if d.Kind() != reflect.Struct {
		return errScanStructValue
	}

	if len(src)%2 != 0 {
		return errors.New("redigo.ScanStruct: number of values not a multiple of 2")
	}

	ss, err := structSpecForType(d.Type())
	if err != nil {
		return fmt.Errorf("redigo.ScanStruct: %w", err)
	}

	for i := 0; i < len(src); i += 2 {
		s := src[i+1]
		if s == nil {
			continue
		}

		name, ok := src[i].([]byte)
		if !ok {
			return fmt.Errorf("redigo.ScanStruct: key %d not a bulk string value", i)
		}

		fs := ss.fieldSpec(name)
		if fs == nil {
			continue
		}

		if err := convertAssignValue(fieldByIndexCreate(d, fs.index), s); err != nil {
			return fmt.Errorf("redigo.ScanStruct: cannot assign field %s: %v", fs.name, err)
		}
	}
	return nil
}

var (
	errScanSliceValue = errors.New("redigo.ScanSlice: dest must be non-nil pointer to a struct")
)

// ScanSlice scans src to the slice pointed to by dest.
//
// If the target is a slice of types which implement Scanner then the custom
// RedisScan method is used otherwise the following rules apply:
//
// The elements in the dest slice must be integer, float, boolean, string, struct
// or pointer to struct values.
//
// Struct fields must be integer, float, boolean or string values. All struct
// fields are used unless a subset is specified using fieldNames.
func ScanSlice(src []interface{}, dest interface{}, fieldNames ...string) error {
	d := reflect.ValueOf(dest)
	if d.Kind() != reflect.Ptr || d.IsNil() {
		return errScanSliceValue
	}
	d = d.Elem()
	if d.Kind() != reflect.Slice {
		return errScanSliceValue
	}

	isPtr := false
	t := d.Type().Elem()
	st := t
	if t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Struct {
		isPtr = true
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct || st.Implements(scannerType) {
		ensureLen(d, len(src))
		for i, s := range src {
			if s == nil {
				continue
			}
			if err := convertAssignValue(d.Index(i), s); err != nil {
				return fmt.Errorf("redigo.ScanSlice: cannot assign element %d: %v", i, err)
			}
		}
		return nil
	}

	ss, err := structSpecForType(t)
	if err != nil {
		return fmt.Errorf("redigo.ScanSlice: %w", err)
	}

	fss := ss.l
	if len(fieldNames) > 0 {
		fss = make([]*fieldSpec, len(fieldNames))
		for i, name := range fieldNames {
			fss[i] = ss.m[name]
			if fss[i] == nil {
				return fmt.Errorf("redigo.ScanSlice: ScanSlice bad field name %s", name)
			}
		}
	}

	if len(fss) == 0 {
		return errors.New("redigo.ScanSlice: no struct fields")
	}

	n := len(src) / len(fss)
	if n*len(fss) != len(src) {
		return errors.New("redigo.ScanSlice: length not a multiple of struct field count")
	}

	ensureLen(d, n)
	for i := 0; i < n; i++ {
		d := d.Index(i)
		if isPtr {
			if d.IsNil() {
				d.Set(reflect.New(t))
			}
			d = d.Elem()
		}
		for j, fs := range fss {
			s := src[i*len(fss)+j]
			if s == nil {
				continue
			}
			if err := convertAssignValue(d.FieldByIndex(fs.index), s); err != nil {
				return fmt.Errorf("redigo.ScanSlice: cannot assign element %d to field %s: %v", i*len(fss)+j, fs.name, err)
			}
		}
	}
	return nil
}

// Args is a helper for constructing command arguments from structured values.
type Args []interface{}

// Add returns the result of appending value to args.
func (args Args) Add(value ...interface{}) Args {
	return append(args, value...)
}

// AddFlat returns the result of appending the flattened value of v to args.
//
// Maps are flattened by appending the alternating keys and map values to args.
//
// Slices are flattened by appending the slice elements to args.
//
// Structs are flattened by appending the alternating names and values of
// exported fields to args. If v is a nil struct pointer, then nothing is
// appended. The 'redis' field tag overrides struct field names. See ScanStruct
// for more information on the use of the 'redis' field tag.
//
// Other types are appended to args as is.
// panics if v includes a recursive anonymous struct.
func (args Args) AddFlat(v interface{}) Args {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Struct:
		args = flattenStruct(args, rv)
	case reflect.Slice:
		for i := 0; i < rv.Len(); i++ {
			args = append(args, rv.Index(i).Interface())
		}
	case reflect.Map:
		for _, k := range rv.MapKeys() {
			args = append(args, k.Interface(), rv.MapIndex(k).Interface())
		}
	case reflect.Ptr:
		if rv.Type().Elem().Kind() == reflect.Struct {
			if !rv.IsNil() {
				args = flattenStruct(args, rv.Elem())
			}
		} else {
			args = append(args, v)
		}
	default:
		args = append(args, v)
	}
	return args
}

func flattenStruct(args Args, v reflect.Value) Args {
	ss, err := structSpecForType(v.Type())
	if err != nil {
		panic(fmt.Errorf("redigo.AddFlat: %w", err))
	}

	for _, fs := range ss.l {
		fv, err := fieldByIndexErr(v, fs.index)
		if err != nil {
			// Nil item ignore.
			continue
		}
		if fs.omitEmpty {
			var empty = false
			switch fv.Kind() {
			case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
				empty = fv.Len() == 0
			case reflect.Bool:
				empty = !fv.Bool()
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				empty = fv.Int() == 0
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
				empty = fv.Uint() == 0
			case reflect.Float32, reflect.Float64:
				empty = fv.Float() == 0
			case reflect.Interface, reflect.Ptr:
				empty = fv.IsNil()
			}
			if empty {
				continue
			}
		}
		if arg, ok := fv.Interface().(Argument); ok {
			args = append(args, fs.name, arg.RedisArg())
		} else if fv.Kind() == reflect.Ptr {
			if !fv.IsNil() {
				args = append(args, fs.name, fv.Elem().Interface())
			}
		} else {
			args = append(args, fs.name, fv.Interface())
		}
	}
	return args
}
