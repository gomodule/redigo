package redis

import (
	"encoding/json"
	"fmt"
)

const (
	// ExpireSecondsOption time
	ExpireSecondsOption = "EX"

	// ExpireMillisecondsOption time
	ExpireMillisecondsOption = "PX"

	// ExistOption = is exist key
	ExistOption = "XX"

	// NotExitOption is not exist key
	NotExitOption = "NX"
)

// Options is redis options
type Options struct {
	key               string
	oldKey            string
	keyValue          string
	data              interface{}
	count             int // for lrem
	expireSecond      int64
	expireMilliSecond int64
	notExist          bool
	exist             bool
	keepTTL           int
	rangeLower        int
	rangeUpper        int
}

// NewCommandOptions is build new options
func NewCommandOptions() *Options {
	return &Options{
		expireSecond:      0,
		expireMilliSecond: 0,
		keepTTL:           0,
	}
}

// CommandOptions is function options
type CommandOptions func(*Options)

// Build is generate options
func (options *Options) Build() ([]interface{}, error) {
	var op []interface{}
	if options == nil {
		return op, nil
	}

	if options.exist && options.notExist {
		return nil, fmt.Errorf("Error invalid. Please choose exist or not exist, dont take all")
	}

	if options.expireSecond > 0 && options.expireMilliSecond > 0 {
		return nil, fmt.Errorf("Error invalid. Please choose expire with second or expire with millisecond, dont take all")
	}

	if options.key != "" {
		op = append(op, options.key)
	}

	if options.oldKey != "" {
		op = append(op, options.oldKey)
	}

	if options.keyValue != "" {
		op = append(op, options.keyValue)
	}

	// For lrem command
	if options.oldKey != "" {
		op = append(op, options.oldKey)
	}

	if options.data != nil {
		raw, err := json.Marshal(options.data)
		if err != nil {
			return op, err
		}

		op = append(op, raw)
	}

	if options.expireSecond > 0 {
		op = append(op, ExpireSecondsOption)
		op = append(op, options.expireSecond)
	}

	if options.expireMilliSecond > 0 {
		op = append(op, ExpireMillisecondsOption)
		op = append(op, options.expireMilliSecond)
	}

	if options.exist {
		op = append(op, ExistOption)
	}

	if options.notExist {
		op = append(op, NotExitOption)
	}

	//return fmt.Sprintf("%s", strings.Join(op, ",")), nil
	return op, nil
}

// WithKey is set key
func WithKey(key string) CommandOptions {
	return func(options *Options) {
		options.key = key
	}
}

// WithOldKey is set key
func WithOldKey(key string) CommandOptions {
	return func(options *Options) {
		options.oldKey = key
	}
}

// WithKeyValue is set key
func WithKeyValue(keyValue string) CommandOptions {
	return func(options *Options) {
		options.keyValue = keyValue
	}
}

// WithCount is set key
func WithCount(count int) CommandOptions {
	return func(options *Options) {
		options.count = count
	}
}

// WithData is set key
func WithData(data interface{}) CommandOptions {
	return func(options *Options) {
		options.data = data
	}
}

// WithExpireSecond set expire time
func WithExpireSecond(ex int64) CommandOptions {
	return func(options *Options) {
		options.expireSecond = ex
	}

}

// WithExpireMillisecond is expire in millisecond
func WithExpireMillisecond(ex int64) CommandOptions {
	return func(options *Options) {
		options.expireMilliSecond = ex
	}
}

// WithExist is exist key
func WithExist() CommandOptions {
	return func(options *Options) {
		options.exist = true
	}
}

// WithNotExist is not exits key
func WithNotExist() CommandOptions {
	return func(options *Options) {
		options.notExist = true
	}
}

// WithRange is not exits key
func WithRange(lower, upper int) CommandOptions {
	return func(options *Options) {
		options.rangeLower = lower
		options.rangeUpper = upper
	}
}
