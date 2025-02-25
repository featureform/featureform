// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package types

import (
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
)

// default logger (can override)
var logger = logging.NewLogger("featureform types")

// basic representation of our featureform types,
// using iota for now, doesn't need an actual ordering for the moment
// todo: will change name later
type Type int

const (
	Int Type = iota
	Int8
	Int16
	Int32
	Int64
	UInt8
	UInt16
	UInt32
	UInt64
	Float32
	Float64
	String
	Bool
	Timestamp
	Datetime
	// todo: include byte/array/list enum
)

// our accepted numeric types
var NumericTypes = []Type{
	Int, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64}

type Provider int

const (
	Snowflake Provider = iota
	Iceberg
	DynamoDB
	Arrow
	// todo: expand
)

type NativeType string
type ColumnName string

type Schema struct {
	Fields []ColumnSchema
	// todo: can include more state or behavior, etc.
}

type ColumnSchema struct {
	Name       ColumnName
	NativeType NativeType
	Type       Type
	IsNullable bool
}

type ValueName string

// todo: core structs. update the Value to fit a better type representation other than any/interface?
type Value struct {
	NativeType NativeType  // todo: this will be some constant. is the provider's type (DynamoDB INT64)
	Type       Type        // this is our internal application type
	IsNull     bool        // indicates if the value from the provider should be considered Nil/NULL/None/Undefined
	Value      interface{} // "FFValue" may update later to a diff type with extensible receiver methods
	// todo: should we encapsulate the value and make it private?
}

// will safely convert the value to a string
func (v Value) ToString() string {
	if v.IsNull || v.Value == nil {
		logger.Info("value is null or nil, returning zeroed string")
		return ""
	}
	switch val := v.Value.(type) {
	case string:
		return val
	case int, int64, float64, bool:
		return fmt.Sprintf("%v", val)
	default:
		msg := fmt.Sprintf("unsupported type: %T", val)
		logger.Info(msg)
		return msg
	}
}

func (v Value) ToInt() (int, error) {
	if v.IsNull || v.Value == nil {
		logger.Info("value is null or nil, returning error")
		return 0, fferr.NewInternalErrorf("value is NULL")
	}
	switch val := v.Value.(type) {
	case int:
		return val, nil
	case int64:
		return int(val), nil
	case float64:
		return int(val), nil
	case string:
		logger.Warnw("value is type 'string', converting to int")
		return strconv.Atoi(val)
	default:
		defaultErr := fferr.NewInternalErrorf("cannot convert %T to int", val)
		logger.Error(defaultErr)
		return 0, defaultErr
	}
}

func (v Value) ToFloat() (float64, error) {
	if v.IsNull || v.Value == nil {
		logger.Info("value is null or nil, returning error")
		return 0, fferr.NewInternalErrorf("value is NULL")
	}
	switch val := v.Value.(type) {
	case float64:
		return val, nil
	case int:
		return float64(val), nil
	case string:
		logger.Warnw("value is type 'string', converting to float64")
		return strconv.ParseFloat(val, 64)
	default:
		defaultErr := fferr.NewInternalErrorf("cannot convert %T to float", val)
		logger.Error(defaultErr)
		return 0, defaultErr
	}
}

func (v Value) ToBool() (bool, error) {
	if v.IsNull || v.Value == nil {
		logger.Info("value is null or nil, returning error")
		return false, fferr.NewInternalErrorf("value is NULL")
	}
	switch val := v.Value.(type) {
	case bool:
		return val, nil
	case string:
		logger.Warnw("value is type 'string', converting to bool")
		return strconv.ParseBool(val)
	default:
		defaultErr := fferr.NewInternalErrorf("cannot convert %T to bool", val)
		logger.Error(defaultErr)
		return false, defaultErr
	}
}

// converts the value to a time.Time, but only if it's valid
func (v Value) ToTime() (time.Time, error) {
	if v.IsNull || v.Value == nil {
		logger.Info("value is null or nil, returning error")
		return time.Time{}, fferr.NewInternalErrorf("value is NULL")
	}
	switch val := v.Value.(type) {
	case time.Time:
		return val, nil
	case string:
		logger.Warnw("value is type 'string', converting to time.Time")
		return time.Parse(time.RFC3339, val)
	default:
		defaultErr := fferr.NewInternalErrorf("cannot convert %T to time.Time", val)
		logger.Error(defaultErr)
		return time.Time{}, defaultErr
	}
}

// todo: we can use a isNumeric() style check directly on the VALUE, but
// IMO defeats the purpose of tracking our own internal type
// since the provider value might be a number, but one that doesn't fit
// into FF_Value's accepted types. don't want to throw the caller off
// by assuming they can safely handle a number (as reported by the provider)
// if your lib can't handle it since it falls outside of our types
func (v Value) IsNumeric() bool {
	return slices.Contains(NumericTypes, v.Type)
}

func (v Value) IsText() bool {
	return v.Type == String
}

// just returns the raw interface value.
// probably won't be used much, but could be useful.
func (v Value) AsInterface() interface{} {
	return v.Value
}
