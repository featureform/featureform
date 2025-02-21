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
)

// basic representation of our featureform types,
// using iota for now, doesn't need an actual ordering for the moment
// todo: will change name later
type FF_Type int

const (
	Int FF_Type = iota
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
var NumericTypes = []FF_Type{
	Int, Int8, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64}

// todo: core structs. update the Value to fit a better type representation other than any/interface?
type Value struct {
	Provider   string      // where the value originated from, can be enumeration note: adding this for extra context
	Name       string      // the name of the value
	NativeType string      // todo: this will be some constant. is the provider's type (DynamoDB INT64)
	Type       FF_Type     // this is our internal application type
	IsNull     bool        // indicates if the value from the provider should be considered Nil/NULL/None/Undefined
	Value      interface{} // "FFValue" may update later to a diff type with extensible receiver methods
}

// will safely convert the value to a string
func (v Value) ToString() string {
	if v.IsNull || v.Value == nil {
		return ""
	}
	switch val := v.Value.(type) {
	case string:
		return val
	case int, int64, float64, bool:
		return fmt.Sprintf("%v", val)
	default:
		return fmt.Sprintf("unsupported type: %T", val)
	}
}

func (v Value) ToInt() (int, error) {
	if v.IsNull || v.Value == nil {
		return 0, fmt.Errorf("value is NULL")
	}
	switch val := v.Value.(type) {
	case int:
		return val, nil
	case int64:
		return int(val), nil
	case float64:
		return int(val), nil
	case string:
		return strconv.Atoi(val)
	default:
		return 0, fmt.Errorf("cannot convert %T to int", val)
	}
}

func (v Value) ToFloat() (float64, error) {
	if v.IsNull || v.Value == nil {
		return 0, fmt.Errorf("value is NULL")
	}
	switch val := v.Value.(type) {
	case float64:
		return val, nil
	case int:
		return float64(val), nil
	case string:
		return strconv.ParseFloat(val, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float", val)
	}
}

func (v Value) ToBool() (bool, error) {
	if v.IsNull || v.Value == nil {
		return false, fmt.Errorf("value is NULL")
	}
	switch val := v.Value.(type) {
	case bool:
		return val, nil
	case string:
		return strconv.ParseBool(val)
	default:
		return false, fmt.Errorf("cannot convert %T to bool", val)
	}
}

// converts the value to a time.Time, but only if it's valid
func (v Value) ToTime() (time.Time, error) {
	if v.IsNull || v.Value == nil {
		return time.Time{}, fmt.Errorf("value is NULL")
	}
	switch val := v.Value.(type) {
	case time.Time:
		return val, nil
	case string:
		return time.Parse(time.RFC3339, val) // Example format, may need customization
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", val)
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
