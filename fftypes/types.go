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

var logger = logging.NewLogger("featureform types")

type Type int

const (
	Int Type = iota
	Int64
	Float64
	String
	Bool
	Timestamp
	Datetime
	// todo: include byte/array/list enum. may skip for 1st iteration?
)

var NumericTypes = []Type{Int, Int64, Float64}

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

type Value struct {
	NativeType NativeType
	Type       Type
	IsNull     bool
	Value      any

}

func (v Value) ToString() (string, error) {
	if v.IsNull {
		logger.Info("value is null, returning empty string")
		return "", nil
	}
	if v.Value == nil {
		logger.Info("value is nil, returning empty string")
		return "", nil
	}
	switch val := v.Value.(type) {
	case string:
		return val, nil
	case int, int64, float64, bool:
		return fmt.Sprintf("%v", val), nil
	default:
		defaultErr := fferr.NewInternalErrorf("unsupported type: %T", val)
		logger.Error(defaultErr)
		return "", defaultErr
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
		logger.Debug("value is type 'string', converting to int")
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
		logger.Debug("value is type 'string', converting to bool")
		return strconv.ParseBool(val)
	default:
		defaultErr := fferr.NewInternalErrorf("cannot convert %T to bool", val)
		logger.Error(defaultErr)
		return false, defaultErr
	}
}

func (v Value) ToTime() (time.Time, error) {
	if v.IsNull || v.Value == nil {
		logger.Info("value is null or nil, returning error")
		return time.Time{}, fferr.NewInternalErrorf("value is NULL")
	}
	switch val := v.Value.(type) {
	case time.Time:
		return val, nil
	case string:
		logger.Debug("value is type 'string', converting to time.Time")
		return time.Parse(time.RFC3339, val)
	default:
		defaultErr := fferr.NewInternalErrorf("cannot convert %T to time.Time", val)
		logger.Error(defaultErr)
		return time.Time{}, defaultErr
	}
}

func (v Value) IsNumeric() bool {
	return slices.Contains(NumericTypes, v.Type)
}

func (v Value) IsText() bool {
	return v.Type == String
}

func (v Value) AsAny() any {
	return v.Value
}
