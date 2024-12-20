// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package serialization

import (
	"fmt"
	"strconv"
)

func CastNumberToFloat32(value any) (float32, error) {
	// I have to do one type per case for this to work properly.
	switch typed := value.(type) {
	case int:
		return float32(typed), nil
	case int32:
		return float32(typed), nil
	case int64:
		return float32(typed), nil
	case int8:
		return float32(typed), nil
	case int16:
		return float32(typed), nil
	case float32:
		return typed, nil
	case float64:
		return float32(typed), nil
	case string:
		f64, err := strconv.ParseFloat(typed, 32)
		if err != nil {
			return 0, fmt.Errorf("Type error: Expected numerical type and got %T: %e", typed, err)
		}
		return float32(f64), nil
	default:
		return 0, fmt.Errorf("Type error: Expected numerical type and got %T", typed)
	}
}

func CastNumberToFloat64(value any) (float64, error) {
	// I have to do one type per case for this to work properly.
	switch typed := value.(type) {
	case int:
		return float64(typed), nil
	case int32:
		return float64(typed), nil
	case int64:
		return float64(typed), nil
	case int8:
		return float64(typed), nil
	case int16:
		return float64(typed), nil
	case float32:
		return float64(typed), nil
	case float64:
		return typed, nil
	case string:
		f64, err := strconv.ParseFloat(typed, 64)
		if err != nil {
			return 0, fmt.Errorf("Type error: Expected numerical type and got %T: %e", typed, err)
		}
		return f64, nil
	default:
		return 0, fmt.Errorf("Type error: Expected numerical type and got %T", typed)
	}
}

func CastNumberToInt(value any) (int, error) {
	// I have to do one type per case for this to work properly.
	switch typed := value.(type) {
	case int:
		return typed, nil
	case int32:
		return int(typed), nil
	case int64:
		return int(typed), nil
	case int8:
		return int(typed), nil
	case int16:
		return int(typed), nil
	case float32:
		return int(typed), nil
	case float64:
		return int(typed), nil
	case string:
		val, err := strconv.ParseInt(typed, 10, 64)
		// Handle cases like 1.0
		if err != nil {
			fVal, nErr := strconv.ParseFloat(typed, 64)
			if nErr == nil {
				return int(fVal), nil
			}
		}
		return int(val), err
	default:
		return 0, fmt.Errorf("Type error: Expected numerical type and got %T", typed)
	}
}

func CastNumberToInt32(value any) (int32, error) {
	// I have to do one type per case for this to work properly.
	switch typed := value.(type) {
	case int:
		return int32(typed), nil
	case int32:
		return typed, nil
	case int64:
		return int32(typed), nil
	case int8:
		return int32(typed), nil
	case int16:
		return int32(typed), nil
	case float32:
		return int32(typed), nil
	case float64:
		return int32(typed), nil
	case string:
		val, err := strconv.ParseInt(typed, 10, 32)
		// Handle cases like 1.0
		if err != nil {
			fVal, nErr := strconv.ParseFloat(typed, 64)
			if nErr == nil {
				return int32(fVal), nil
			}
		}
		return int32(val), err
	default:
		return 0, fmt.Errorf("Type error: Expected numerical type and got %T", typed)
	}
}

func CastNumberToInt64(value any) (int64, error) {
	// I have to do one type per case for this to work properly.
	switch typed := value.(type) {
	case int:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case int64:
		return typed, nil
	case int8:
		return int64(typed), nil
	case int16:
		return int64(typed), nil
	case float32:
		return int64(typed), nil
	case float64:
		return int64(typed), nil
	case string:
		val, err := strconv.ParseInt(typed, 10, 64)
		// Handle cases like 1.0
		if err != nil {
			fVal, nErr := strconv.ParseFloat(typed, 64)
			if nErr == nil {
				return int64(fVal), nil
			}
		}
		return val, err
	default:
		return 0, fmt.Errorf("Type error: Expected numerical type and got %T", typed)
	}
}

func CastBool(value any) (bool, error) {
	switch casted := value.(type) {
	case bool:
		return casted, nil
	case string:
		return strconv.ParseBool(casted)
	case int, int32, int64:
		isFalse := casted == 0
		return !isFalse, nil
	default:
		return false, fmt.Errorf("Type error: Expected numerical type and got %T", casted)
	}
}
