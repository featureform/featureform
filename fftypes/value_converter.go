package types

import (
	"fmt"
	"strconv"
)

type ValueConverter[T any] interface {
	ConvertValue(nativeType NativeType, value T) (Value, error)
	GetTypeMap() NativeToValueTypeMapper
}

func CastNumberToInt(v any) (int, error) {
	switch casted := v.(type) {
	case int:
		return casted, nil
	case int32:
		return int(casted), nil
	case int64:
		return int(casted), nil
	case float32:
		return int(casted), nil
	case float64:
		return int(casted), nil
	case string:
		intVal, err := strconv.ParseInt(casted, 10, 0)
		if err != nil {
			return 0, fmt.Errorf("failed to parse int from string: %w", err)
		}
		return int(intVal), nil
	default:
		return 0, fmt.Errorf("cannot cast %T to int", v)
	}
}

func CastNumberToInt32(v any) (int32, error) {
	switch casted := v.(type) {
	case int:
		return int32(casted), nil
	case int32:
		return casted, nil
	case int64:
		return int32(casted), nil
	case float32:
		return int32(casted), nil
	case float64:
		return int32(casted), nil
	case string:
		intVal, err := strconv.ParseInt(casted, 10, 32)
		if err != nil {
			return 0, fmt.Errorf("failed to parse int32 from string: %w", err)
		}
		return int32(intVal), nil
	default:
		return 0, fmt.Errorf("cannot cast %T to int32", v)
	}
}

func CastNumberToInt64(v any) (int64, error) {
	switch casted := v.(type) {
	case int:
		return int64(casted), nil
	case int32:
		return int64(casted), nil
	case int64:
		return casted, nil
	case float32:
		return int64(casted), nil
	case float64:
		return int64(casted), nil
	case string:
		return strconv.ParseInt(casted, 10, 64)
	default:
		return 0, fmt.Errorf("cannot cast %T to int64", v)
	}
}

func CastNumberToFloat32(v any) (float32, error) {
	switch casted := v.(type) {
	case int:
		return float32(casted), nil
	case int32:
		return float32(casted), nil
	case int64:
		return float32(casted), nil
	case float32:
		return casted, nil
	case float64:
		return float32(casted), nil
	case string:
		floatVal, err := strconv.ParseFloat(casted, 32)
		if err != nil {
			return 0, fmt.Errorf("failed to parse float32 from string: %w", err)
		}
		return float32(floatVal), nil
	default:
		return 0, fmt.Errorf("cannot cast %T to float32", v)
	}
}

func CastNumberToFloat64(v any) (float64, error) {
	switch casted := v.(type) {
	case int:
		return float64(casted), nil
	case int32:
		return float64(casted), nil
	case int64:
		return float64(casted), nil
	case float32:
		return float64(casted), nil
	case float64:
		return casted, nil
	case string:
		return strconv.ParseFloat(casted, 64)
	default:
		return 0, fmt.Errorf("cannot cast %T to float64", v)
	}
}

func CastBool(v any) (bool, error) {
	switch casted := v.(type) {
	case bool:
		return casted, nil
	case string:
		return strconv.ParseBool(casted)
	case int:
		return casted != 0, nil
	case int32:
		return casted != 0, nil
	case int64:
		return casted != 0, nil
	default:
		return false, fmt.Errorf("cannot cast %T to bool", v)
	}
}
