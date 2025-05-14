package types

import (
	"fmt"
	"strconv"
	"time"

	"github.com/araddon/dateparse"

	"github.com/featureform/fferr"
)

type ValueConverter[T any] interface {
	ParseNativeType(nativeTypeDetails NativeTypeDetails) (NewNativeType, error)
	GetType(nativeType NewNativeType) (ValueType, error)
	ConvertValue(nativeType NewNativeType, value T) (Value, error)
}

func ConvertNumberToInt(v any) (int, error) {
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

func ConvertNumberToInt8(v any) (int8, error) {
	switch casted := v.(type) {
	case int8:
		return casted, nil
	case int:
		return int8(casted), nil
	case int16:
		return int8(casted), nil
	case int32:
		return int8(casted), nil
	case int64:
		return int8(casted), nil
	case float32:
		return int8(casted), nil
	case float64:
		return int8(casted), nil
	case string:
		intVal, err := strconv.ParseInt(casted, 10, 8)
		if err != nil {
			return 0, fmt.Errorf("failed to parse int8 from string: %w", err)
		}
		return int8(intVal), nil
	default:
		return 0, fmt.Errorf("cannot cast %T to int8", v)
	}
}

func ConvertNumberToInt16(v any) (int16, error) {
	switch casted := v.(type) {
	case int8:
		return int16(casted), nil
	case int16:
		return casted, nil
	case int:
		return int16(casted), nil
	case int32:
		return int16(casted), nil
	case int64:
		return int16(casted), nil
	case float32:
		return int16(casted), nil
	case float64:
		return int16(casted), nil
	case string:
		intVal, err := strconv.ParseInt(casted, 10, 16)
		if err != nil {
			return 0, fmt.Errorf("failed to parse int16 from string: %w", err)
		}
		return int16(intVal), nil
	default:
		return 0, fmt.Errorf("cannot cast %T to int16", v)
	}
}

func ConvertNumberToInt32(v any) (int32, error) {
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

func ConvertNumberToInt64(v any) (int64, error) {
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
		intVal, err := strconv.ParseInt(casted, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse int64 from string: %w", err)
		}
		return intVal, nil
	default:
		return 0, fmt.Errorf("cannot cast %T to int64", v)
	}
}

func ConvertNumberToUint8(v any) (uint8, error) {
	switch casted := v.(type) {
	case uint8:
		return casted, nil
	case int:
		return uint8(casted), nil
	case int8:
		return uint8(casted), nil
	case int16:
		return uint8(casted), nil
	case int32:
		return uint8(casted), nil
	case int64:
		return uint8(casted), nil
	case uint16:
		return uint8(casted), nil
	case uint32:
		return uint8(casted), nil
	case uint64:
		return uint8(casted), nil
	case float32:
		return uint8(casted), nil
	case float64:
		return uint8(casted), nil
	case string:
		uintVal, err := strconv.ParseUint(casted, 10, 8)
		if err != nil {
			return 0, fmt.Errorf("failed to parse uint8 from string: %w", err)
		}
		return uint8(uintVal), nil
	default:
		return 0, fmt.Errorf("cannot cast %T to uint8", v)
	}
}

func ConvertNumberToUint16(v any) (uint16, error) {
	switch casted := v.(type) {
	case uint8:
		return uint16(casted), nil
	case uint16:
		return casted, nil
	case int:
		return uint16(casted), nil
	case int8:
		return uint16(casted), nil
	case int16:
		return uint16(casted), nil
	case int32:
		return uint16(casted), nil
	case int64:
		return uint16(casted), nil
	case uint32:
		return uint16(casted), nil
	case uint64:
		return uint16(casted), nil
	case float32:
		return uint16(casted), nil
	case float64:
		return uint16(casted), nil
	case string:
		uintVal, err := strconv.ParseUint(casted, 10, 16)
		if err != nil {
			return 0, fmt.Errorf("failed to parse uint16 from string: %w", err)
		}
		return uint16(uintVal), nil
	default:
		return 0, fmt.Errorf("cannot cast %T to uint16", v)
	}
}

func ConvertNumberToUint32(v any) (uint32, error) {
	switch casted := v.(type) {
	case uint8:
		return uint32(casted), nil
	case uint16:
		return uint32(casted), nil
	case uint32:
		return casted, nil
	case int:
		return uint32(casted), nil
	case int8:
		return uint32(casted), nil
	case int16:
		return uint32(casted), nil
	case int32:
		return uint32(casted), nil
	case int64:
		return uint32(casted), nil
	case uint64:
		return uint32(casted), nil
	case float32:
		return uint32(casted), nil
	case float64:
		return uint32(casted), nil
	case string:
		uintVal, err := strconv.ParseUint(casted, 10, 32)
		if err != nil {
			return 0, fmt.Errorf("failed to parse uint32 from string: %w", err)
		}
		return uint32(uintVal), nil
	default:
		return 0, fmt.Errorf("cannot cast %T to uint32", v)
	}
}

func ConvertNumberToUint64(v any) (uint64, error) {
	switch casted := v.(type) {
	case uint8:
		return uint64(casted), nil
	case uint16:
		return uint64(casted), nil
	case uint32:
		return uint64(casted), nil
	case uint64:
		return casted, nil
	case int:
		return uint64(casted), nil
	case int8:
		return uint64(casted), nil
	case int16:
		return uint64(casted), nil
	case int32:
		return uint64(casted), nil
	case int64:
		return uint64(casted), nil
	case float32:
		return uint64(casted), nil
	case float64:
		return uint64(casted), nil
	case string:
		uintVal, err := strconv.ParseUint(casted, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse uint64 from string: %w", err)
		}
		return uintVal, nil
	default:
		return 0, fmt.Errorf("cannot cast %T to uint64", v)
	}
}

func ConvertNumberToFloat32(v any) (float32, error) {
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

func ConvertNumberToFloat64(v any) (float64, error) {
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
		floatVal, err := strconv.ParseFloat(casted, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse float64 from string: %w", err)
		}
		return floatVal, nil
	default:
		return 0, fmt.Errorf("cannot cast %T to float64", v)
	}
}

func ConvertToBool(v any) (bool, error) {
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
	case int8:
		return casted != 0, nil
	case uint8:
		return casted != 0, nil
	default:
		return false, fmt.Errorf("cannot cast %T to bool", v)
	}
}

func ConvertToString(v any) (string, error) {
	switch v := v.(type) {
	case string:
		return v, nil
	case int:
		return strconv.Itoa(v), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(v), nil
	default:
		wrapped := fferr.NewTypeError(string(String), v, nil)
		return "", wrapped
	}
}

func ConvertDatetime(v any) (time.Time, error) {
	switch x := v.(type) {
	case time.Time:
		return x.UTC(), nil
	case string:
		dt, err := dateparse.ParseIn(x, time.UTC)
		if err != nil {
			wrapped := fferr.NewTypeError(Datetime.String(), v, err)
			return time.Time{}, wrapped
		}
		return dt.UTC(), nil
	case int, int32, int64, float32, float64:
		unixTime, err := ConvertNumberToInt64(x)
		if err != nil {
			wrapped := fferr.NewTypeError(Datetime.String(), v, err)
			return time.Time{}, wrapped
		}
		return time.Unix(unixTime, 0).UTC(), nil
	default:
		wrapped := fferr.NewTypeError(Datetime.String(), v, nil)
		return time.Time{}, wrapped
	}
}
