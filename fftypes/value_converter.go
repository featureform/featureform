package types

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/araddon/dateparse"

	"github.com/featureform/fferr"
)

type ValueConverter[T any] interface {
	GetType(nativeType NativeType) (ValueType, error)
	ConvertValue(nativeType NativeType, value T) (Value, error)
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
		return strconv.ParseInt(casted, 10, 64)
	default:
		return 0, fmt.Errorf("cannot cast %T to int64", v)
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
		return strconv.ParseFloat(casted, 64)
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

func ConvertVectorValue(value any) (ScalarType, any, error) {
	// Step 1: Handle JSON string
	if strVal, ok := value.(string); ok {
		var jsonArray []any
		if err := json.Unmarshal([]byte(strVal), &jsonArray); err != nil {
			return Unknown, nil, fferr.NewInternalErrorf("Failed to parse array JSON: %v", err)
		}
		value = jsonArray
	}

	// Step 2: Process the array as a slice
	list := reflect.ValueOf(value)
	if list.Kind() != reflect.Slice {
		err := fferr.NewTypeError("vector", value, nil)
		return Unknown, nil, err
	}

	length := list.Len()
	vals := make([]any, length)
	for i := 0; i < length; i++ {
		vals[i] = list.Index(i).Interface()
	}

	// Step 3: Infer scalar type
	var scalarType ScalarType
	for i := 0; i < length; i++ {
		elem := vals[i]
		if elem != nil {
			scalarType = inferScalarType(elem)
			break
		}
	}

	if scalarType == "" || scalarType == Unknown {
		err := fferr.NewInternalErrorf("Type conversion failed due to unknown scalar type")
		return Unknown, nil, err
	}

	// Step 4: Convert to a typed slice based on inferred scalar type
	var converted any
	var err error
	switch scalarType {
	case Int:
		converted, err = convertList[int](scalarType, vals)
	case Int32:
		converted, err = convertList[int32](scalarType, vals)
	case Int64:
		converted, err = convertList[int64](scalarType, vals)
	case Float32:
		converted, err = convertList[float32](scalarType, vals)
	case Float64:
		converted, err = convertList[float64](scalarType, vals)
	case Bool:
		converted, err = convertList[bool](scalarType, vals)
	case String:
		converted, err = convertList[string](scalarType, vals)
	default:
		err := fferr.NewInternalErrorf("Type conversion not supported for inferred vector type")
		err.AddDetail("inferred_type", fmt.Sprintf("%v", scalarType))
		return Unknown, nil, err
	}

	if err != nil {
		return Unknown, nil, err
	}

	return scalarType, converted, nil
}

func convertList[T any](scalar ScalarType, values []any) ([]T, error) {
	result := make([]T, len(values))
	for i, value := range values {
		casted, ok := value.(T)
		if !ok {
			wrapped := fferr.NewInternalErrorf("Type conversion failed due to wrong type")
			wrapped.AddDetail("found_type", fmt.Sprintf("%T", value))
			wrapped.AddDetail("expected_type", scalar.String())
			wrapped.AddDetail("list_element", strconv.Itoa(i))
			return nil, wrapped
		}
		result[i] = casted
	}
	return result, nil
}

func inferScalarType(value any) ScalarType {
	switch value.(type) {
	case int:
		return Int
	case int32:
		return Int32
	case int64:
		return Int64
	case float32:
		return Float32
	case float64:
		return Float64
	case bool:
		return Bool
	case string:
		return String
	default:
		return Unknown
	}
}
