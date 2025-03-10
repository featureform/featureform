package types

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/araddon/dateparse"

	"github.com/featureform/fferr"
)

type GenericSerializer struct{}

func (ser GenericSerializer) Serialize(t ValueType, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if !t.IsVector() {
		return ser.serializeScalar(t, value)
	} else {
		return ser.serializeVector(t, value)
	}
}

func (ser GenericSerializer) serializeVector(t ValueType, value any) (any, error) {
	vecT := t.(VectorType)
	scalar := vecT.Scalar()

	list := reflect.ValueOf(value)
	if list.Kind() != reflect.Slice {
		wrapped := fferr.NewTypeError(vecT.String(), value, nil)
		return nil, wrapped
	}

	length := list.Len()
	if int32(length) != vecT.Dimension {
		errMsg := "Type error. Wrong length.\nFound %d\nExpected %d"
		wrapped := fferr.NewTypeErrorf(vecT.String(), value, errMsg, vecT.Dimension, length)
		return nil, wrapped
	}

	vals := make([]any, length)
	for i := 0; i < length; i++ {
		elem := list.Index(i).Interface()
		val, err := ser.serializeScalar(scalar, elem)
		if err != nil {
			if typed, ok := err.(fferr.Error); ok {
				typed.AddDetail("list_element", strconv.Itoa(i))
			}
			return nil, err
		}
		vals[i] = val
	}
	return vals, nil
}

func (ser GenericSerializer) serializeScalar(t ValueType, value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch t {
	case Int:
		intVal, err := CastNumberToInt(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			return nil, wrapped
		}
		return intVal, nil
	case Int32:
		intVal, err := CastNumberToInt32(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			return nil, wrapped
		}
		return intVal, nil
	case Int64:
		intVal, err := CastNumberToInt64(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			return nil, wrapped
		}
		return intVal, nil
	case Float32:
		floatVal, err := CastNumberToFloat32(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			return nil, wrapped
		}
		return floatVal, nil
	case Float64:
		floatVal, err := CastNumberToFloat64(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			return nil, wrapped
		}
		return floatVal, nil
	case Bool:
		casted, err := CastBool(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			return nil, wrapped
		}
		return casted, nil
	case String:
		casted, ok := value.(string)
		if !ok {
			// Try to convert to string if possible
			switch v := value.(type) {
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
				wrapped := fferr.NewTypeError(t.String(), value, nil)
				return nil, wrapped
			}
		}
		return casted, nil
	case Timestamp, Datetime:
		ts, isTs := value.(time.Time)
		if isTs {
			return ts.UTC(), nil
		}
		unixTime, unixTimeErr := CastNumberToInt64(value)
		isUnixTs := unixTimeErr == nil
		if isUnixTs {
			return time.Unix(unixTime, 0).UTC(), nil
		}
		strForm, isString := value.(string)
		if !isString {
			wrapped := fferr.NewTypeError(t.String(), value, nil)
			return nil, wrapped
		}
		// If timezone is ambiguous, this makes it UTC
		dt, err := dateparse.ParseIn(strForm, time.UTC)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			return nil, wrapped
		}
		return dt.UTC(), nil
	default:
		wrapped := fferr.NewInternalErrorf("SQL doesn't support type")
		wrapped.AddDetail("type", SerializeType(t))
		return nil, wrapped
	}
}

// Deserialize converts a SQL value to a Go value based on the specified type
func (ser GenericSerializer) Deserialize(t ValueType, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if !t.IsVector() {
		return deserializeScalar(t, value)
	}

	// Handle vector/list types
	list, ok := value.([]any)
	if !ok {
		wrapped := fferr.NewInternalErrorf("unable to deserialize SQL value into list, is %T", value)
		return nil, wrapped
	}

	dims := t.(VectorType).Dimension
	if len(list) != int(dims) {
		msg := "unable to deserialize SQL value into list, wrong size %d. Expected %d"
		wrapped := fferr.NewInternalErrorf(msg, len(list), dims)
		return nil, wrapped
	}

	scalar := t.Scalar()
	switch scalar {
	case Int:
		return deserializeList[int](scalar, list)
	case Int32:
		return deserializeList[int32](scalar, list)
	case Int64:
		return deserializeList[int64](scalar, list)
	case Float32:
		return deserializeList[float32](scalar, list)
	case Float64:
		return deserializeList[float64](scalar, list)
	case Bool:
		return deserializeList[bool](scalar, list)
	case String:
		return deserializeList[string](scalar, list)
	default:
		wrapped := fferr.NewInternalErrorf("SQL doesn't support type")
		wrapped.AddDetail("type", SerializeType(t))
		return nil, wrapped
	}
}

func deserializeList[T any](scalar ScalarType, values []any) ([]T, error) {
	deserList := make([]T, len(values))
	for i, value := range values {
		deser, err := deserializeScalar(scalar, value)
		if err != nil {
			if typed, ok := err.(fferr.Error); ok {
				typed.AddDetail("list_element", strconv.Itoa(i))
			}
			return nil, err
		}
		casted, ok := deser.(T)
		if !ok {
			wrapped := fferr.NewInternalErrorf("Deserialize failed due to wrong generic")
			wrapped.AddDetail("found_type", fmt.Sprintf("%T", deser))
			wrapped.AddDetail("expected_type", scalar.String())
			wrapped.AddDetail("list_element", strconv.Itoa(i))
			return nil, wrapped
		}
		deserList[i] = casted
	}
	return deserList, nil
}

func deserializeScalar(t ValueType, value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	// Handle SQL specific conversions
	switch t {
	case Int:
		switch v := value.(type) {
		case int:
			return v, nil
		case int32:
			return int(v), nil
		case int64:
			return int(v), nil
		case float32:
			return int(v), nil
		case float64:
			return int(v), nil
		case string:
			val, err := strconv.ParseInt(v, 10, 0)
			if err != nil {
				wrapped := fferr.NewInternalErrorf("unable to deserialize string value into int: %s", err)
				return nil, wrapped
			}
			return int(val), nil
		default:
			wrapped := fferr.NewInternalErrorf("unable to deserialize SQL value into int, is %T", value)
			return nil, wrapped
		}
	case Int32:
		switch v := value.(type) {
		case int:
			return int32(v), nil
		case int32:
			return v, nil
		case int64:
			return int32(v), nil
		case float32:
			return int32(v), nil
		case float64:
			return int32(v), nil
		case string:
			val, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				wrapped := fferr.NewInternalErrorf("unable to deserialize string value into int32: %s", err)
				return nil, wrapped
			}
			return int32(val), nil
		default:
			wrapped := fferr.NewInternalErrorf("unable to deserialize SQL value into int32, is %T", value)
			return nil, wrapped
		}
	case Int64:
		switch v := value.(type) {
		case int:
			return int64(v), nil
		case int32:
			return int64(v), nil
		case int64:
			return v, nil
		case float32:
			return int64(v), nil
		case float64:
			return int64(v), nil
		case string:
			val, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				wrapped := fferr.NewInternalErrorf("unable to deserialize string value into int64: %s", err)
				return nil, wrapped
			}
			return val, nil
		default:
			wrapped := fferr.NewInternalErrorf("unable to deserialize SQL value into int64, is %T", value)
			return nil, wrapped
		}
	case Float32:
		switch v := value.(type) {
		case int:
			return float32(v), nil
		case int32:
			return float32(v), nil
		case int64:
			return float32(v), nil
		case float32:
			return v, nil
		case float64:
			return float32(v), nil
		case string:
			val, err := strconv.ParseFloat(v, 32)
			if err != nil {
				wrapped := fferr.NewInternalErrorf("unable to deserialize string value into float32: %s", err)
				return nil, wrapped
			}
			return float32(val), nil
		default:
			wrapped := fferr.NewInternalErrorf("unable to deserialize SQL value into float32, is %T", value)
			return nil, wrapped
		}
	case Float64:
		switch v := value.(type) {
		case int:
			return float64(v), nil
		case int32:
			return float64(v), nil
		case int64:
			return float64(v), nil
		case float32:
			return float64(v), nil
		case float64:
			return v, nil
		case string:
			val, err := strconv.ParseFloat(v, 64)
			if err != nil {
				wrapped := fferr.NewInternalErrorf("unable to deserialize string value into float64: %s", err)
				return nil, wrapped
			}
			return val, nil
		default:
			wrapped := fferr.NewInternalErrorf("unable to deserialize SQL value into float64, is %T", value)
			return nil, wrapped
		}
	case Bool:
		switch v := value.(type) {
		case bool:
			return v, nil
		case string:
			val, err := strconv.ParseBool(v)
			if err != nil {
				wrapped := fferr.NewInternalErrorf("unable to deserialize string value into bool: %s", err)
				return nil, wrapped
			}
			return val, nil
		default:
			wrapped := fferr.NewInternalErrorf("unable to deserialize SQL value into bool, is %T", value)
			return nil, wrapped
		}
	case String:
		switch v := value.(type) {
		case string:
			return v, nil
		case int, int32, int64, float32, float64, bool:
			return fmt.Sprintf("%v", v), nil
		default:
			wrapped := fferr.NewInternalErrorf("unable to deserialize SQL value into string, is %T", value)
			return nil, wrapped
		}
	case Timestamp, Datetime:
		switch v := value.(type) {
		case time.Time:
			return v.UTC(), nil
		case string:
			dt, err := dateparse.ParseIn(v, time.UTC)
			if err != nil {
				wrapped := fferr.NewInternalErrorf("unable to deserialize string value into timestamp: %s", err)
				return nil, wrapped
			}
			return dt.UTC(), nil
		case int:
			return time.Unix(int64(v), 0).UTC(), nil
		case int64:
			return time.Unix(v, 0).UTC(), nil
		default:
			wrapped := fferr.NewInternalErrorf("unable to deserialize SQL value into timestamp, is %T", value)
			return nil, wrapped
		}
	default:
		wrapped := fferr.NewInternalErrorf("SQL doesn't support type")
		return nil, wrapped
	}
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
