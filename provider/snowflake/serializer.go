package snowflake

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/araddon/dateparse"

	"github.com/featureform/fferr"
	vt "github.com/featureform/fftypes"
)

const serializeSnowflakeV1 = "snowflake-v1"

type Serializer struct{}

func (ser Serializer) Version() string {
	return serializeSnowflakeV1
}

// Serialize converts a Go value to a Snowflake value based on the specified type
func (ser Serializer) Serialize(t vt.ValueType, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if !t.IsVector() {
		return ser.serializeScalar(t, value)
	} else {
		return ser.serializeVector(t, value)
	}
}

func (ser Serializer) serializeVector(t vt.ValueType, value any) (any, error) {
	vecT := t.(vt.VectorType)
	scalar := vecT.Scalar()

	list := reflect.ValueOf(value)
	if list.Kind() != reflect.Slice {
		wrapped := fferr.NewTypeError(vecT.String(), value, nil)
		wrapped.AddDetail("version", ser.Version())
		return nil, wrapped
	}

	length := list.Len()
	if int32(length) != vecT.Dimension {
		errMsg := "Type error. Wrong length.\nFound %d\nExpected %d"
		wrapped := fferr.NewTypeErrorf(vecT.String(), value, errMsg, vecT.Dimension, length)
		wrapped.AddDetail("version", ser.Version())
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

func (ser Serializer) serializeScalar(t vt.ValueType, value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch t {
	//case vt.NilType:
	//	return nil, nil
	case vt.Int:
		intVal, err := CastNumberToInt(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			wrapped.AddDetail("version", ser.Version())
			return nil, wrapped
		}
		return intVal, nil
	case vt.Int32:
		intVal, err := CastNumberToInt32(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			wrapped.AddDetail("version", ser.Version())
			return nil, wrapped
		}
		return intVal, nil
	case vt.Int64:
		intVal, err := CastNumberToInt64(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			wrapped.AddDetail("version", ser.Version())
			return nil, wrapped
		}
		return intVal, nil
	case vt.Float32:
		floatVal, err := CastNumberToFloat32(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			wrapped.AddDetail("version", ser.Version())
			return nil, wrapped
		}
		return floatVal, nil
	case vt.Float64:
		floatVal, err := CastNumberToFloat64(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			wrapped.AddDetail("version", ser.Version())
			return nil, wrapped
		}
		return floatVal, nil
	case vt.Bool:
		casted, err := CastBool(value)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			wrapped.AddDetail("version", ser.Version())
			return nil, wrapped
		}
		return casted, nil
	case vt.String:
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
				wrapped.AddDetail("version", ser.Version())
				return nil, wrapped
			}
		}
		return casted, nil
	case vt.Timestamp, vt.Datetime:
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
			wrapped.AddDetail("version", ser.Version())
			return nil, wrapped
		}
		// If timezone is ambiguous, this makes it UTC
		dt, err := dateparse.ParseIn(strForm, time.UTC)
		if err != nil {
			wrapped := fferr.NewTypeError(t.String(), value, err)
			wrapped.AddDetail("version", ser.Version())
			return nil, wrapped
		}
		return dt.UTC(), nil
	default:
		wrapped := fferr.NewInternalErrorf("Snowflake doesn't support type")
		wrapped.AddDetail("type", vt.SerializeType(t))
		return nil, wrapped
	}
}

// Deserialize converts a Snowflake value to a Go value based on the specified type
func (ser Serializer) Deserialize(t vt.ValueType, value any) (any, error) {
	version := ser.Version()
	if value == nil {
		return nil, nil
	}
	if !t.IsVector() {
		return deserializeScalar(t, value, version)
	}

	// Handle vector/list types
	list, ok := value.([]any)
	if !ok {
		wrapped := fferr.NewInternalErrorf("unable to deserialize Snowflake value into list, is %T", value)
		wrapped.AddDetail("version", version)
		return nil, wrapped
	}

	dims := t.(vt.VectorType).Dimension
	if len(list) != int(dims) {
		msg := "unable to deserialize Snowflake value into list, wrong size %d. Expected %d"
		wrapped := fferr.NewInternalErrorf(msg, len(list), dims)
		wrapped.AddDetail("version", version)
		return nil, wrapped
	}

	scalar := t.Scalar()
	switch scalar {
	case vt.Int:
		return deserializeList[int](scalar, list, version)
	case vt.Int32:
		return deserializeList[int32](scalar, list, version)
	case vt.Int64:
		return deserializeList[int64](scalar, list, version)
	case vt.Float32:
		return deserializeList[float32](scalar, list, version)
	case vt.Float64:
		return deserializeList[float64](scalar, list, version)
	case vt.Bool:
		return deserializeList[bool](scalar, list, version)
	case vt.String:
		return deserializeList[string](scalar, list, version)
	default:
		wrapped := fferr.NewInternalErrorf("Snowflake doesn't support type")
		wrapped.AddDetail("type", vt.SerializeType(t))
		return nil, wrapped
	}
}

func deserializeList[T any](scalar vt.ScalarType, values []any, version string) ([]T, error) {
	deserList := make([]T, len(values))
	for i, value := range values {
		deser, err := deserializeScalar(scalar, value, version)
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
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
		deserList[i] = casted
	}
	return deserList, nil
}

func deserializeScalar(t vt.ValueType, value any, version string) (any, error) {
	if value == nil {
		return nil, nil
	}

	// Handle Snowflake specific conversions
	switch t {
	case vt.Int:
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
				wrapped.AddDetail("version", version)
				return nil, wrapped
			}
			return int(val), nil
		default:
			wrapped := fferr.NewInternalErrorf("unable to deserialize Snowflake value into int, is %T", value)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
	case vt.Int32:
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
				wrapped.AddDetail("version", version)
				return nil, wrapped
			}
			return int32(val), nil
		default:
			wrapped := fferr.NewInternalErrorf("unable to deserialize Snowflake value into int32, is %T", value)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
	case vt.Int64:
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
				wrapped.AddDetail("version", version)
				return nil, wrapped
			}
			return val, nil
		default:
			wrapped := fferr.NewInternalErrorf("unable to deserialize Snowflake value into int64, is %T", value)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
	case vt.Float32:
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
				wrapped.AddDetail("version", version)
				return nil, wrapped
			}
			return float32(val), nil
		default:
			wrapped := fferr.NewInternalErrorf("unable to deserialize Snowflake value into float32, is %T", value)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
	case vt.Float64:
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
				wrapped.AddDetail("version", version)
				return nil, wrapped
			}
			return val, nil
		default:
			wrapped := fferr.NewInternalErrorf("unable to deserialize Snowflake value into float64, is %T", value)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
	case vt.Bool:
		switch v := value.(type) {
		case bool:
			return v, nil
		case string:
			val, err := strconv.ParseBool(v)
			if err != nil {
				wrapped := fferr.NewInternalErrorf("unable to deserialize string value into bool: %s", err)
				wrapped.AddDetail("version", version)
				return nil, wrapped
			}
			return val, nil
		default:
			wrapped := fferr.NewInternalErrorf("unable to deserialize Snowflake value into bool, is %T", value)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
	case vt.String:
		switch v := value.(type) {
		case string:
			return v, nil
		case int, int32, int64, float32, float64, bool:
			return fmt.Sprintf("%v", v), nil
		default:
			wrapped := fferr.NewInternalErrorf("unable to deserialize Snowflake value into string, is %T", value)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
	case vt.Timestamp, vt.Datetime:
		switch v := value.(type) {
		case time.Time:
			return v.UTC(), nil
		case string:
			dt, err := dateparse.ParseIn(v, time.UTC)
			if err != nil {
				wrapped := fferr.NewInternalErrorf("unable to deserialize string value into timestamp: %s", err)
				wrapped.AddDetail("version", version)
				return nil, wrapped
			}
			return dt.UTC(), nil
		case int:
			return time.Unix(int64(v), 0).UTC(), nil
		case int64:
			return time.Unix(v, 0).UTC(), nil
		default:
			wrapped := fferr.NewInternalErrorf("unable to deserialize Snowflake value into timestamp, is %T", value)
			wrapped.AddDetail("version", version)
			return nil, wrapped
		}
	default:
		wrapped := fferr.NewInternalErrorf("Snowflake doesn't support type")
		wrapped.AddDetail("version", version)
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
