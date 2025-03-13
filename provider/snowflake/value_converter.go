package snowflake

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/araddon/dateparse"

	"github.com/featureform/fferr"
	. "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/provider/provider_type"
)

var sfConverter = Converter{TypeMap: TypeMap}

func init() {
	Register()
}

func Register() {
	logging.GlobalLogger.Info("Registering Snowflake converter")
	provider_type.RegisterConverter(provider_type.SnowflakeOffline, Converter{TypeMap: TypeMap})
}

type Converter struct {
	TypeMap NativeToValueTypeMapper
}

func (c Converter) GetTypeMap() NativeToValueTypeMapper {
	return c.TypeMap
}

func (c Converter) ConvertValue(nativeType NativeType, value any) (Value, error) {
	if value == nil {
		return Value{
			NativeType: nativeType,
			Type:       nil,
			Value:      nil,
		}, nil
	}

	vType, exists := TypeMap[nativeType]
	if !exists {
		wrapped := fferr.NewInternalErrorf("Unknown native type: %v", nativeType)
		return Value{}, wrapped
	}

	var converted any
	var err error
	if !vType.IsVector() {
		converted, err = convertScalar(vType, value)
	} else {
		converted, err = convertVector(vType, value)
	}
	if err != nil {
		return Value{}, err
	}

	return Value{
		NativeType: nativeType,
		Type:       vType,
		Value:      converted,
	}, nil
}

func convertVector(t ValueType, value any) (any, error) {
	vecT := t.(VectorType)
	scalar := vecT.Scalar()

	list := reflect.ValueOf(value)
	if list.Kind() != reflect.Slice {
		wrapped := fferr.NewTypeError(vecT.String(), value, nil)
		return nil, wrapped
	}

	length := list.Len()
	if vecT.Dimension != 0 && int32(length) != vecT.Dimension {
		errMsg := "Type error. Wrong length.\nFound %d\nExpected %d"
		wrapped := fferr.NewTypeErrorf(vecT.String(), value, errMsg, vecT.Dimension, length)
		return nil, wrapped
	}

	// Try to infer scalar type if it's unknown
	if scalar == "" || scalar == "UNKNOWN" {
		for i := 0; i < length; i++ {
			elem := list.Index(i).Interface()
			if elem != nil {
				scalar = inferScalarType(elem)
				break
			}
		}
	}

	if scalar == "UNKNOWN" {
		wrapped := fferr.NewInternalErrorf("Unable to infer scalar type for vector")
		wrapped.AddDetail("vector", fmt.Sprintf("%v", value))
		return nil, wrapped
	}

	vals := make([]any, length)
	for i := 0; i < length; i++ {
		elem := list.Index(i).Interface()
		val, err := convertScalar(scalar, elem)
		if err != nil {
			if typed, ok := err.(fferr.Error); ok {
				typed.AddDetail("list_element", strconv.Itoa(i))
			}
			return nil, err
		}
		vals[i] = val
	}

	// Convert to a typed slice based on inferred scalar type
	switch scalar {
	case Int:
		return convertList[int](scalar, vals)
	case Int32:
		return convertList[int32](scalar, vals)
	case Int64:
		return convertList[int64](scalar, vals)
	case Float32:
		return convertList[float32](scalar, vals)
	case Float64:
		return convertList[float64](scalar, vals)
	case Bool:
		return convertList[bool](scalar, vals)
	case String:
		return convertList[string](scalar, vals)
	default:
		wrapped := fferr.NewInternalErrorf("Type conversion not supported for inferred vector type")
		wrapped.AddDetail("inferred_type", fmt.Sprintf("%v", scalar))
		return nil, wrapped
	}
}

// inferScalarType attempts to determine the scalar type of a value
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
		return "UNKNOWN"
	}
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

func convertScalar(t ValueType, value any) (any, error) {
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
		// Try to convert to string if possible
		switch v := value.(type) {
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
			wrapped := fferr.NewTypeError(t.String(), value, nil)
			return nil, wrapped
		}
	case Timestamp, Datetime:
		// Handle timestamp conversions
		switch v := value.(type) {
		case time.Time:
			return v.UTC(), nil
		case string:
			dt, err := dateparse.ParseIn(v, time.UTC)
			if err != nil {
				wrapped := fferr.NewTypeError(t.String(), value, err)
				return nil, wrapped
			}
			return dt.UTC(), nil
		case int, int32, int64, float32, float64:
			unixTime, err := CastNumberToInt64(v)
			if err != nil {
				wrapped := fferr.NewTypeError(t.String(), value, err)
				return nil, wrapped
			}
			return time.Unix(unixTime, 0).UTC(), nil
		default:
			wrapped := fferr.NewTypeError(t.String(), value, nil)
			return nil, wrapped
		}
	default:
		wrapped := fferr.NewInternalErrorf("Type conversion not supported")
		wrapped.AddDetail("type", fmt.Sprintf("%v", t))
		return nil, wrapped
	}
}
