package clickhouse

import (
	"fmt"
	"math"
	"time"

	"github.com/araddon/dateparse"

	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/provider/provider_type"
)

// SupportedTypes is a set of all supported ClickHouse types
// NOTE: If you add a new type here, make sure to add a corresponding conversion function in ConvertValue
var SupportedTypes = map[types.NativeType]bool{
	// Integer types
	"Int8":   true,
	"Int16":  true,
	"Int32":  true,
	"Int64":  true,
	"UInt8":  true,
	"UInt16": true,
	"UInt32": true,
	"UInt64": true,

	// Floating point types
	"Float32": true,
	"Float64": true,

	// String types
	"String":      true,
	"FixedString": true,

	// Boolean type (ClickHouse does not have native bool, usually UInt8 is used)
	"Bool": true,

	// Date/Time types
	"Date":       true,
	"DateTime":   true,
	"DateTime64": true,
}

var chConverter = Converter{}

func init() {
	Register()
}

func Register() {
	logging.GlobalLogger.Info("Registering ClickHouse converter")
	provider_type.RegisterConverter(provider_type.ClickHouseOffline, chConverter)
}

type Converter struct{}

func (c Converter) IsSupportedType(nativeType types.NativeType) bool {
	_, exists := SupportedTypes[nativeType]
	return exists
}

// ConvertValue converts a value from its ClickHouse representation to a types.Value
func (c Converter) ConvertValue(nativeType types.NativeType, value any) (types.Value, error) {
	// Handle nil values
	if value == nil {
		return types.Value{
			NativeType: nativeType,
			Type:       nil,
			Value:      nil,
		}, nil
	}

	// Check if the type is supported
	if !c.IsSupportedType(nativeType) {
		return types.Value{}, fferr.NewInternalErrorf("Unsupported ClickHouse type: %s", nativeType)
	}

	// Convert the value based on the native type
	switch nativeType {
	// Integer types (int32 group)
	case "Int8", "Int16", "Int32", "UInt8", "UInt16":
		var convertedValue int32
		var err error

		switch v := value.(type) {
		case int8:
			convertedValue = int32(v)
		case int16:
			convertedValue = int32(v)
		case int32:
			convertedValue = v
		case int64:
			if v >= math.MinInt32 && v <= math.MaxInt32 {
				convertedValue = int32(v)
			} else {
				err = fmt.Errorf("int64 value %d out of range for int32", v)
			}
		case uint8:
			convertedValue = int32(v)
		case uint16:
			convertedValue = int32(v)
		case int:
			if v >= math.MinInt32 && v <= math.MaxInt32 {
				convertedValue = int32(v)
			} else {
				err = fmt.Errorf("int value %d out of range for int32", v)
			}
		default:
			err = fmt.Errorf("cannot convert %T to int32", value)
		}

		if err != nil {
			return types.Value{}, fferr.NewTypeError(string(nativeType), value, err)
		}

		return types.Value{
			NativeType: nativeType,
			Type:       types.Int32,
			Value:      convertedValue,
		}, nil

	// Integer types (int64 group)
	case "Int64", "UInt32", "UInt64":
		var convertedValue int64
		var err error

		switch v := value.(type) {
		case int64:
			convertedValue = v
		case uint32:
			convertedValue = int64(v)
		case uint64:
			if v <= math.MaxInt64 {
				convertedValue = int64(v)
			} else {
				err = fmt.Errorf("uint64 value %d out of range for int64", v)
			}
		case int:
			convertedValue = int64(v)
		default:
			err = fmt.Errorf("cannot convert %T to int64", value)
		}

		if err != nil {
			return types.Value{}, fferr.NewTypeError(string(nativeType), value, err)
		}

		return types.Value{
			NativeType: nativeType,
			Type:       types.Int64,
			Value:      convertedValue,
		}, nil

	// Floating point types
	case "Float32":
		var convertedValue float32
		var err error

		switch v := value.(type) {
		case float32:
			convertedValue = v
		case float64:
			if v >= -math.MaxFloat32 && v <= math.MaxFloat32 {
				convertedValue = float32(v)
			} else {
				err = fmt.Errorf("float64 value %f out of range for float32", v)
			}
		default:
			err = fmt.Errorf("cannot convert %T to float32", value)
		}

		if err != nil {
			return types.Value{}, fferr.NewTypeError(string(nativeType), value, err)
		}

		return types.Value{
			NativeType: nativeType,
			Type:       types.Float32,
			Value:      convertedValue,
		}, nil

	case "Float64":
		var convertedValue float64
		var err error

		switch v := value.(type) {
		case float64:
			convertedValue = v
		case float32:
			convertedValue = float64(v)
		default:
			err = fmt.Errorf("cannot convert %T to float64", value)
		}

		if err != nil {
			return types.Value{}, fferr.NewTypeError(string(nativeType), value, err)
		}

		return types.Value{
			NativeType: nativeType,
			Type:       types.Float64,
			Value:      convertedValue,
		}, nil

	// String types
	case "String", "FixedString":
		var convertedValue string

		// Try to convert to string
		switch v := value.(type) {
		case string:
			convertedValue = v
		case []byte:
			convertedValue = string(v)
		default:
			// For other types, use fmt.Sprintf
			convertedValue = fmt.Sprintf("%v", value)
		}

		return types.Value{
			NativeType: nativeType,
			Type:       types.String,
			Value:      convertedValue,
		}, nil

	// Boolean type
	case "Bool":
		converted, err := types.ConvertToBool(value)

		if err != nil {
			return types.Value{}, fferr.NewTypeError(string(nativeType), value, err)
		}

		return types.Value{
			NativeType: nativeType,
			Type:       types.Bool,
			Value:      converted,
		}, nil

	// Date/Time types
	case "Date", "DateTime", "DateTime64":
		var convertedValue time.Time
		var err error

		switch v := value.(type) {
		case time.Time:
			convertedValue = v.UTC()
		case string:
			parsed, parseErr := dateparse.ParseIn(v, time.UTC)
			if parseErr != nil {
				err = parseErr
			} else {
				convertedValue = parsed.UTC()
			}
		case int64:
			convertedValue = time.Unix(v, 0).UTC()
		case int:
			convertedValue = time.Unix(int64(v), 0).UTC()
		default:
			err = fmt.Errorf("cannot convert %T to time.Time", value)
		}

		if err != nil {
			return types.Value{}, fferr.NewTypeError(string(nativeType), value, err)
		}

		return types.Value{
			NativeType: nativeType,
			Type:       types.Datetime,
			Value:      convertedValue,
		}, nil

	default:
		// This should never happen since we check IsSupportedType above
		return types.Value{}, fferr.NewInternalErrorf("Unsupported ClickHouse type: %s", nativeType)
	}
}
