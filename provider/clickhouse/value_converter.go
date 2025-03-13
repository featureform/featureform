package clickhouse

import (
	"fmt"
	"math"
	"time"

	"github.com/araddon/dateparse"

	"github.com/featureform/fferr"
	. "github.com/featureform/fftypes"
	"github.com/featureform/provider/provider_type"
)

var clickhouseConverter = Converter{TypeMap: TypeMap}

type Converter struct {
	TypeMap NativeToValueTypeMapper
}

func init() {
	provider_type.RegisterConverter(provider_type.ClickHouseOffline, clickhouseConverter)
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

	converted, err := convertScalar(vType, value)
	if err != nil {
		return Value{}, err
	}

	return Value{
		NativeType: nativeType,
		Type:       vType,
		Value:      converted,
	}, nil
}

func convertScalar(t ValueType, value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch t {
	case Int32:
		return int(value.(int32)), nil
	case Int64:
		if intValue, ok := value.(int64); ok && intValue >= math.MinInt && intValue <= math.MaxInt {
			return int(intValue), nil
		}
		return value.(int64), nil
	case UInt32:
		if intValue, ok := value.(uint32); ok && intValue <= math.MaxInt32 {
			return int(intValue), nil
		}
		return value.(uint32), nil
	case UInt64:
		if intValue, ok := value.(uint64); ok && intValue <= uint64(math.MaxInt) {
			return int(intValue), nil
		}
		return value.(uint64), nil
	case Float32:
		return float32(value.(float64)), nil
	case Float64:
		return value.(float64), nil
	case Bool:
		if v, ok := value.(uint8); ok {
			return v == 1, nil
		}
		return nil, fferr.NewTypeError(t.String(), value, nil)
	case String:
		return fmt.Sprintf("%v", value), nil
	case Datetime:
		switch v := value.(type) {
		case time.Time:
			return v.UTC(), nil
		case string:
			dt, err := dateparse.ParseIn(v, time.UTC)
			if err != nil {
				return nil, fferr.NewTypeError(t.String(), value, err)
			}
			return dt.UTC(), nil
		case int, int32, int64:
			return time.Unix(int64(v.(int)), 0).UTC(), nil
		}
	}

	return nil, fferr.NewInternalErrorf("Unsupported type conversion for %v", t)
}
