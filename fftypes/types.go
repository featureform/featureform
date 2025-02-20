package fftypes

import (
	"fmt"
	"strconv"
)

type FFType string

// todo: just handle the basic types for now
const (
	FF_INT       FFType = "FF_INT"
	FF_STRING    FFType = "FF_STRING"
	FF_FLOAT     FFType = "FF_FLOAT"
	FF_BOOL      FFType = "FF_BOOL"
	FF_DECIMAL   FFType = "FF_DECIMAL"
	FF_DATE      FFType = "FF_DATE"
	FF_TIME      FFType = "FF_TIME"
	FF_TIMESTAMP FFType = "FF_TIMESTAMP"
)

// todo: this can be the exact types from the dependent library.
type IcebergType string

const (
	ICEBERG_INT         IcebergType = "int"
	ICEBERG_INT32       IcebergType = "int32"
	ICEBERG_STRING      IcebergType = "string"
	ICEBERG_STRING_UTF8 IcebergType = "utf8"
	ICEBERG_FLOAT       IcebergType = "float"
	ICEBERG_BOOL        IcebergType = "bool"
	ICEBERG_DECIMAL     IcebergType = "decimal"
	ICEBERG_DATE        IcebergType = "date"
	ICEBERG_TIME        IcebergType = "time"
	ICEBERG_TIMESTAMP   IcebergType = "timestamp"
)

// Iceberg -> to FFTypes. precision could be an issue here with decimals
var IcebergToFeatureform = map[IcebergType]FFType{
	ICEBERG_INT:         FF_INT,
	ICEBERG_INT32:       FF_INT,
	ICEBERG_STRING:      FF_STRING,
	ICEBERG_STRING_UTF8: FF_STRING,
	ICEBERG_FLOAT:       FF_FLOAT,
	ICEBERG_BOOL:        FF_BOOL,
	ICEBERG_DECIMAL:     FF_DECIMAL,
	ICEBERG_DATE:        FF_DATE,
	ICEBERG_TIME:        FF_TIME,
	ICEBERG_TIMESTAMP:   FF_TIMESTAMP,
}

// FFTypes -> Iceberg
var FeatureformToIceberg = map[FFType]IcebergType{
	FF_INT:       ICEBERG_INT,
	FF_STRING:    ICEBERG_STRING,
	FF_FLOAT:     ICEBERG_FLOAT,
	FF_BOOL:      ICEBERG_BOOL,
	FF_DECIMAL:   ICEBERG_DECIMAL,
	FF_DATE:      ICEBERG_DATE,
	FF_TIME:      ICEBERG_TIME,
	FF_TIMESTAMP: ICEBERG_TIMESTAMP,
}

// this struct represents a combination of what a value that can
// be used across our entire go stack. It houses the various types, and a value
// ideally we also include helper functions to accomodate their changes better.
type FeatureformValue struct {
	Provider     string      // specifies which provider this value originats from
	FFType       FFType      // our internal type
	ProviderType string      // the original provider type ("int", "string") but should be arrow type?
	Value        interface{} // the actual stored value itself
}

// todox: the following can be part of the struct's behavior.
func ConvertIcebergToFeatureform(icebergType IcebergType) (FFType, error) {
	if ffType, exists := IcebergToFeatureform[icebergType]; exists {
		return ffType, nil
	}
	return "", fmt.Errorf("unknown Iceberg type: %s", icebergType)
}

func ConvertFeatureformToIceberg(ffType FFType) (IcebergType, error) {
	if icebergType, exists := FeatureformToIceberg[ffType]; exists {
		return icebergType, nil
	}
	return "", fmt.Errorf("unknown Featureform type: %s", ffType)
}

func (fv FeatureformValue) ToString() string {
	if fv.Value == nil {
		return ""
	}
	switch v := fv.Value.(type) {
	case string:
		return v
	case int, int64, float64, bool:
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf("unsupported type: %T", v)
	}
}

func (fv FeatureformValue) ToInt() (int, error) {
	switch v := fv.Value.(type) {
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case string:
		return strconv.Atoi(v)
	default:
		return 0, fmt.Errorf("cannot convert %T to int", v)
	}
}

func (fv FeatureformValue) ToFloat() (float64, error) {
	switch v := fv.Value.(type) {
	case float64:
		return v, nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float", v)
	}
}

func (fv FeatureformValue) ToBool() (bool, error) {
	switch v := fv.Value.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(v)
	default:
		return false, fmt.Errorf("cannot convert %T to bool", v)
	}
}

// stand in helper function. will prob create one for each type of provider
func NewFeatureformValue(provider, providerType string, value interface{}) FeatureformValue {
	ffType, err := ConvertIcebergToFeatureform(IcebergType(providerType))
	if err != nil {
		fmt.Println("there was an error converting from iceberg to featureform: ", err)
		ffType = FF_STRING
	}
	return FeatureformValue{
		Provider:     provider,
		FFType:       ffType,
		ProviderType: providerType,
		Value:        value,
	}
}
