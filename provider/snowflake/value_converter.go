// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package snowflake

import (
	"database/sql"

	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
	"github.com/featureform/provider/provider_type"
)

var SfConverter = Converter{}

func init() {
	Register()
}

func Register() {
	logging.GlobalLogger.Info("Registering Snowflake converter")
	provider_type.RegisterConverter(provider_type.SnowflakeOffline, SfConverter)
}

type Converter struct{}

type nativeTypeDetails struct {
	columnName string
	precision  sql.NullInt64
	scale      sql.NullInt64
}

func NewNativeTypeDetails(columnName string, precision, scale sql.NullInt64) types.NativeTypeDetails {
	return &nativeTypeDetails{
		columnName: columnName,
		precision:  precision,
		scale:      scale,
	}
}

func (n nativeTypeDetails) ColumnName() string {
	return n.columnName
}

func (c Converter) ParseNativeType(typeDetails types.NativeTypeDetails) (types.NativeType, error) {
	sfDetails, ok := typeDetails.(*nativeTypeDetails)
	if !ok {
		return nil, fferr.NewInternalErrorf("Invalid type details: %T", typeDetails)
	}

	nativeType, ok := StringToNativeType[sfDetails.ColumnName()]
	if !ok {
		return nil, fferr.NewUnsupportedTypeError("Unsupported native type: " + sfDetails.ColumnName())
	}

	switch nt := nativeType.(type) {
	case *NumberType:
		if !sfDetails.precision.Valid && !sfDetails.scale.Valid {
			return nt, fferr.NewInternalErrorf("Invalid precision/scale for native type: %s", sfDetails.ColumnName())
		}

		nt = nt.WithPrecision(sfDetails.precision.Int64).
			WithScale(sfDetails.scale.Int64)
		return nt, nil
	default:
		return nativeType, nil
	}
}

func (c Converter) GetType(nativeType types.NativeType) (types.ValueType, error) {
	// Handle nil case
	if nativeType == nil {
		return types.String, nil
	}

	if numeric, ok := nativeType.(*NumberType); ok {
		// If scale is 0, this is an integer type
		if numeric.Scale == 0 {
			if numeric.Precision <= 9 {
				return types.Int32, nil
			}
			// All larger integers use Int64, with potential precision loss for values > 2^63-1
			return types.Int64, nil
		}

		// For numbers with decimal places
		if numeric.Precision <= 7 {
			return types.Float32, nil
		}
		return types.Float64, nil
	}

	switch nativeType {
	// Integer types
	case INTEGER, INT, SMALLINT:
		return types.Int32, nil
	case BIGINT:
		return types.Int64, nil

	// Floating point types
	case FLOAT, FLOAT4, REAL:
		return types.Float64, nil
	case FLOAT8, DOUBLE, DOUBLE_PRECISION:
		return types.Float64, nil

	// String types
	case VARCHAR, CHAR, CHARACTER, STRING, TEXT:
		return types.String, nil

	// Boolean type
	case BOOLEAN, BOOL:
		return types.Bool, nil

	// Date/Time types
	case DATE, DATETIME, TIME:
		return types.Datetime, nil
	case TIMESTAMP, TIMESTAMP_LTZ, TIMESTAMP_NTZ, TIMESTAMP_TZ:
		return types.Timestamp, nil
	}

	// For literal types we don't recognize, return an error with the type name
	if typeLiteral, ok := nativeType.(types.NativeTypeLiteral); ok {
		return nil, fferr.NewUnsupportedTypeError(string(typeLiteral))
	}

	// For any other type, return a generic error
	return nil, fferr.NewUnsupportedTypeError("unknown type")
}

// ConvertValue converts a value from its Snowflake representation to a types.Value
func (c Converter) ConvertValue(nativeType types.NativeType, value any) (types.Value, error) {
	// Get the target Featureform type for this native type
	targetType, err := c.GetType(nativeType)
	if err != nil {
		return types.Value{}, err
	}

	// If value is nil, return a nil value of the target type
	if value == nil {
		return types.Value{
			NativeType: nativeType,
			Type:       targetType,
			Value:      nil,
		}, nil
	}

	// Convert the value according to target type
	var convertedValue any
	var convErr error

	switch targetType {
	case types.Int32:
		convertedValue, convErr = types.ConvertNumberToInt32(value)
	case types.Int64:
		convertedValue, convErr = types.ConvertNumberToInt64(value)
	case types.Float32:
		convertedValue, convErr = types.ConvertNumberToFloat32(value)
	case types.Float64:
		convertedValue, convErr = types.ConvertNumberToFloat64(value)
	case types.String:
		convertedValue, convErr = types.ConvertToString(value)
	case types.Bool:
		convertedValue, convErr = types.ConvertToBool(value)
	case types.Timestamp, types.Datetime:
		convertedValue, convErr = types.ConvertDatetime(value)
	default:
		return types.Value{}, fferr.NewUnsupportedTypeError("Unsupported target type")
	}

	if convErr != nil {
		return types.Value{}, fferr.NewInternalErrorf("conversion error: %v", convErr)
	}

	return types.Value{
		NativeType: nativeType,
		Type:       targetType,
		Value:      convertedValue,
	}, nil
}
