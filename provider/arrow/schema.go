// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package arrow

import (
	"fmt"

	"github.com/featureform/types"

	arrowlib "github.com/apache/arrow/go/v18/arrow"
)

var scalarTypeMap = map[string]types.ScalarType{
	"utf8":              types.String,
	"large_utf8":        types.String,
	"fixed_size_binary": types.String,
	"binary":            types.String,
	"bool":              types.Bool,

	"int8":   types.Int8,
	"int16":  types.Int16,
	"int32":  types.Int32,
	"int64":  types.Int64,
	"uint8":  types.UInt8,
	"uint16": types.UInt16,
	"uint32": types.UInt32,
	"uint64": types.UInt64,

	"float32": types.Float32,
	"float64": types.Float64,

	"timestamp": types.Timestamp,
	"date32":    types.Timestamp,
	"date64":    types.Timestamp,
	"time32":    types.Timestamp,
	"time64":    types.Timestamp,
}

func ConvertSchema(schema *arrowlib.Schema) (types.Schema, error) {
	var sch types.Schema

	for _, field := range schema.Fields() {
		nativeType, valType := mapType(field.Type)

		sch.Fields = append(sch.Fields, types.ColumnSchema{
			Name:       types.ColumnName(field.Name),
			NativeType: nativeType,
			Type:       valType,
		})
	}
	return sch, nil
}

func mapType(dt arrowlib.DataType) (types.NativeType, types.ValueType) {
	if fsl, ok := dt.(*arrowlib.FixedSizeListType); ok {
		innerNative, innerType := mapScalarType(fsl.Elem())
		nativeType := types.NativeType(
			fmt.Sprintf("%s<%s, %d>", fsl.Name(), innerNative, fsl.Len()),
		)
		if innerType == types.Unknown {
			return nativeType, types.Unknown
		}
		return nativeType, types.VectorType{
			ScalarType: innerType,
			Dimension:  fsl.Len(),
		}
	}
	return mapScalarType(dt)
}

func mapScalarType(dt arrowlib.DataType) (types.NativeType, types.ScalarType) {
	nativeType := dt.Name()
	valueType, ok := scalarTypeMap[nativeType]
	if !ok {
		valueType = types.Unknown
	}
	return types.NativeType(nativeType), valueType
}
