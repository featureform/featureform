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
	"reflect"
	"testing"

	"github.com/featureform/types"

	arrowlib "github.com/apache/arrow/go/v18/arrow"
)

func TestConvertSchema(t *testing.T) {
	const expectedNumFields = 9

	fields := []arrowlib.Field{
		{Name: "TransactionID", Type: &arrowlib.LargeStringType{}},
		{Name: "TransactionAmount", Type: arrowlib.PrimitiveTypes.Float64},
		{Name: "Timestamp", Type: arrowlib.FixedWidthTypes.Timestamp_ns},
		{Name: "IsFraud", Type: arrowlib.FixedWidthTypes.Boolean},
		{Name: "UserAge", Type: arrowlib.PrimitiveTypes.Int32},
		{Name: "SignupDate", Type: arrowlib.FixedWidthTypes.Date32},
		{Name: "AccountBalance", Type: &arrowlib.Decimal128Type{38, 2}},
		{Name: "FeatureVector", Type: &arrowlib.FixedSizeBinaryType{16}},
		{Name: "Float64Vec4", Type: arrowlib.FixedSizeListOf(4, arrowlib.PrimitiveTypes.Float64)},
	}
	arrowSchema := arrowlib.NewSchema(fields, nil)
	ffSchema, err := ConvertSchema(arrowSchema)
	if err != nil {
		t.Fatalf("Failed to convert schema: %s", err)
	}
	if len(ffSchema.Fields) != expectedNumFields {
		t.Fatalf("expected %d fields, got %d", expectedNumFields, len(ffSchema.Fields))
	}
	wantFields := []types.ColumnSchema{
		{
			Name:       "TransactionID",
			NativeType: "large_utf8",
			Type:       types.String,
		},
		{
			Name:       "TransactionAmount",
			NativeType: "float64",
			Type:       types.Float64,
		},
		{
			Name:       "Timestamp",
			NativeType: "timestamp",
			Type:       types.Timestamp,
		},
		{
			Name:       "IsFraud",
			NativeType: "bool",
			Type:       types.Bool,
		},
		{
			Name:       "UserAge",
			NativeType: "int32",
			Type:       types.Int32,
		},
		{
			Name:       "SignupDate",
			NativeType: "date32",
			Type:       types.Timestamp,
		},
		{
			Name:       "AccountBalance",
			NativeType: "decimal",
			Type:       types.Unknown,
		},
		{
			Name:       "FeatureVector",
			NativeType: "fixed_size_binary",
			Type:       types.String,
		},
		{
			Name:       "Float64Vec4",
			NativeType: "fixed_size_list<float64, 4>",
			Type: types.VectorType{
				ScalarType: types.Float64,
				Dimension:  4,
			},
		},
	}
	for i := range wantFields {
		if !reflect.DeepEqual(ffSchema.Fields[i], wantFields[i]) {
			t.Errorf("Fields mismatch.\nGot:\n%#v\nWant:\n%#v",
				ffSchema.Fields[i], wantFields[i])
		}
	}
}
