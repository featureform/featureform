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
	"time"

	"github.com/featureform/types"

	arrowlib "github.com/apache/arrow/go/v18/arrow"
	arrowarr "github.com/apache/arrow/go/v18/arrow/array"
	arrowdec "github.com/apache/arrow/go/v18/arrow/decimal128"
	arrowmem "github.com/apache/arrow/go/v18/arrow/memory"
)

// testRecord holds row data that can be converted into an Arrow Record.
type testRecord struct {
	TransactionID     string
	TransactionAmount float64
	Timestamp         time.Time
	IsFraud           bool
	UserAge           int32
	SignupDays        int32  // Days since epoch
	AccountBalance    int64  // For decimal scale=2
	FeatureVector     []byte // Exactly 16 bytes
	Float64Vec4       []float64
}

// Make a schema with your fields.
func newTestSchema() *arrowlib.Schema {
	tsType := &arrowlib.TimestampType{
		Unit:     arrowlib.Nanosecond,
		TimeZone: "",
	}
	fields := []arrowlib.Field{
		{Name: "TransactionID", Type: &arrowlib.LargeStringType{}},
		{Name: "TransactionAmount", Type: arrowlib.PrimitiveTypes.Float64},
		{Name: "Timestamp", Type: tsType},
		{Name: "IsFraud", Type: arrowlib.FixedWidthTypes.Boolean},
		{Name: "UserAge", Type: arrowlib.PrimitiveTypes.Int32},
		{Name: "SignupDate", Type: arrowlib.FixedWidthTypes.Date32},
		{Name: "AccountBalance", Type: &arrowlib.Decimal128Type{Precision: 38, Scale: 2}},
		{Name: "FeatureVector", Type: &arrowlib.FixedSizeBinaryType{ByteWidth: 16}},
		{Name: "Float64Vec4", Type: arrowlib.FixedSizeListOf(4, arrowlib.PrimitiveTypes.Float64)},
	}
	return arrowlib.NewSchema(fields, nil)
}

// Build a single Arrow Record from a slice of testRecord data.
func newRecord(recs []testRecord, schema *arrowlib.Schema) arrowlib.Record {
	pool := arrowmem.NewGoAllocator()

	// --- Builders for each column ---
	// (A) TransactionID => LargeString
	txIDBuilder := arrowarr.NewLargeStringBuilder(pool)
	defer txIDBuilder.Release()

	// (B) TransactionAmount => Float64
	amtBuilder := arrowarr.NewFloat64Builder(pool)
	defer amtBuilder.Release()

	// (C) Timestamp => *arrow.TimestampType
	tsType := schema.Field(2).Type.(*arrowlib.TimestampType)
	tsBuilder := arrowarr.NewTimestampBuilder(pool, tsType)
	defer tsBuilder.Release()

	// (D) IsFraud => bool
	boolBuilder := arrowarr.NewBooleanBuilder(pool)
	defer boolBuilder.Release()

	// (E) UserAge => Int32
	i32Builder := arrowarr.NewInt32Builder(pool)
	defer i32Builder.Release()

	// (F) SignupDate => Date32
	dateBuilder := arrowarr.NewDate32Builder(pool)
	defer dateBuilder.Release()

	// (G) AccountBalance => Decimal128(precision=38, scale=2)
	decType := schema.Field(6).Type.(*arrowlib.Decimal128Type)
	decBuilder := arrowarr.NewDecimal128Builder(pool, decType)
	defer decBuilder.Release()

	// (H) FeatureVector => FixedSizeBinary(16)
	fsbType := schema.Field(7).Type.(*arrowlib.FixedSizeBinaryType)
	fsbBuilder := arrowarr.NewFixedSizeBinaryBuilder(pool, fsbType)
	defer fsbBuilder.Release()

	// (I) Float64Vec4 => FixedSizeList(4, Float64)
	fslBuilder := arrowarr.NewFixedSizeListBuilder(pool, 4, arrowlib.PrimitiveTypes.Float64)
	defer fslBuilder.Release()
	childBuilder := fslBuilder.ValueBuilder().(*arrowarr.Float64Builder)

	// --- Append data for each row ---
	for _, row := range recs {
		txIDBuilder.Append(row.TransactionID)
		amtBuilder.Append(row.TransactionAmount)

		tsBuilder.Append(arrowlib.Timestamp(row.Timestamp.UnixNano()))
		boolBuilder.Append(row.IsFraud)
		i32Builder.Append(row.UserAge)
		dateBuilder.Append(arrowlib.Date32(row.SignupDays))     // days since epoch
		decBuilder.Append(arrowdec.FromI64(row.AccountBalance)) // scaled by 10^(Scale)

		fsbBuilder.Append(row.FeatureVector) // must be 16 bytes

		// For the FixedSizeList[4] builder
		fslBuilder.Append(true) // "true" = present
		childBuilder.AppendValues(row.Float64Vec4, nil)
	}

	// Build the final arrays
	arrTransactionID := txIDBuilder.NewArray()
	arrTransactionAmount := amtBuilder.NewArray()
	arrTimestamp := tsBuilder.NewArray()
	arrIsFraud := boolBuilder.NewArray()
	arrUserAge := i32Builder.NewArray()
	arrSignupDate := dateBuilder.NewArray()
	arrAccountBalance := decBuilder.NewArray()
	arrFeatureVector := fsbBuilder.NewArray()
	arrFloat64Vec4 := fslBuilder.NewArray()

	// Make sure to Release them if you won't keep references:
	return arrowarr.NewRecord(
		schema,
		[]arrowlib.Array{
			arrTransactionID,
			arrTransactionAmount,
			arrTimestamp,
			arrIsFraud,
			arrUserAge,
			arrSignupDate,
			arrAccountBalance,
			arrFeatureVector,
			arrFloat64Vec4,
		},
		int64(len(recs)), // number of rows
	)
}

func newTestRecordAndRows(t *testing.T) (arrowlib.Record, types.Rows) {
	rec := []testRecord{
		{
			TransactionID:     "tx001",
			TransactionAmount: 42.75,
			Timestamp:         time.Date(2023, 1, 2, 15, 4, 5, 0, time.UTC),
			IsFraud:           false,
			UserAge:           25,
			SignupDays:        19000, // ~2022-01-01
			AccountBalance:    12345, // 123.45 with scale=2
			FeatureVector:     []byte("abcdefghijklmnop"),
			Float64Vec4:       []float64{1.0, 2.0, 3.0, 4.0},
		},
		{
			TransactionID:     "tx002",
			TransactionAmount: 99.99,
			Timestamp:         time.Date(2023, 1, 3, 10, 30, 0, 0, time.UTC),
			IsFraud:           true,
			UserAge:           40,
			SignupDays:        19100, // ~2022-04-11
			AccountBalance:    6789,  // 67.89
			FeatureVector:     []byte("qrstuvwxyz012345"),
			Float64Vec4:       []float64{5.0, 6.0, 7.0, 8.0},
		},
	}
	expected := types.Rows{
		types.Row{
			types.Value{
				NativeType: "large_utf8",
				Type:       types.String,
				Value:      "tx001",
			},
			types.Value{
				NativeType: "float64",
				Type:       types.Float64,
				Value:      42.75,
			},
			types.Value{
				NativeType: "timestamp",
				Type:       types.Timestamp,
				Value:      time.Date(2023, time.January, 2, 15, 4, 5, 0, time.UTC),
			},
			types.Value{
				NativeType: "bool",
				Type:       types.Bool,
				Value:      false,
			},
			types.Value{
				NativeType: "int32",
				Type:       types.Int32,
				Value:      int32(25),
			},
			types.Value{
				NativeType: "date32",
				Type:       types.Timestamp,
				Value:      time.Date(2022, time.January, 8, 0, 0, 0, 0, time.UTC),
			},
			types.Value{
				NativeType: "decimal",
				Type:       types.Unknown,
				Value:      nil,
			},
			types.Value{
				NativeType: "fixed_size_binary",
				Type:       types.String,
				Value:      "abcdefghijklmnop",
			},
			types.Value{
				NativeType: "fixed_size_list<float64, 4>",
				Type: types.VectorType{
					ScalarType:  types.Float64,
					Dimension:   4,
					IsEmbedding: false,
				},
				Value: nil,
			},
		},
		types.Row{
			types.Value{
				NativeType: "large_utf8",
				Type:       types.String,
				Value:      "tx002",
			},
			types.Value{
				NativeType: "float64",
				Type:       types.Float64,
				Value:      99.99,
			},
			types.Value{
				NativeType: "timestamp",
				Type:       types.Timestamp,
				Value:      time.Date(2023, time.January, 3, 10, 30, 0, 0, time.UTC),
			},
			types.Value{
				NativeType: "bool",
				Type:       types.Bool,
				Value:      true,
			},
			types.Value{
				NativeType: "int32",
				Type:       types.Int32,
				Value:      int32(40),
			},
			types.Value{
				NativeType: "date32",
				Type:       types.Timestamp,
				Value:      time.Date(2022, time.April, 18, 0, 0, 0, 0, time.UTC),
			},
			types.Value{
				NativeType: "decimal",
				Type:       types.Unknown,
				Value:      nil,
			},
			types.Value{
				NativeType: "fixed_size_binary",
				Type:       types.String,
				Value:      "qrstuvwxyz012345",
			},
			types.Value{
				NativeType: "fixed_size_list<float64, 4>",
				Type: types.VectorType{
					ScalarType:  types.Float64,
					Dimension:   4,
					IsEmbedding: false,
				},
				Value: nil,
			},
		},
	}
	return newRecord(rec, newTestSchema()), expected
}

func TestRows(t *testing.T) {
	schema, err := ConvertSchema(newTestSchema())
	if err != nil {
		t.Fatalf("Conversion failed: %v", err)
	}
	rec, expRows := newTestRecordAndRows(t)
	rows, err := ConvertRecordToRows(rec, schema)
	if err != nil {
		t.Fatalf("Conversion failed: %v", err)
	}
	assertRowsEqual(t, rows, expRows)
}

func assertRowsEqual(t *testing.T, rows, expRows types.Rows) {
	if len(expRows) != len(rows) {
		t.Fatalf("Lengths do no match\nFound: %#v\nExpected: %#v", rows, expRows)
	}
	for i := range expRows {
		expRow := expRows[i]
		row := rows[i]
		if len(row) != len(expRow) {
			t.Fatalf("Lengths do no match\nFound: %#v\nExpected: %#v", row, expRow)
		}
		for j := range expRow {
			expVal := expRow[j]
			val := row[j]
			if !reflect.DeepEqual(expVal, val) {
				t.Fatalf("Found: %#v\nExpected: %#v", expVal, val)
			}
		}
	}
}
