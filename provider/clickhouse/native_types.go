// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

// Package clickhouse provides ClickHouse-specific type definitions
package clickhouse

import (
	"fmt"

	fftypes "github.com/featureform/fftypes"
)

var (
	// String type
	STRING = fftypes.NativeTypeLiteral("String")

	// Boolean type
	BOOL = fftypes.NativeTypeLiteral("Bool")

	// Integer types
	INT   = fftypes.NativeTypeLiteral("Int")
	INT8  = fftypes.NativeTypeLiteral("Int8")
	INT16 = fftypes.NativeTypeLiteral("Int16")
	INT32 = fftypes.NativeTypeLiteral("Int32")
	INT64 = fftypes.NativeTypeLiteral("Int64")

	// Unsigned integer types
	UINT8  = fftypes.NativeTypeLiteral("UInt8")
	UINT16 = fftypes.NativeTypeLiteral("UInt16")
	UINT32 = fftypes.NativeTypeLiteral("UInt32")
	UINT64 = fftypes.NativeTypeLiteral("UInt64")

	// Floating point types
	FLOAT32 = fftypes.NativeTypeLiteral("Float32")
	FLOAT64 = fftypes.NativeTypeLiteral("Float64")

	// DateTime type with default precision
	DATETIME64 = fftypes.NativeTypeLiteral("DATETIME64(9)")
)

// StringToNativeType maps the string representation to the corresponding NativeTypeLiteral
var StringToNativeType = map[string]fftypes.NativeType{
	"String":        STRING,
	"Bool":          BOOL,
	"Int":           INT,
	"Int8":          INT8,
	"Int16":         INT16,
	"Int32":         INT32,
	"Int64":         INT64,
	"UInt8":         UINT8,
	"UInt16":        UINT16,
	"UInt32":        UINT32,
	"UInt64":        UINT64,
	"Float32":       FLOAT32,
	"Float64":       FLOAT64,
	"DateTime64(9)": DATETIME64,
}

// NullableType represents a nullable version of another type
type NullableType struct {
	innerType fftypes.NativeType
}

func (t *NullableType) TypeName() string {
	return fmt.Sprintf("Nullable(%s)", t.innerType.TypeName())
}

// NewNullableType creates a new nullable type from an inner type
func NewNullableType(innerType fftypes.NativeType) *NullableType {
	return &NullableType{
		innerType: innerType,
	}
}

// GetInnerType returns the inner type of a nullable type
func (t *NullableType) GetInnerType() fftypes.NativeType {
	return t.innerType
}
