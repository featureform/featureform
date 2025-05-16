// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

// Package bigquery provides BigQuery-specific type definitions
package bigquery

import (
	fftypes "github.com/featureform/fftypes"
)

var (
	// Integer types
	INT64   = fftypes.NativeTypeLiteral("INT64")
	INTEGER = fftypes.NativeTypeLiteral("INTEGER")
	BIGINT  = fftypes.NativeTypeLiteral("BIGINT")

	// Floating point types
	FLOAT64 = fftypes.NativeTypeLiteral("FLOAT64")
	DECIMAL = fftypes.NativeTypeLiteral("DECIMAL")

	// String type
	STRING = fftypes.NativeTypeLiteral("STRING")

	// Boolean type
	BOOL = fftypes.NativeTypeLiteral("BOOL")

	// Date/Time types
	DATE      = fftypes.NativeTypeLiteral("DATE")
	DATETIME  = fftypes.NativeTypeLiteral("DATETIME")
	TIME      = fftypes.NativeTypeLiteral("TIME")
	TIMESTAMP = fftypes.NativeTypeLiteral("TIMESTAMP")
)

// StringToNativeType maps the string representation to the corresponding NativeTypeLiteral
var StringToNativeType = map[string]fftypes.NativeType{
	"INT64":     INT64,
	"INTEGER":   INTEGER,
	"BIGINT":    BIGINT,
	"FLOAT64":   FLOAT64,
	"DECIMAL":   DECIMAL,
	"STRING":    STRING,
	"BOOL":      BOOL,
	"DATE":      DATE,
	"DATETIME":  DATETIME,
	"TIME":      TIME,
	"TIMESTAMP": TIMESTAMP,
}
