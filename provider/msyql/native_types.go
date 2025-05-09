// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

// Package mysql provides MySQL-specific type definitions
package mysql

import (
	fftypes "github.com/featureform/fftypes"
)

var (
	// Integer types
	INTEGER = fftypes.NativeTypeLiteral("integer")
	BIGINT  = fftypes.NativeTypeLiteral("bigint")

	// Floating point types
	FLOAT = fftypes.NativeTypeLiteral("float")

	// String types
	VARCHAR = fftypes.NativeTypeLiteral("varchar")

	// Boolean type
	BOOLEAN = fftypes.NativeTypeLiteral("boolean")

	// Date/Time types
	TIMESTAMP = fftypes.NativeTypeLiteral("timestamp")
)

// StringToNativeType maps the string representation to the corresponding NativeTypeLiteral
var StringToNativeType = map[string]fftypes.NewNativeType{
	"integer":   INTEGER,
	"bigint":    BIGINT,
	"float":     FLOAT,
	"varchar":   VARCHAR,
	"boolean":   BOOLEAN,
	"timestamp": TIMESTAMP,
}
