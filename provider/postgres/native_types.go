// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

// Package postgres provides PostgreSQL-specific type definitions
package postgres

import (
	fftypes "github.com/featureform/fftypes"
)

var (
	// Integer types
	INTEGER = fftypes.NativeTypeLiteral("integer")
	INT     = fftypes.NativeTypeLiteral("int")
	BIGINT  = fftypes.NativeTypeLiteral("bigint")

	// Floating point types
	FLOAT8           = fftypes.NativeTypeLiteral("float8")
	NUMERIC          = fftypes.NativeTypeLiteral("numeric")
	DOUBLE_PRECISION = fftypes.NativeTypeLiteral("double precision")

	// String types
	VARCHAR           = fftypes.NativeTypeLiteral("varchar")
	CHARACTER_VARYING = fftypes.NativeTypeLiteral("character varying")

	// Boolean type
	BOOLEAN = fftypes.NativeTypeLiteral("boolean")

	// Date/Time types
	DATE                     = fftypes.NativeTypeLiteral("date")
	TIMESTAMP_WITH_TIME_ZONE = fftypes.NativeTypeLiteral("timestamp with time zone")
	TIMESTAMPTZ              = fftypes.NativeTypeLiteral("timestamptz")
)

// StringToNativeType maps the string representation to the corresponding NativeTypeLiteral
var StringToNativeType = map[string]fftypes.NewNativeType{
	"integer":                  INTEGER,
	"int":                      INT,
	"bigint":                   BIGINT,
	"float8":                   FLOAT8,
	"numeric":                  NUMERIC,
	"double precision":         DOUBLE_PRECISION,
	"varchar":                  VARCHAR,
	"character varying":        CHARACTER_VARYING,
	"boolean":                  BOOLEAN,
	"date":                     DATE,
	"timestamp with time zone": TIMESTAMP_WITH_TIME_ZONE,
	"timestamptz":              TIMESTAMPTZ,
}
