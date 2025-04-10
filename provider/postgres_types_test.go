// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fftypes "github.com/featureform/fftypes"
)

// NewPostgresTestData creates test data for PostgreSQL
func NewPostgresTestData(t *testing.T) TestColumnData {
	t.Helper()
	now := time.Now().UTC()
	formattedTime := now.Format("2006-01-02 15:04:05")

	return TestColumnData{
		Columns: []TestColumn{
			{
				Name:           "int_col",
				NativeType:     "integer",
				ExpectedGoType: fftypes.Int32,
				TestValue:      int32(42),
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, int32(42), actual.(int32), "integer value mismatch")
				},
			},
			{
				Name:           "bigint_col",
				NativeType:     "bigint",
				ExpectedGoType: fftypes.Int64,
				TestValue:      int64(9223372036854775807),
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, int64(9223372036854775807), actual.(int64), "bigint value mismatch")
				},
			},
			{
				Name:           "float_col",
				NativeType:     "float8",
				ExpectedGoType: fftypes.Float64,
				TestValue:      float64(3.14159),
				VerifyFunc: func(t *testing.T, actual any) {
					assert.InDelta(t, float64(3.14159), actual.(float64), 0.0001, "float8 value mismatch")
				},
			},
			{
				Name:           "string_col",
				NativeType:     "varchar",
				ExpectedGoType: fftypes.String,
				TestValue:      "string value",
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, "string value", actual.(string), "varchar value mismatch")
				},
			},
			{
				Name:           "bool_col",
				NativeType:     "boolean",
				ExpectedGoType: fftypes.Bool,
				TestValue:      true,
				VerifyFunc: func(t *testing.T, actual any) {
					assert.Equal(t, true, actual.(bool), "boolean value mismatch")
				},
			},
			{
				Name:           "timestamp_col",
				NativeType:     "timestamp with time zone",
				ExpectedGoType: fftypes.Timestamp,
				TestValue:      formattedTime,
				VerifyFunc: func(t *testing.T, actual any) {
					_, ok := actual.(time.Time)
					assert.True(t, ok, "timestamp with time zone not converted to time.Time")
				},
			},
		},
	}
}

func TestPostgresNativeTypeConversions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Configure test environment
	test := getConfiguredPostgresTester(t)
	writableTester, ok := test.storeTester.(OfflineSqlStoreWriteableDatasetTester)
	require.True(t, ok, "Store tester does not support writable datasets")

	// Initialize our test data structure
	pgTestData := NewPostgresTestData(t)

	// Run the common type conversion test
	TestDatabaseTypeConversions(t, writableTester, pgTestData)
}
