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

	pl "github.com/featureform/provider/location"
	pt "github.com/featureform/provider/provider_type"
)

func TestOfflineStorePostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	// Disabling these tests, as the majority of them assume older behavior of providers
	// (ex. utilizing deprecated ResourceSchema locations, etc.). Currently we just
	// run the tests in correctness_tests instead.

	// TODO: Refactor the tests, and re-enable.
	//postgresTester := getConfiguredPostgresTester(t, false)
	//test := OfflineStoreTest{
	//	t:     t,
	//	store: postgresTester.storeTester,
	//}

	//test.Run()
	//test.RunSQL()
}

func getConfiguredPostgresTester(t *testing.T) OfflineSqlTest {
	postgresConfig, err := getPostgresConfig(t, "")
	if err != nil {
		t.Fatalf("could not get postgres config: %s\n", err)
	}

	store, err := GetOfflineStore(pt.PostgresOffline, postgresConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	offlineStore, err := store.AsOfflineStore()
	if err != nil {
		t.Fatalf("could not initialize offline store: %s\n", err)
	}

	dbName := postgresConfig.Database
	storeTester := postgresOfflineStoreTester{
		defaultDbName:   dbName,
		sqlOfflineStore: offlineStore.(*sqlOfflineStore),
	}

	if err := storeTester.CreateSchema(postgresConfig.Database, postgresConfig.Schema); err != nil {
		t.Fatalf("could not create schema: %s\n", err)
	}
	// TODO: Drop Schema

	sanitizeTableName := func(obj pl.FullyQualifiedObject) string {
		loc := pl.NewSQLLocationFromParts(obj.Database, obj.Schema, obj.Table)
		return SanitizeFullyQualifiedObject(loc.TableLocation())
	}

	return OfflineSqlTest{
		storeTester: &storeTester,
		testConfig: OfflineSqlTestConfig{
			sanitizeTableName: sanitizeTableName,
		},
	}
}

func TestPostgresCastTableItemType(t *testing.T) {
	q := postgresSQLQueries{}

	testTime := time.Date(2025, time.February, 13, 12, 0, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		input    interface{}
		typeSpec interface{}
		expected interface{}
	}{
		{
			name:     "Nil input returns nil",
			input:    nil,
			typeSpec: pgInt,
			expected: nil,
		},
		{
			name:     "pgInt conversion",
			input:    int64(42),
			typeSpec: pgInt,
			expected: int32(42),
		},
		{
			name:     "pgBigInt conversion",
			input:    int64(42),
			typeSpec: pgBigInt,
			expected: 42,
		},
		{
			name:     "pgFloat conversion",
			input:    3.14,
			typeSpec: pgFloat,
			expected: 3.14,
		},
		{
			name:     "pgFloat numeric type conversion",
			input:    []uint8{49, 57, 49, 46, 56, 51},
			typeSpec: pgFloat,
			expected: 191.83,
		},
		{
			name:     "pgString conversion",
			input:    "hello",
			typeSpec: pgString,
			expected: "hello",
		},
		{
			name:     "pgBool conversion",
			input:    true,
			typeSpec: pgBool,
			expected: true,
		},
		{
			name:     "pgTimestamp conversion",
			input:    testTime,
			typeSpec: pgTimestamp,
			expected: testTime,
		},
		{
			name:     "Default case returns input unchanged",
			input:    "unchanged",
			typeSpec: "unknown", // an unrecognized type specifier
			expected: "unchanged",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			result := q.castTableItemType(tc.input, tc.typeSpec)
			assert.Equal(t, tc.expected, result)
		})
	}
}
