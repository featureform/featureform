// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"github.com/featureform/provider/location"
)

type offlineSqlTest struct {
	testConfig  offlineSqlTestConfig
	storeTester offlineSqlStoreTester
}

type offlineSqlTestConfig struct {
	testCrossDbJoins bool
	// sanitizeTableName is used to manually sanitize the tables, as most of the correctness
	// tests assume a Snowflake-like identifier quoting interface. This is used as a stop-gap
	// until locations are refactored, and the tests are truly generic over the location interface.
	sanitizeTableName func(obj location.FullyQualifiedObject) string
	// removeSchemaFromLocation is used to manually "zero" out the schema in any SQLLocations. This is used
	// as a stop-gap until proper Location based support in the tests / providers is implemented.
	removeSchemaFromLocation bool
}

type offlineSqlStoreCoreTester interface {
	AsOfflineStore() (OfflineStore, error)
	GetTestDatabase() string
	CreateSchema(database, schema string) error
	CreateTable(loc location.Location, schema TableSchema) (PrimaryTable, error)
}

type offlineSqlStoreCreateDb interface {
	CreateDatabase(name string) error
	DropDatabase(name string) error
	offlineSqlStoreTester
}

type offlineSqlStoreTester interface {
	offlineSqlStoreCoreTester
	OfflineStore
}

type offlineSqlStoreDatasetTester interface {
	offlineSqlStoreCoreTester
	OfflineStoreDataset
}

type offlineMaterializationSqlStoreTester interface {
	offlineSqlStoreCoreTester
	OfflineStoreDataset
	OfflineStoreMaterialization
}

type offlineTrainingSetSqlStoreTester interface {
	offlineSqlStoreCoreTester
	OfflineStoreDataset
	OfflineStoreMaterialization
	OfflineStoreTrainingSet
}
