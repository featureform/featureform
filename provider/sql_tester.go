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
	storeTester         offlineSqlStoreTester
	testCrossDbJoins    bool
	transformationQuery string
	sanitizeTableName   func(obj location.FullyQualifiedObject) string
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
