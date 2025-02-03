// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package storage

import (
	"testing"

	"github.com/featureform/helpers"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"
)

func TestPSQLMetadataStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	host := helpers.GetEnv("POSTGRES_HOST", "localhost")
	port := helpers.GetEnv("POSTGRES_PORT", "5432")
	username := helpers.GetEnv("POSTGRES_USER", "postgres")
	password := helpers.GetEnv("POSTGRES_PASSWORD", "mysecretpassword")
	dbName := helpers.GetEnv("POSTGRES_DB", "postgres")
	sslMode := helpers.GetEnv("POSTGRES_SSL_MODE", "disable")

	config := postgres.Config{
		Host:     host,
		Port:     port,
		User:     username,
		Password: password,
		DBName:   dbName,
		SSLMode:  sslMode,
	}
	ctx := logging.NewTestContext(t)
	pool, err := postgres.NewPool(ctx, config)
	if err != nil {
		t.Fatalf("Failed to connect to postgres pool")
	}

	psqlStorage, err := NewPSQLStorageImplementation(ctx, pool, "test_table")
	if err != nil {
		t.Fatalf("Failed to create PSQL storage: %v", err)
	}

	test := MetadataStorageTest{
		t:       t,
		storage: psqlStorage,
	}
	test.Run()
}
