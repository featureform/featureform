// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/featureform/helpers"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"
)

func createTestTable(ctx context.Context, t *testing.T, tableName string, pool *postgres.Pool) (psqlStorage metadataStorageImplementation, cleanup func()) {
	t.Helper()
	logger := logging.GetLoggerFromContext(ctx)
	psqlStorage, err := NewPSQLStorageImplementation(ctx, pool, tableName)
	if err != nil {
		t.Fatalf("Failed to create PSQL storage: %v", err)
	}
	indexName := "ff_key_pattern"
	sanitizedName := postgres.Sanitize(tableName)
	// Create a table to store the key-value pairs
	tableCreationSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (key VARCHAR(2048) PRIMARY KEY, value TEXT)", sanitizedName)
	if _, err := pool.Exec(ctx, tableCreationSQL); err != nil {
		logger.Errorw("Failed to create table", "table-name", sanitizedName, "err", err)
		t.Fatalf("Failed to create table %s: %v", sanitizedName, err)
	}

	// Add cleanup defer to drop table and index after tests complete
	cleanup = func() {
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", sanitizedName)
		if _, err := pool.Exec(ctx, dropSQL); err != nil {
			logger.Errorw("Failed to drop table", "table-name", sanitizedName, "err", err)
		}
	}

	// This column is used in deletion
	addClm := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS marked_for_deletion_at TIMESTAMP DEFAULT null", sanitizedName)
	if _, err := pool.Exec(ctx, addClm); err != nil {
		logger.Errorw("Failed to add deletion column", "table-name", sanitizedName, "err", err)
		t.Fatalf("Failed to add deletion column to %s: %v", sanitizedName, err)
	}

	// Add a text index to use for LIKE queries
	indexCreationSQL := fmt.Sprintf("CREATE INDEX %s ON %s (key text_pattern_ops);", indexName, sanitizedName)
	if _, err := pool.Exec(ctx, indexCreationSQL); err != nil {
		// Index probably aleady exists, ignore the error
		logger.Errorw("Failed to create index", "index-name", indexName, "table-name", sanitizedName, "err", err)
	}

	return psqlStorage, cleanup
}

func TestPSQLMetadataStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}
	ctx := logging.NewTestContext(t)
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
	pool, err := postgres.NewPool(ctx, config)
	if err != nil {
		t.Fatalf("Failed to connect to postgres pool")
	}
	dbName = fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	psqlStorage, cleanup := createTestTable(ctx, t, dbName, pool)
	defer cleanup()

	test := MetadataStorageTest{
		t:       t,
		storage: psqlStorage,
	}
	test.Run()

}
