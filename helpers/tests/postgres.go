// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package tests

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/featureform/helpers"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
)

func GetTestPostgresParams(dbName string) postgres.Config {
	return postgres.Config{
		Host:     helpers.GetEnv("POSTGRES_HOST", "localhost"),
		Port:     helpers.GetEnv("POSTGRES_PORT", "5432"),
		User:     helpers.GetEnv("POSTGRES_USER", "postgres"),
		Password: helpers.GetEnv("POSTGRES_PASSWORD", "password"),
		DBName:   dbName,
		SSLMode:  helpers.GetEnv("POSTGRES_SSL_MODE", "disable"),
	}
}

func runGooseMigrations(ctx context.Context, db *sql.DB, migrationsDir string) error {
	logger := logging.GetLoggerFromContext(ctx)
	if err := goose.SetDialect("pgx"); err != nil {
		logger.Errorw("Failed to set goose dialect", "err", err)
		return err
	}
	if err := goose.Up(db, migrationsDir); err != nil {
		logger.Errorw("Failed to run migrations", "err", err)
		return err
	}
	logger.Debug("Migrations applied successfully")
	return nil
}

func CreateTestDatabase(ctx context.Context, t *testing.T, migrationsDir string) (dbName string, cleanup func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	dbName = fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	pgConfig := postgres.Config{
		Host:     helpers.GetEnv("POSTGRES_HOST", "localhost"),
		Port:     helpers.GetEnv("POSTGRES_PORT", "5432"),
		User:     helpers.GetEnv("POSTGRES_USER", "postgres"),
		Password: helpers.GetEnv("POSTGRES_PASSWORD", "password"),
		DBName:   "postgres",
		SSLMode:  helpers.GetEnv("POSTGRES_SSL_MODE", "disable"),
	}
	adminPool, err := postgres.NewPool(ctx, pgConfig)

	// Create the test DB
	conn, err := adminPool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire connection to admin pool")
	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE DATABASE "%s";`, dbName))
	require.NoError(t, err, "failed to create test database")
	conn.Release()

	// Run migrations
	pgConfig.DBName = dbName
	db, err := sql.Open("pgx", pgConfig.ConnectionString())
	require.NoError(t, err, "failed to open connection to test database")
	defer db.Close()
	if err := runGooseMigrations(ctx, db, migrationsDir); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}
	// Return a cleanup function
	cleanup = func() {
		adminPool.Close()
	}

	return dbName, cleanup
}
