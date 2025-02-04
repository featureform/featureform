package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/featureform/helpers/postgres"
)

func runGooseMigrations(db *sql.DB, migrationsDir string) error {
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}

	if err := goose.Up(db, migrationsDir); err != nil {
		return err
	}
	log.Println("Migrations applied successfully")
	return nil
}

func createTestDatabase(t *testing.T) (dbName string, cleanup func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Helper()

	dbName = fmt.Sprintf("test_db_%d", time.Now().UnixNano())

	pgConfig := postgres.Config{
		Host:     "localhost",
		Port:     "5432",
		User:     "postgres",
		Password: "password",
		DBName:   "postgres",
		SSLMode:  "disable",
	}
	adminPool, err := postgres.NewPool(ctx, pgConfig)

	// 3. Create the test DB
	conn, err := adminPool.Acquire(ctx)
	require.NoError(t, err, "failed to acquire connection to admin pool")
	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE DATABASE "%s";`, dbName))
	require.NoError(t, err, "failed to create test database")
	conn.Release()

	// Run migrations
	pgConfig.DBName = dbName
	db, err := sql.Open("pgx", pgConfig.ConnectionString())
	defer db.Close()
	require.NoError(t, err, "failed to open connection to test database")
	err = runGooseMigrations(db, "../db/migrations")
	if err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	// 4. Return a cleanup function
	cleanup = func() {
		// Step 1: Terminate all connections to the test database
		terminateConnectionsQuery := `
			SELECT pg_terminate_backend(pg_stat_activity.pid)
			FROM pg_stat_activity
			WHERE pg_stat_activity.datname = $1
			  AND pid <> pg_backend_pid();
		`
		if _, err := adminPool.Exec(context.Background(), terminateConnectionsQuery, dbName); err != nil {
			log.Printf("Warning: Failed to terminate connections to test database: %v", err)
		}

		// Step 2: Drop the test database
		dropDatabaseQuery := `DROP DATABASE ` + postgres.Sanitize(dbName) + `;`
		if _, err = adminPool.Exec(context.Background(), dropDatabaseQuery); err != nil {
			log.Printf("Warning: Failed to drop test database: %v", err)
		}

		adminPool.Close()
	}

	return dbName, cleanup
}
