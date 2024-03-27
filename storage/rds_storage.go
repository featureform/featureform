package storage

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	_ "github.com/lib/pq"
)

func NewRDSStorageImplementation(tableName string) (metadataStorageImplementation, error) {
	host := helpers.GetEnv("POSTGRES_HOST", "localhost")
	port := helpers.GetEnv("POSTGRES_PORT", "5432")
	username := helpers.GetEnv("POSTGRES_USER", "postgres")
	password := helpers.GetEnv("POSTGRES_PASSWORD", "mysecretpassword")
	dbName := helpers.GetEnv("POSTGRES_DB", "postgres")
	sslMode := helpers.GetEnv("POSTGRES_SSL_MODE", "disable")

	connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", host, port, username, password, dbName, sslMode)

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to open connection to Postgres: %w", err))
	}

	// Create a table to store the key-value pairs
	tableCreationSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (key VARCHAR(255) PRIMARY KEY, value TEXT)", tableName)
	_, err = db.Exec(tableCreationSQL)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to create table %s: %w", tableName, err))
	}

	return &rdsStorageImplementation{
		db:        db,
		tableName: tableName,
	}, nil
}

type rdsStorageImplementation struct {
	db        *sql.DB
	tableName string
}

func (rds *rdsStorageImplementation) Set(key string, value string) error {
	if key == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("cannot set an empty key"))
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2", rds.tableName)
	_, err := rds.db.Exec(insertSQL, key, value)
	if err != nil {
		return fferr.NewInternalError(fmt.Errorf("failed to set key %s: %w", key, err))
	}

	return nil
}

func (rds *rdsStorageImplementation) Get(key string) (string, error) {
	if key == "" {
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("cannot get an empty key"))
	}

	selectSQL := fmt.Sprintf("SELECT value FROM %s WHERE key = $1", rds.tableName)
	row := rds.db.QueryRow(selectSQL, key)

	var value string
	err := row.Scan(&value)
	if err != nil {
		return "", fferr.NewInternalError(fmt.Errorf("failed to get key %s: %w", key, err))
	}

	return value, nil
}

func (rds *rdsStorageImplementation) List(prefix string) (map[string]string, error) {
	selectSQL := fmt.Sprintf("SELECT key, value FROM %s WHERE key LIKE $1", rds.tableName)
	rows, err := rds.db.Query(selectSQL, prefix+"%")
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to list keys with prefix %s: %w", prefix, err))
	}

	result := make(map[string]string)
	for rows.Next() {
		var key, value string
		err := rows.Scan(&key, &value)
		if err != nil {
			return nil, fferr.NewInternalError(fmt.Errorf("failed to scan key-value pair: %w", err))
		}
		result[key] = value
	}

	return result, nil
}

func (rds *rdsStorageImplementation) Delete(key string) (string, error) {
	if key == "" {
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("cannot delete empty key"))
	}

	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE key = $1 RETURNING value", rds.tableName)
	row := rds.db.QueryRow(deleteSQL, key)

	var value string
	err := row.Scan(&value)
	if err != nil && strings.Contains(err.Error(), "sql: no rows in result set") {
		return "", fferr.NewKeyNotFoundError(key, nil)
	} else if err != nil {
		return "", fferr.NewInternalError(fmt.Errorf("failed to delete key %s: %v", key, err))
	}

	return value, nil
}
