package storage

import (
	"context"
	"fmt"
	"strings"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
)

func NewRDSStorageImplementation(tableName string) (metadataStorageImplementation, error) {
	db, err := helpers.NewRDSPoolConnection()
	if err != nil {
		return nil, err
	}

	connection, err := db.Acquire(context.Background())
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to acquire connection from the database pool: %w", err))
	}

	err = connection.Ping(context.Background())
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to ping the database: %w", err))
	}

	// Create a table to store the key-value pairs
	tableCreationSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (key VARCHAR(255) PRIMARY KEY, value TEXT)", tableName)
	_, err = db.Exec(context.Background(), tableCreationSQL)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to create table %s: %w", tableName, err))
	}

	return &rdsStorageImplementation{
		db:         db,
		tableName:  tableName,
		connection: connection,
	}, nil
}

type rdsStorageImplementation struct {
	db         *pgxpool.Pool
	tableName  string
	connection *pgxpool.Conn
}

func (rds *rdsStorageImplementation) Set(key string, value string) error {
	if key == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("cannot set an empty key"))
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2", rds.tableName)
	_, err := rds.db.Exec(context.Background(), insertSQL, key, value)
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
	row := rds.db.QueryRow(context.Background(), selectSQL, key)

	var value string
	err := row.Scan(&value)
	if err != nil {
		return "", fferr.NewInternalError(fmt.Errorf("failed to get key %s: %w", key, err))
	}

	return value, nil
}

func (rds *rdsStorageImplementation) List(prefix string) (map[string]string, error) {
	selectSQL := fmt.Sprintf("SELECT key, value FROM %s WHERE key LIKE $1", rds.tableName)
	rows, err := rds.db.Query(context.Background(), selectSQL, prefix+"%")
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
	row := rds.db.QueryRow(context.Background(), deleteSQL, key)

	var value string
	err := row.Scan(&value)
	if err != nil && strings.Contains(err.Error(), "no rows in result set") {
		return "", fferr.NewKeyNotFoundError(key, nil)
	} else if err != nil {
		return "", fferr.NewInternalError(fmt.Errorf("failed to delete key %s: %v", key, err))
	}

	return value, nil
}

func (rds *rdsStorageImplementation) Close() {
	rds.connection.Release()
	rds.db.Close()
}
