package ffsync

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type rdsKey struct {
	id  string
	key string
}

func (k rdsKey) ID() string {
	return k.id
}

func (k rdsKey) Key() string {
	return k.key
}

func NewRDSLocker() (Locker, error) {
	tableName := "ff_locks"
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

	// Create a table to store the locks
	tableCreationSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (lock_id VARCHAR(255) PRIMARY KEY, resource_key VARCHAR(255) NOT NULL, lock_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, UNIQUE (resource_key));", tableName)
	_, err = db.Exec(tableCreationSQL)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to create table %s: %w", tableName, err))
	}

	return &rdsLocker{
		db:        db,
		tableName: tableName,
	}, nil
}

type rdsLocker struct {
	db        *sql.DB
	tableName string
}

func (l *rdsLocker) Lock(key string) (Key, error) {
	if key == "" {
		return nil, fferr.NewInternalError(fmt.Errorf("cannot lock an empty key"))
	}

	id := uuid.New().String()

	lockSQLCommand := fmt.Sprintf("INSERT INTO %s (lock_id, resource_key) VALUES ($1, $2)", l.tableName)
	_, err := l.db.Exec(lockSQLCommand, id, key)
	if err != nil && strings.Contains(err.Error(), "duplicate key value violates unique constraint") {
		return nil, fferr.NewKeyAlreadyLockedError(key, "", nil) // TODO: Should we query to see who locked it?
	} else if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to lock key %s: %w", key, err))
	}

	lockKey := rdsKey{
		id:  id,
		key: key,
	}

	return lockKey, nil
}

func (l *rdsLocker) Unlock(key Key) error {
	if key.Key() == "" {
		return fferr.NewInternalError(fmt.Errorf("cannot unlock an empty key"))
	}

	queryForLock := fmt.Sprintf("SELECT lock_id FROM %s WHERE resource_key = $1", l.tableName)
	row := l.db.QueryRow(queryForLock, key.Key())

	var lockID string
	err := row.Scan(&lockID)
	if err != nil {
		return fferr.NewInternalError(fmt.Errorf("failed to unlock key %s: %w", key.Key(), err))
	}

	if lockID != key.ID() {
		return fferr.NewInternalError(fmt.Errorf("the key '%s' is locked by '%s', trying to unlock with '%s'", key.Key(), lockID, key.ID()))
	}

	unlockSQLCommand := fmt.Sprintf("DELETE FROM %s WHERE lock_id = $1", l.tableName)
	_, err = l.db.Exec(unlockSQLCommand, key.ID())
	if err != nil {
		return fferr.NewInternalError(fmt.Errorf("failed to unlock key %s", key.Key()))
	}

	return nil
}
