package ffsync

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
)

type rdsKey struct {
	id   string
	key  string
	done *chan error
}

func (k rdsKey) ID() string {
	return k.id
}

func (k rdsKey) Key() string {
	return k.key
}

func NewRDSLocker(config helpers.RDSConfig) (Locker, error) {
	const tableName = "ff_locks"

	db, err := helpers.NewRDSPoolConnection(config)
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

	// Create a table to store the locks
	tableCreationSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (lock_id VARCHAR(255), resource_key VARCHAR(255) NOT NULL, lock_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (resource_key));", tableName)
	_, err = db.Exec(context.Background(), tableCreationSQL)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to create table %s: %w", tableName, err))
	}

	return &rdsLocker{
		db:         db,
		tableName:  tableName,
		connection: connection,
	}, nil
}

type rdsLocker struct {
	db         *pgxpool.Pool
	tableName  string
	connection *pgxpool.Conn
}

func (l *rdsLocker) Lock(key string) (Key, error) {
	if key == "" {
		return nil, fferr.NewInternalError(fmt.Errorf("cannot lock an empty key"))
	}

	id := uuid.New().String()

	// Get the lock
	var existingLockID, existingResourceKey string
	var existingTimestamp time.Time
	selectQuery := l.getLockQuery()
	err := l.db.QueryRow(context.Background(), selectQuery, key).Scan(&existingLockID, &existingResourceKey, &existingTimestamp)
	if err != nil {
		if strings.Contains(err.Error(), "no rows in result set") {
			insertQuery := l.insertLockQuery()
			_, err := l.db.Exec(context.Background(), insertQuery, id, key)
			if err != nil {
				return nil, fferr.NewInternalError(fmt.Errorf("failed to insert lock: %w", err))
			}
		} else {
			return nil, fferr.NewInternalError(fmt.Errorf("failed to fetch lock: %w", err))
		}
	}

	if time.Since(existingTimestamp) < 5*time.Second {
		return nil, fferr.NewKeyAlreadyLockedError(key, existingLockID, nil)
	} else {
		// Update the lock
		updateQuery := l.updateLockQuery()
		_, err = l.db.Exec(context.Background(), updateQuery, id, key)
		if err != nil {
			return nil, fferr.NewInternalError(fmt.Errorf("failed to update lock: %w", err))
		}
	}

	done := make(chan error)
	lockKey := rdsKey{
		id:   id,
		key:  key,
		done: &done,
	}

	go l.updateLockTime(&lockKey)

	return lockKey, nil
}

func (l *rdsLocker) updateLockTime(key *rdsKey) {
	ticker := time.NewTicker(UpdateSleepTime)
	defer ticker.Stop()

	for {
		select {
		case <-*key.done:
			// Received signal to stop
			return
		case <-ticker.C:
			// Continue updating lock time
			// We need to check if the key still exists because it could have been deleted
			updateQueryTime := l.updateTimeLockQuery()
			_, err := l.db.Exec(context.Background(), updateQueryTime, key.id, key.key)
			if err != nil {
				// Key no longer exists, stop updating
				return
			}
		}
	}
}

func (l *rdsLocker) Unlock(key Key) error {
	if key.Key() == "" {
		return fferr.NewInternalError(fmt.Errorf("cannot unlock an empty key"))
	}

	unlockSQLCommand := l.unlockQuery()
	_, err := l.db.Exec(context.Background(), unlockSQLCommand, key.ID(), key.Key())
	if err != nil {
		return fferr.NewInternalError(fmt.Errorf("failed to unlock key %s: %v", key.Key(), err))
	}

	rdsKey, ok := key.(rdsKey)
	if !ok {
		return fferr.NewInternalError(fmt.Errorf("key is not an RDS key: %v", key.Key()))
	}

	close(*rdsKey.done)

	return nil
}

func (l *rdsLocker) Close() {
	l.connection.Release()
	l.db.Close()
}

// SQL Queries
func (l *rdsLocker) getLockQuery() string {
	return fmt.Sprintf("SELECT lock_id, resource_key, lock_timestamp FROM %s WHERE resource_key = $1 FOR UPDATE", l.tableName)
}

func (l *rdsLocker) insertLockQuery() string {
	return fmt.Sprintf("INSERT INTO %s (lock_id, resource_key, lock_timestamp) VALUES ($1, $2, CURRENT_TIMESTAMP)", l.tableName)
}

func (l *rdsLocker) updateLockQuery() string {
	return fmt.Sprintf("UPDATE %s SET lock_id = $1, lock_timestamp = CURRENT_TIMESTAMP WHERE resource_key = $2", l.tableName)
}

func (l *rdsLocker) unlockQuery() string {
	return fmt.Sprintf("DELETE FROM %s WHERE lock_id = $1 AND resource_key = $2", l.tableName)
}

func (l *rdsLocker) updateTimeLockQuery() string {
	return fmt.Sprintf("UPDATE %s SET lock_timestamp = CURRENT_TIMESTAMP WHERE lock_id = $1 AND resource_key = $2 FOR UPDATE", l.tableName)
}
