package ffsync

import (
	"context"
	"fmt"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
)

const (
	lock_expiration_time = "5 minutes"
	key_length           = 2048
)

type rdsKey struct {
	id   string
	key  string
	done chan error
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
	tableCreationSQL := createTableQuery(tableName)
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
	} else if len(key) > key_length {
		return nil, fferr.NewInternalError(fmt.Errorf("key is too long: %d", len(key)))
	}

	id := uuid.New().String()

	// Get the lock
	lockQuery := l.lockQuery()
	r, err := l.db.Exec(context.Background(), lockQuery, id, key)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to lock key %s: %v", key, err))
	} else if r.RowsAffected() == 0 {
		return nil, fferr.NewKeyAlreadyLockedError(key, id, nil)
	}

	done := make(chan error)
	lockKey := rdsKey{
		id:   id,
		key:  key,
		done: done,
	}

	go l.updateLockTime(&lockKey)

	return lockKey, nil
}

func (l *rdsLocker) updateLockTime(key *rdsKey) {
	ticker := time.NewTicker(UpdateSleepTime)
	defer ticker.Stop()

	for {
		select {
		case <-key.done:
			// Received signal to stop
			return
		case <-ticker.C:
			// Continue updating lock time
			// We need to check if the key still exists because it could have been deleted
			updateQueryTime := l.updateLockExpirationQuery()

			r, err := l.db.Exec(context.Background(), updateQueryTime, key.id, key.key)
			if err != nil {
				// Key no longer exists, stop updating
				return
			}
			if r.RowsAffected() == 0 {
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

	close(rdsKey.done)

	return nil
}

func (l *rdsLocker) Close() {
	l.connection.Release()
	l.db.Close()
}

// SQL Queries
func createTableQuery(tableName string) string {
	tableName = helpers.SanitizePostgres(tableName)
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (owner VARCHAR(255), key VARCHAR(%d) NOT NULL, expiration TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP + INTERVAL '%s', PRIMARY KEY (key));", tableName, key_length, lock_expiration_time)
}

func (l *rdsLocker) lockQuery() string {
	tableName := helpers.SanitizePostgres(l.tableName)
	return fmt.Sprintf("INSERT INTO %s (owner, key, expiration) VALUES ($1, $2, NOW() + INTERVAL '%s') ON CONFLICT (key) DO UPDATE SET owner = EXCLUDED.owner, expiration = NOW() + INTERVAL '%s' WHERE %s.expiration < NOW();", tableName, lock_expiration_time, lock_expiration_time, tableName)
}

func (l *rdsLocker) unlockQuery() string {
	tableName := helpers.SanitizePostgres(l.tableName)
	return fmt.Sprintf("DELETE FROM %s WHERE owner = $1 AND key = $2", tableName)
}

func (l *rdsLocker) updateLockExpirationQuery() string {
	tableName := helpers.SanitizePostgres(l.tableName)
	return fmt.Sprintf("UPDATE %s SET expiration = CURRENT_TIMESTAMP + INTERVAL '%s' WHERE owner = $1 AND key = $2 FOR UPDATE", tableName, lock_expiration_time)
}
