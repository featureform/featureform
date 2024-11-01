// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package ffsync

import (
	"context"
	"fmt"
	"time"

	"github.com/featureform/logging"
	"go.uber.org/zap"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
)

const key_length = 2048
const tickerFailedUpdateLimit = 5
const psqlLoggerKey = "psqlLogger"

var lock_expiration_time = ValidTimePeriod.AsPSQLString()

type psqlKey struct {
	id   string
	key  string
	done chan error
}

func (k psqlKey) ID() string {
	return k.id
}

func (k psqlKey) Key() string {
	return k.key
}

func NewPSQLLocker(config helpers.PSQLConfig) (Locker, error) {
	const tableName = "ff_locks"

	db, err := helpers.NewPSQLPoolConnection(config)
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

	return &psqlLocker{
		db:         db,
		tableName:  tableName,
		connection: connection,
		logger:     logging.NewLogger("ffsync.psqlLocker"),
	}, nil
}

type psqlLocker struct {
	db         *pgxpool.Pool
	tableName  string
	connection *pgxpool.Conn
	logger     logging.Logger
}

func (l *psqlLocker) runLockQuery(key string, id string) error {
	lockQuery := l.lockQuery()
	if r, err := l.db.Exec(context.Background(), lockQuery, id, key); err != nil {
		return fferr.NewInternalError(fmt.Errorf("failed to lock key %s: %v", key, err))
	} else if r.RowsAffected() == 0 {
		return fferr.NewKeyAlreadyLockedError(key, id, nil)
	}
	return nil
}

func (l *psqlLocker) attemptLock(ctx context.Context, key string, id string, wait bool) error {
	logger := ctx.Value(psqlLoggerKey).(*zap.SugaredLogger)
	logger.Debug("Attempting to lock key")
	startTime := time.Now()
	for {
		if hasExceededWaitTime(startTime) {
			return fferr.NewExceededWaitTimeError("psql", key)
		}
		logger.Debug("Running lock query")
		if err := l.runLockQuery(key, id); err == nil {
			return nil
		} else if err != nil && fferr.IsKeyAlreadyLockedError(err) && wait == true {
			time.Sleep(100 * time.Millisecond)
		} else {
			logger.Errorw("Failed to lock key", "error", err.Error())
			return err
		}
	}
}

func (l *psqlLocker) Lock(ctx context.Context, key string, wait bool) (Key, error) {
	logger := l.logger.With("key", key, "wait", wait, "request_id", ctx.Value("request_id"))
	ctx = context.WithValue(ctx, psqlLoggerKey, logger)
	logger.Debug("Locking key")
	if key == "" {
		return nil, fferr.NewInternalError(fmt.Errorf("cannot lock an empty key"))
	} else if len(key) > key_length {
		return nil, fferr.NewInternalError(fmt.Errorf("key is too long: %d, max length: %d", len(key), key_length))
	}

	id := uuid.New().String()

	if err := l.attemptLock(ctx, key, id, wait); err != nil {
		return nil, err
	}

	done := make(chan error)
	lockKey := psqlKey{
		id:   id,
		key:  key,
		done: done,
	}

	logger.Debug("Starting lock expiration update")
	go l.updateLockTime(&lockKey)

	logger.Debug("Successfully locked key")
	return lockKey, nil
}

func (l *psqlLocker) updateLockTime(key *psqlKey) {
	ticker := time.NewTicker(UpdateSleepTime.Duration())
	defer ticker.Stop()

	// Keep track of failed updates and will return if it exceeds the limit
	failedUpdatesInARow := 0

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
				failedUpdatesInARow++
				if failedUpdatesInARow >= tickerFailedUpdateLimit {
					return
				}
				continue // Retry
			}

			// Key no longer exists, stop updating
			if r.RowsAffected() == 0 {
				return
			}

			failedUpdatesInARow = 0
		}
	}
}

func (l *psqlLocker) Unlock(ctx context.Context, key Key) error {
	logger := l.logger.With("key", key.Key(), "request_id", ctx.Value("request_id"))
	ctx = context.WithValue(ctx, psqlLoggerKey, logger)
	logger.Debug("Unlocking key")
	if key == nil {
		return fferr.NewInternalError(fmt.Errorf("cannot unlock a nil key"))
	}

	if key.Key() == "" {
		return fferr.NewInternalError(fmt.Errorf("cannot unlock an empty key"))
	}

	psqlKey, ok := key.(psqlKey)
	if !ok {
		return fferr.NewInternalError(fmt.Errorf("key is not an PSQL key: %v", key.Key()))
	}

	logger.Debug("Running unlock query")
	unlockSQLCommand := l.unlockQuery()
	_, err := l.db.Exec(context.Background(), unlockSQLCommand, key.ID(), key.Key())
	if err != nil {
		logger.Errorw("Failed to unlock key", "error", err.Error())
		return fferr.NewInternalError(fmt.Errorf("failed to unlock key %s: %v", key.Key(), err))
	}

	close(psqlKey.done)

	logger.Debug("Successfully unlocked key")
	return nil
}

func (l *psqlLocker) Close() {
	l.connection.Release()
	l.db.Close()
}

// SQL Queries
func createTableQuery(tableName string) string {
	tableName = helpers.SanitizePostgres(tableName)
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (owner VARCHAR(255), key VARCHAR(%d) NOT NULL, expiration TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP + INTERVAL '%s', PRIMARY KEY (key));", tableName, key_length, lock_expiration_time)
}

func (l *psqlLocker) lockQuery() string {
	tableName := helpers.SanitizePostgres(l.tableName)
	return fmt.Sprintf("INSERT INTO %s (owner, key, expiration) VALUES ($1, $2, NOW() + INTERVAL '%s') ON CONFLICT (key) DO UPDATE SET owner = EXCLUDED.owner, expiration = NOW() + INTERVAL '%s' WHERE %s.expiration < NOW();", tableName, lock_expiration_time, lock_expiration_time, tableName)
}

func (l *psqlLocker) unlockQuery() string {
	tableName := helpers.SanitizePostgres(l.tableName)
	return fmt.Sprintf("DELETE FROM %s WHERE owner = $1 AND key = $2", tableName)
}

func (l *psqlLocker) updateLockExpirationQuery() string {
	tableName := helpers.SanitizePostgres(l.tableName)
	return fmt.Sprintf("UPDATE %s SET expiration = CURRENT_TIMESTAMP + INTERVAL '%s' WHERE owner = $1 AND key = $2 FOR UPDATE", tableName, lock_expiration_time)
}
