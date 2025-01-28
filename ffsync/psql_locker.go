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

	"github.com/featureform/fferr"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
)

const maxKeyLength = 2048
const tickerFailedUpdateLimit = 5
const psqlLoggerKey = "psqlLogger"

type psqlKey struct {
	owner string
	key   string
	done  chan error
}

func (k psqlKey) Owner() string {
	return k.owner
}

func (k psqlKey) Key() string {
	return k.key
}

func NewPSQLLocker(ctx context.Context, connPool *postgres.Pool) (Locker, error) {
	const tableName = "ff_locks"

	logger := logging.GetLoggerFromContext(ctx).With(
		"lock-table-name", tableName,
	)

	// Create a table to store the locks
	tableCreationSQL := createTableQuery(tableName)
	logger.Debugw("Creating locker table if not exists", "query", tableCreationSQL)
	if _, err := connPool.Exec(ctx, tableCreationSQL); err != nil {
		errMsg := "failed to create psql locker table"
		logger.Errorw(errMsg, "err", err)
		return nil, fferr.NewInternalErrorf("%s: %w", errMsg, err)
	}

	return &psqlLocker{
		connPool:  connPool,
		tableName: tableName,
		logger:    logger,
		clock:     clockwork.NewRealClock(),
	}, nil
}

type psqlLocker struct {
	connPool  *postgres.Pool
	tableName string
	logger    logging.Logger
	clock     clockwork.Clock
}

func (l *psqlLocker) runLockQuery(
	ctx context.Context, key string, owner string, logger logging.Logger,
) error {
	lockQuery := l.lockQuery()
	nowTime := l.clock.Now()
	validTime := nowTime.Add(validTimePeriod.Duration())
	logger.With("lock-query", lockQuery, "lock-now-time", nowTime, "lock-valid-time", validTime)
	logger.Debug("Running lock query")
	if r, err := l.connPool.Exec(ctx, lockQuery, owner, key, validTime, nowTime); err != nil {
		logger.Error("Failed to lock key")
		return fferr.NewInternalErrorf("failed to lock key %s: %v", key, err)
	} else if r.RowsAffected() == 0 {
		logger.Debug("Key already lock")
		return fferr.NewKeyAlreadyLockedError(key, owner, nil)
	}
	logger.Debug("Acquired lock")
	return nil
}

func (l *psqlLocker) attemptLock(
	ctx context.Context, key string, owner string, shouldWait bool, logger logging.Logger,
) error {
	startTime := l.clock.Now()
	logger.Debug("Attempting to lock key", "start-lock-time", startTime)
	for {
		if hasExceededWaitTime(startTime) {
			logger.Error("Exceeded time waiting for lock")
			return fferr.NewExceededWaitTimeError("psql", key)
		}
		logger.Debug("Running lock query")
		if err := l.runLockQuery(ctx, key, owner, logger); err == nil {
			return nil
		} else if fferr.IsKeyAlreadyLockedError(err) && shouldWait {
			sleepTime := 100 * time.Millisecond
			logger.Debugw("Key is locked. Waiting...", "time", sleepTime)
			l.clock.Sleep(sleepTime)
		} else {
			logger.Errorw("Failed to lock key", "error", err)
			return err
		}
	}
}

func (l *psqlLocker) Lock(ctx context.Context, key string, shouldWait bool) (Key, error) {
	owner := uuid.New().String()
	logger := l.logger.WithRequestIDFromContext(ctx).With(
		"lock-key", key, "should-wait-for-lock", shouldWait, "lock-owner", owner,
	)
	logger.Debug("Locking key")
	if key == "" {
		errMsg := "cannot lock an empty key"
		logger.Error(errMsg)
		return nil, fferr.NewInternalErrorf(errMsg)
	} else if len(key) > maxKeyLength {
		errMsg := fmt.Sprintf("key is too long: %d, max length: %d", len(key), maxKeyLength)
		logger.Error(errMsg)
		return nil, fferr.NewInternalErrorf(errMsg)
	}
	if err := l.attemptLock(ctx, key, owner, shouldWait, logger); err != nil {
		if !fferr.IsKeyAlreadyLockedError(err) {
			logger.Errorw("Failed to lock key", "err", err)
		} else if shouldWait {
			logger.Errorw("Timed out waiting for lock", "err", err)
		} else {
			logger.Debug("Key was already locked and didnt wait", "err", err)
		}
		return nil, err
	}

	done := make(chan error)
	lockKey := &psqlKey{
		owner: owner,
		key:   key,
		done:  done,
	}

	logger.Debug("Starting lock expiration update")
	go l.updateLockTime(lockKey, logger)

	logger.Debug("Successfully locked key")
	return lockKey, nil
}

func (l *psqlLocker) updateLockTime(key *psqlKey, logger logging.Logger) {
	sleepDuration := updateSleepTime.Duration()
	logger.With("lock-update-sleep-duration", sleepDuration)
	ticker := l.clock.NewTicker(sleepDuration)
	defer ticker.Stop()
	logger.Debug("Lock time extender thread started")

	// Keep track of failed updates and will return if it exceeds the limit
	failedUpdatesInARow := 0

	for {
		select {
		case <-key.done:
			logger.Debug("Lock time extender received signal to stop")
			// Received signal to stop
			return
		case <-ticker.Chan():
			// Continue updating lock time
			// We need to check if the key still exists because it could have been deleted
			revalidateLockQuery := l.updateLockExpirationQuery()
			validUntil := l.clock.Now().Add(validTimePeriod.Duration())
			logger = logger.With(
				"lock-update-query", revalidateLockQuery, "lock-update-valid-until", validUntil,
			)
			logger.Debug("Extending lock time")
			r, err := l.connPool.Exec(
				context.Background(), revalidateLockQuery, key.owner, key.key, validUntil,
			)
			if err != nil {
				failedUpdatesInARow++
				errLogger := logger.With("error", err, "lock-failed-updates-in-row", failedUpdatesInARow)
				if failedUpdatesInARow >= tickerFailedUpdateLimit {
					errLogger.Error("Failed to revalidate lock, not trying again")
					return
				} else {
					errLogger.Warn("Failed to revalidate lock, trying again")
					continue // Retry
				}
			}

			// Key no longer exists, stop updating
			if r.RowsAffected() == 0 {
				logger.Debug("Lock no longer exists, exiting lock extender thread")
				return
			}

			failedUpdatesInARow = 0
		}
	}
}

func (l *psqlLocker) Unlock(ctx context.Context, key Key) error {
	logger := l.logger.WithRequestIDFromContext(ctx).With(
		"unlock-key-owner", key.Owner(),
		"unlock-key-key", key.Key(),
	)
	logger.Debug("Unlocking key")
	if key == nil {
		errMsg := "Cannot unlock a nil key"
		logger.Error(errMsg)
		return fferr.NewInternalErrorf(errMsg)
	}

	if key.Key() == "" {
		errMsg := "Cannot unlock an empty key"
		logger.Error(errMsg)
		return fferr.NewInternalErrorf(errMsg)
	}

	psqlKey, ok := key.(*psqlKey)
	if !ok {
		errMsg := fmt.Sprintf("Trying to unlock key %#v as a psqlKey, wrong type.", key)
		logger.Error(errMsg)
		return fferr.NewInternalErrorf(errMsg)
	}

	unlockSQLCommand := l.unlockQuery()
	logger = logger.With("unlock-query", unlockSQLCommand)
	logger.Debug("Running unlock query")
	if _, err := l.connPool.Exec(ctx, unlockSQLCommand, key.Owner(), key.Key()); err != nil {
		logger.Errorw("Failed to unlock key", "error", err)
		return fferr.NewInternalErrorf("failed to unlock key %s: %v", key.Key(), err)
	}
	logger.Debug("Successfully ran unlock query, stopping update thread")
	close(psqlKey.done)
	logger.Debug("Successfully unlocked key")
	return nil
}

func (l *psqlLocker) Close() {
	// No-op
}

// SQL Queries
func createTableQuery(tableName string) string {
	tableName = postgres.Sanitize(tableName)
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (owner VARCHAR(255), key VARCHAR(%d) NOT NULL, expiration TIMESTAMP NOT NULL, PRIMARY KEY (key));", tableName, maxKeyLength)
}

func (l *psqlLocker) lockQuery() string {
	tableName := postgres.Sanitize(l.tableName)
	return fmt.Sprintf("INSERT INTO %s (owner, key, expiration) VALUES ($1, $2, $3) ON CONFLICT (key) DO UPDATE SET owner = EXCLUDED.owner, expiration = $3 WHERE %s.expiration < $4;", tableName, tableName)
}

func (l *psqlLocker) unlockQuery() string {
	tableName := postgres.Sanitize(l.tableName)
	return fmt.Sprintf("DELETE FROM %s WHERE owner = $1 AND key = $2", tableName)
}

func (l *psqlLocker) updateLockExpirationQuery() string {
	tableName := postgres.Sanitize(l.tableName)
	return fmt.Sprintf("UPDATE %s SET expiration = $3 WHERE owner = $1 AND key = $2", tableName)
}
