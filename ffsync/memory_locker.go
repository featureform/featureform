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
	"github.com/jonboulle/clockwork"
	"strings"
	"sync"
	"time"

	"github.com/featureform/logging"

	"github.com/featureform/fferr"
	"github.com/google/uuid"
)

const memoryLoggerKey = "memoryLogger"

type memoryKey struct {
	id   string
	key  string
	Done chan error
}

func (k memoryKey) ID() string {
	return k.id
}

func (k memoryKey) Key() string {
	return k.key
}

func NewMemoryLocker() (memoryLocker, error) {
	return memoryLocker{
		lockedItems: &sync.Map{},
		mutex:       &sync.Mutex{},
		logger:      logging.NewLogger("ffsync.memoryLocker"),
		clock:       clockwork.NewRealClock(),
	}, nil
}

type memoryLocker struct {
	lockedItems *sync.Map
	mutex       *sync.Mutex
	logger      logging.Logger
	clock       clockwork.Clock
}

func (m *memoryLocker) checkLock(ctx context.Context, key string) error {
	logger, ok := ctx.Value(memoryLoggerKey).(logging.Logger)
	if !ok {
		logger.DPanic("Unable to get logger from context. Using global logger.")
		logger = logging.GlobalLogger
	}
	logger.Debug("Checking lock for key ", key)

	logger.Debug("Checking if key is prefix of existing key ", key)
	existingKey, isPrefix := m.isPrefixOfExistingKey(key)
	if isPrefix {
		logger.Debugw("key is the prefix of a locked key ", "currentKey", key, "existingKey", existingKey)
		return fferr.NewKeyAlreadyLockedError(key, existingKey, nil)
	}

	logger.Debug("Checking if key has locked prefix ", key)
	prefix, hasPrefix := m.hasPrefixLocked(key)
	if hasPrefix {
		logger.Debugw("key has its prefix locked ", "currentKey", key, "prefix", prefix)
		return fferr.NewKeyAlreadyLockedError(key, prefix, nil)
	}

	logger.Debug("Fetching key item ", key)
	if lockInfo, ok := m.lockedItems.Load(key); ok {
		keyLock := lockInfo.(LockInformation)
		if m.clock.Since(keyLock.Date) < validTimePeriod.Duration() {
			logger.Debugw("failed to lock a living key ", "currentKey", key, "living key", keyLock.Key)
			return fferr.NewKeyAlreadyLockedError(key, keyLock.ID, nil)
		}
	}
	return nil
}

func (m *memoryLocker) attemptLock(ctx context.Context, key string, wait bool) error {
	logger, ok := ctx.Value(memoryLoggerKey).(logging.Logger)
	if !ok {
		logger.DPanic("Unable to get logger from context. Using global logger.")
		logger = logging.GlobalLogger
	}
	logger.Debug("Attempting to lock key")
	startTime := m.clock.Now()

	// Unlock the mutex so we can lock it in the loop
	m.mutex.Unlock()
	for {
		// Lock at the beginning of each check. If the check fails, the unlock in the defer Lock() function will
		// unlock before exiting the attempt.
		// This is required because if checkLock succeeds, we must hold the lock until Lock() exits to prevent race
		// conditions.
		m.mutex.Lock()
		if hasExceededWaitTime(startTime) {
			return fferr.NewExceededWaitTimeError("memory", key)
		}
		if err := m.checkLock(ctx, key); err == nil {
			return nil
		} else if err != nil && fferr.IsKeyAlreadyLockedError(err) && wait == true {
			// Unlock so that other threads can attempt to lock
			m.mutex.Unlock()
			m.clock.Sleep(100 * time.Millisecond)
		} else {
			return err
		}
	}
}

func (m *memoryLocker) Lock(ctx context.Context, key string, wait bool) (Key, error) {
	logger := m.logger.With("key", key, "wait", wait, "request_id", ctx.Value("request_id"))
	ctx = context.WithValue(ctx, memoryLoggerKey, logger)
	logger.Debug("Locking Key")
	if key == "" {
		return nil, fferr.NewLockEmptyKeyError()
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if err := m.attemptLock(ctx, key, wait); err != nil {
		return nil, err
	}

	id := uuid.New().String()

	doneChannel := make(chan error)
	lockKey := &memoryKey{id: id, key: key, Done: doneChannel}

	lock := LockInformation{
		ID:   id,
		Key:  key,
		Date: m.clock.Now().UTC(),
	}

	logger.Debugw("Storing Key", "id", lock.ID)
	m.lockedItems.Store(key, lock)

	go m.updateLockTime(lockKey)

	return lockKey, nil
}

func (m *memoryLocker) isPrefixOfExistingKey(key string) (string, bool) {
	existingKey := ""
	isPrefix := false

	m.lockedItems.Range(func(k, v interface{}) bool {
		if strings.HasPrefix(k.(string), key) {
			isPrefix = true
			existingKey = k.(string)
			return false
		}
		return true
	})

	return existingKey, isPrefix
}

func (m *memoryLocker) hasPrefixLocked(key string) (string, bool) {
	prefix := ""
	hasPrefix := false

	m.lockedItems.Range(func(k, v interface{}) bool {
		if strings.HasPrefix(key, k.(string)) {
			prefix = k.(string)
			hasPrefix = true
			return false
		}
		return true
	})

	return prefix, hasPrefix
}

func (m *memoryLocker) updateLockTime(key *memoryKey) {
	ticker := m.clock.NewTicker(updateSleepTime.Duration())
	defer ticker.Stop()

	for {
		select {
		case <-key.Done:
			// Received signal to stop
			return
		case <-ticker.Chan():
			// Continue updating lock time
			// We need to check if the key still exists because it could have been deleted
			lockInfo, ok := m.lockedItems.Load(key.key)
			if !ok {
				// Key no longer exists, stop updating
				return
			}

			lock, ok := lockInfo.(LockInformation)
			// This will cause lock to fail but shouldn't be possible
			if !ok {
				return
			}
			if lock.ID == key.id {
				lock.Date = m.clock.Now().UTC()
				// Update lock time
				m.lockedItems.Store(key.key, lock)
			}
		}
	}
}

func (m *memoryLocker) Unlock(ctx context.Context, key Key) error {
	logger := m.logger.With("key", key.Key(), "request_id", ctx.Value("request_id"))
	logger.Debug("Unlocking Key")
	if key == nil {
		return fferr.NewInternalError(fmt.Errorf("cannot unlock a nil key"))
	}

	if key.Key() == "" {
		return fferr.NewUnlockEmptyKeyError()
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	logger.Debug("Loading Key Information")
	lockInfo, ok := m.lockedItems.Load(key.Key())
	if !ok {
		return fferr.NewKeyNotLockedError(key.Key(), nil)
	}

	keyLock, ok := lockInfo.(LockInformation)
	if !ok {
		return fferr.NewInternalError(fmt.Errorf("could not cast lock information"))
	}

	if keyLock.ID != key.ID() {
		err := fferr.NewKeyAlreadyLockedError(key.Key(), keyLock.ID, fmt.Errorf("attempting to unlock with incorrect key"))
		err.AddDetail("expected key", keyLock.ID)
		err.AddDetail("received key", key.ID())
		return err
	}

	logger.Debugw("Deleting Key", "id", key.ID())
	m.lockedItems.Delete(key.Key())

	mKey, ok := key.(*memoryKey)
	if !ok {
		return fferr.NewInternalError(fmt.Errorf("could not cast key to memory key"))
	}
	close(mKey.Done)

	logger.Debugw("Key Unlocked Key", "id", key.ID())
	return nil
}

func (m *memoryLocker) Close() {
	// Do nothing
}
