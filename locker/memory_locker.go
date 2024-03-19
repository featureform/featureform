package locker

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/featureform/fferr"
	"github.com/google/uuid"
)

type memoryKey struct {
	id   string
	key  string
	Done *chan error
}

func (k *memoryKey) ID() string {
	return k.id
}

func (k *memoryKey) Key() string {
	return k.key
}

func NewMemoryLocker() memoryLocker {
	return memoryLocker{
		lockedItems: sync.Map{},
		mutex:       &sync.Mutex{},
	}
}

type memoryLocker struct {
	lockedItems sync.Map
	mutex       *sync.Mutex
}

func (m *memoryLocker) Lock(key string) (Key, fferr.GRPCError) {
	if key == "" {
		return nil, fferr.NewInternalError(fmt.Errorf("cannot lock an empty key"))
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	existingKey, isPrefix := m.isPrefixOfExistingKey(key)
	if isPrefix {
		return nil, fferr.NewKeyAlreadyLockedError(key, existingKey, nil)
	}

	prefix, hasPrefix := m.hasPrefixLocked(key)
	if hasPrefix {
		return nil, fferr.NewKeyAlreadyLockedError(key, prefix, nil)
	}

	id := uuid.New().String()

	if lockInfo, ok := m.lockedItems.Load(key); ok {
		keyLock := lockInfo.(LockInformation)
		if time.Since(keyLock.Date) < ValidTimePeriod {
			return nil, fferr.NewKeyAlreadyLockedError(key, keyLock.ID, nil)
		}
	}

	doneChannel := make(chan error)
	lockKey := &memoryKey{id: id, key: key, Done: &doneChannel}

	lock := LockInformation{
		ID:   id,
		Key:  key,
		Date: time.Now().UTC(),
	}
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
	ticker := time.NewTicker(UpdateSleepTime)
	defer ticker.Stop()

	for {
		select {
		case <-*key.Done:
			// Received signal to stop
			return
		case <-ticker.C:
			// Continue updating lock time
			// We need to check if the key still exists because it could have been deleted
			lockInfo, ok := m.lockedItems.Load(key.key)
			if !ok {
				// Key no longer exists, stop updating
				return
			}
			lock, ok := lockInfo.(LockInformation)
			if !ok {
				return
			}

			if lock.ID == key.id {
				lock.Date = time.Now().UTC()
				// Update lock time
				m.lockedItems.Store(key.key, lock)
			}
		}
	}
}

func (m *memoryLocker) Unlock(key Key) fferr.GRPCError {
	if key.Key() == "" {
		return fferr.NewInternalError(fmt.Errorf("cannot unlock an empty key"))
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

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
	m.lockedItems.Delete(key.Key())
	mKey, ok := key.(*memoryKey)
	if !ok {
		return fferr.NewInternalError(fmt.Errorf("could not cast key to memory key"))
	}
	close(*mKey.Done)

	return nil
}
