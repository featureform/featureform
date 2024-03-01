package locker

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type MemoryKey struct {
	id             string
	key            string
	expirationTime time.Time
	Channel        *chan error
}

func (k *MemoryKey) ID() string {
	return k.id
}

func (k *MemoryKey) Key() string {
	return k.key
}

func (k *MemoryKey) ExpirationTime() time.Time {
	return k.expirationTime
}

func (k *MemoryKey) SetExpirationTime(t time.Time) error {
	k.expirationTime = t
	return nil
}

type MemoryLocker struct {
	lockedItems sync.Map
	mutex       *sync.Mutex
}

func (m *MemoryLocker) Lock(key string) (Key, error) {
	if key == "" {
		return &MemoryKey{}, fmt.Errorf("key is empty")
	}

	id := uuid.New().String()

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if lockInfo, ok := m.lockedItems.Load(key); ok {
		keyLock := lockInfo.(LockInformation)
		if time.Since(keyLock.Date) < ValidTimePeriod {
			return &MemoryKey{}, fmt.Errorf("key '%s' is already locked by: %s", key, keyLock.ID)
		}
	}

	lockChannel := make(chan error)
	lockKey := MemoryKey{id: id, Channel: &lockChannel}

	lock := LockInformation{
		ID:   id,
		Key:  key,
		Date: time.Now().UTC(),
	}
	m.lockedItems.Store(key, lock)

	go m.updateLockTime(id, key, lockChannel)

	return &lockKey, nil
}

func (m *MemoryLocker) updateLockTime(id string, key string, lockChannel chan error) {
	ticker := time.NewTicker(UpdateSleepTime)
	defer ticker.Stop()

	for {
		select {
		case <-lockChannel:
			// Received signal to stop
			return
		case <-ticker.C:
			m.mutex.Lock()
			// Continue updating lock time
			lockInfo, ok := m.lockedItems.Load(key)
			if !ok {
				// Key no longer exists, stop updating
				return
			}
			lock := lockInfo.(LockInformation)
			if lock.ID == id {
				// Update lock time
				m.lockedItems.Store(key, LockInformation{
					ID:   id,
					Key:  key,
					Date: time.Now().UTC(),
				})
			}
			m.mutex.Unlock()
		}
	}
}

func (m *MemoryLocker) Unlock(key Key) error {
	if key.Key() == "" {
		return fmt.Errorf("key is empty")
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	lockInfo, ok := m.lockedItems.Load(key.Key())
	if !ok {
		return fmt.Errorf("key '%s' is not locked", key.Key())
	}

	keyLock := lockInfo.(LockInformation)
	if keyLock.ID != key.ID() {
		return fmt.Errorf("key '%s' is locked by another id: locked by: %s, unlock  by: %s", key.Key(), keyLock.ID, key.ID())
	}
	m.lockedItems.Delete(key.Key())
	memoryKey := key.(*MemoryKey)
	closeOnce(*memoryKey.Channel)

	return nil
}
