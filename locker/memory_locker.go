package locker

import (
	"fmt"
	"sync"
	"time"

	"github.com/featureform/fferr"
	"github.com/google/uuid"
)

type memoryKey struct {
	id             string
	key            string
	ExpirationTime time.Time
	Channel        *chan error
}

func (k *memoryKey) ID() string {
	return k.id
}

func (k *memoryKey) Key() string {
	return k.key
}

type MemoryLocker struct {
	LockedItems sync.Map
	Mutex       *sync.Mutex
}

func (m *MemoryLocker) Lock(key string) (Key, error) {
	if key == "" {
		return &memoryKey{}, fferr.NewInternalError(fmt.Errorf("key is empty"))
	}

	id := uuid.New().String()

	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	if lockInfo, ok := m.LockedItems.Load(key); ok {
		keyLock := lockInfo.(LockInformation)
		if time.Since(keyLock.Date) < ValidTimePeriod {
			return &memoryKey{}, fferr.NewKeyAlreadyLockedError(key, keyLock.ID, nil)
		}
	}

	doneChannel := make(chan error)
	lockKey := memoryKey{id: id, key: key, Channel: &doneChannel}

	lock := LockInformation{
		ID:   id,
		Key:  key,
		Date: time.Now().UTC(),
	}
	m.LockedItems.Store(key, lock)

	go m.updateLockTime(id, key, doneChannel)

	return &lockKey, nil
}

func (m *MemoryLocker) updateLockTime(id string, key string, doneChannel <-chan error) {
	ticker := time.NewTicker(UpdateSleepTime)
	defer ticker.Stop()

	for {
		select {
		case <-doneChannel:
			// Received signal to stop
			return
		case <-ticker.C:
			m.Mutex.Lock()
			defer m.Mutex.Unlock()

			// Continue updating lock time
			lockInfo, ok := m.LockedItems.Load(key)
			if !ok {
				// Key no longer exists, stop updating
				return
			}
			lock := lockInfo.(LockInformation)
			if lock.ID == id {
				// Update lock time
				m.LockedItems.Store(key, LockInformation{
					ID:   id,
					Key:  key,
					Date: time.Now().UTC(),
				})
			}
		}
	}
}

func (m *MemoryLocker) Unlock(key Key) error {
	if key.Key() == "" {
		return fferr.NewInternalError(fmt.Errorf("key is empty"))
	}

	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	lockInfo, ok := m.LockedItems.Load(key.Key())
	if !ok {
		return fferr.NewKeyNotLockedError(key.Key(), nil)
	}

	keyLock := lockInfo.(LockInformation)
	if keyLock.ID != key.ID() {
		err := fferr.NewKeyAlreadyLockedError(key.Key(), keyLock.ID, nil)
		err.AddDetail("trying to unlock key with different id", key.ID())
		return err
	}
	m.LockedItems.Delete(key.Key())
	mKey := key.(*memoryKey)
	close(*mKey.Channel)

	return nil
}
