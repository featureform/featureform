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

func (m *MemoryLocker) Lock(key string) (Key, fferr.GRPCError) {
	if key == "" {
		return &memoryKey{}, fferr.NewInternalError(fmt.Errorf("cannot lock an empty key"))
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
			lock, ok := lockInfo.(LockInformation)
			if !ok {
				return
			}

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

func (m *MemoryLocker) Unlock(key Key) fferr.GRPCError {
	if key.Key() == "" {
		return fferr.NewInternalError(fmt.Errorf("cannot unlock an empty key"))
	}

	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	lockInfo, ok := m.LockedItems.Load(key.Key())
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
		err.AddDetail("recieved key", key.ID())
		return err
	}
	m.LockedItems.Delete(key.Key())
	mKey, ok := key.(*memoryKey)
	if !ok {
		return fferr.NewInternalError(fmt.Errorf("could not cast key to memory key"))
	}
	close(*mKey.Channel)

	return nil
}
