package scheduling

import (
	"github.com/featureform/fferr"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type LockObject struct {
	ID      string
	Channel *chan error
}

type LockInformation struct {
	ID   string
	Key  string
	Date time.Time
	Lock LockObject
}

const (
	UpdateSleepTime = 2 * time.Second
	ValidTimePeriod = 5 * time.Second
)

type MemoryStorageProvider struct {
	storage     sync.Map
	lockedItems sync.Map
}

func NewMemoryStorageProvider() *MemoryStorageProvider {
	storage := sync.Map{}
	lockedItems := sync.Map{}
	return &MemoryStorageProvider{storage: storage, lockedItems: lockedItems}
}

func (m *MemoryStorageProvider) Set(key string, value string, lock LockObject) error {
	if key == "" {
		return fferr.NewInternalErrorf("attempted to set an empty key")
	}
	if value == "" {
		return fferr.NewInternalErrorf("attempted to set a key without a value")
	}

	lockInfo, ok := m.lockedItems.Load(key)
	if !ok {
		return fferr.NewKeyNotLockedError(key, nil)
	}

	currentLock := lockInfo.(LockInformation)
	if currentLock.ID != lock.ID {
		return fferr.NewKeyAlreadyLockedError(key, lock.ID, nil)
	}

	m.storage.Store(key, value)
	return nil
}

func (m *MemoryStorageProvider) Get(key string, prefix bool) (map[string]string, error) {

	result := make(map[string]string)

	if !prefix {
		value, ok := m.storage.Load(key)
		if !ok {
			return nil, &KeyNotFoundError{Key: key}
		}
		result[key] = value.(string)
		return result, nil
	}

	// loops through the keys in sync map
	// and finds the keys that have the prefix
	m.storage.Range(func(k, v interface{}) bool {
		mapKey := k.(string)
		if strings.HasPrefix(mapKey, key) {
			result[mapKey] = v.(string)
		}
		return true
	})

	if len(result) == 0 && !prefix {
		return nil, &KeyNotFoundError{Key: key}
	}
	return result, nil
}

func (m *MemoryStorageProvider) ListKeys(prefix string) ([]string, error) {
	var result []string
	// loops through the keys in sync map
	// and finds the keys that have the prefix
	m.storage.Range(func(k, v interface{}) bool {
		mapKey := k.(string)
		if strings.HasPrefix(mapKey, prefix) {
			result = append(result, mapKey)
		}
		return true
	})
	sort.Strings(result)

	return result, nil
}

func (m *MemoryStorageProvider) Lock(key string) (LockObject, error) {
	if key == "" {
		return LockObject{}, fferr.NewInternalErrorf("attempted to lock an empty key")
	}

	id := uuid.New().String()

	lockMutex := &sync.Mutex{}
	lockMutex.Lock()
	defer lockMutex.Unlock()

	if lockInfo, ok := m.lockedItems.Load(key); ok {
		keyLock := lockInfo.(LockInformation)
		if time.Since(keyLock.Date) < ValidTimePeriod {
			return LockObject{}, fferr.NewKeyAlreadyLockedError(key, keyLock.ID, nil)
		}
	}

	lockChannel := make(chan error)
	go m.updateLockTime(id, key, lockChannel)
	lockObject := LockObject{ID: id, Channel: &lockChannel}

	lock := LockInformation{
		ID:   id,
		Key:  key,
		Date: time.Now(),
		Lock: lockObject,
	}
	m.lockedItems.Store(key, lock)

	return lockObject, nil
}

func (m *MemoryStorageProvider) Unlock(key string, lock LockObject) error {
	lockMutex := &sync.Mutex{}
	lockMutex.Lock()
	defer lockMutex.Unlock()

	if lockInfo, ok := m.lockedItems.Load(key); ok {
		keyLock := lockInfo.(LockInformation)
		if keyLock.ID != lock.ID {
			return fferr.NewKeyAlreadyLockedError(key, lock.ID, nil)
		}
		m.lockedItems.Delete(key)
		return nil
	}
	return fferr.NewKeyNotLockedError(key, nil)
}

func (m *MemoryStorageProvider) updateLockTime(id string, key string, lockChannel chan error) {
	for {
		time.Sleep(UpdateSleepTime)

		select {
		case <-lockChannel:
			// Received signal to stop
			if lockChannel != nil {
				return
			}
		default:
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
					Date: time.Now(),
				})
			}
		}
	}
}
