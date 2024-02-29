package scheduling

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type MemoryStorageProvider struct {
	storage     sync.Map
	lockedItems sync.Map
	mutex       sync.RWMutex
}

func NewMemoryStorageProvider() (*MemoryStorageProvider, error) {
	storage := sync.Map{}
	lockedItems := sync.Map{}
	mutex := sync.RWMutex{}
	return &MemoryStorageProvider{storage: storage, lockedItems: lockedItems, mutex: mutex}, nil
}

func (m *MemoryStorageProvider) Set(key string, value string, lock LockObject) error {
	if key == "" {
		return fmt.Errorf("key is empty")
	}
	if value == "" {
		return fmt.Errorf("value is empty for key %s", key)
	}

	lockInfo, ok := m.lockedItems.Load(key)
	if !ok {
		return fmt.Errorf("key is not locked")
	}

	currentLock := lockInfo.(LockInformation)
	if currentLock.ID != lock.ID {
		return fmt.Errorf("key %s is locked by another id: locked by: %s, unlock by: %s", key, currentLock.ID, lock.ID)
	}

	m.storage.Store(key, value)
	return nil
}

func (m *MemoryStorageProvider) Get(key string, prefix bool) (map[string]string, error) {
	if key == "" {
		return nil, fmt.Errorf("key is empty")
	}

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

	if len(result) == 0 {
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

func (m *MemoryStorageProvider) Delete(key string, lock LockObject) error {
	if key == "" {
		return fmt.Errorf("key is empty")
	}

	lockInfo, ok := m.lockedItems.Load(key)
	if !ok {
		return fmt.Errorf("key is not locked")
	}

	currentLock := lockInfo.(LockInformation)
	if currentLock.ID != lock.ID {
		return fmt.Errorf("key %s is locked by another id: locked by: %s, unlock by: %s", key, currentLock.ID, lock.ID)
	}

	m.storage.Delete(key)
	m.lockedItems.Delete(key)
	return nil
}

func (m *MemoryStorageProvider) Lock(key string) (LockObject, error) {
	if key == "" {
		return LockObject{}, fmt.Errorf("key is empty")
	}

	id := uuid.New().String()

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if lockInfo, ok := m.lockedItems.Load(key); ok {
		keyLock := lockInfo.(LockInformation)
		if time.Since(keyLock.Date) < ValidTimePeriod {
			return LockObject{}, fmt.Errorf("key is already locked by: %s", keyLock.ID)
		}
	}

	lockChannel := make(chan error)
	go m.updateLockTime(id, key, lockChannel)
	lockObject := LockObject{ID: id, Channel: &lockChannel}

	lock := LockInformation{
		ID:   id,
		Key:  key,
		Date: time.Now().UTC(),
	}
	m.lockedItems.Store(key, lock)

	return lockObject, nil
}

func (m *MemoryStorageProvider) Unlock(key string, lock LockObject) error {
	if key == "" {
		return fmt.Errorf("key is empty")
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if lockInfo, ok := m.lockedItems.Load(key); ok {
		keyLock := lockInfo.(LockInformation)
		if keyLock.ID != lock.ID {
			return fmt.Errorf("key is locked by another id: locked by: %s, unlock  by: %s", keyLock.ID, lock.ID)
		}
		m.lockedItems.Delete(key)
		return nil
	}
	return fmt.Errorf("key is not locked")
}

func (m *MemoryStorageProvider) updateLockTime(id string, key string, lockChannel chan error) {
	ticker := time.NewTicker(UpdateSleepTime)
	defer ticker.Stop()

	for {
		select {
		case <-lockChannel:
			// Received signal to stop
			if lockChannel != nil {
				return
			}
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
