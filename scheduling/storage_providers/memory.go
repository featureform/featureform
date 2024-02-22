package scheduling

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type LockInformation struct {
	ID   string
	Key  string
	Date time.Time
}

const (
	UpdateSleepTime = 2 * time.Second
	ValidTimePeriod = 5 * time.Second
	CleanupInterval = 10 * time.Second
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
	lockInfo, ok := m.lockedItems.Load(key)
	if !ok {
		return fmt.Errorf("key is not locked")
	}

	currentLock := lockInfo.(LockInformation)
	if currentLock.ID != lock.ID {
		return fmt.Errorf("key is locked by another id: locked by: %s, unlock by: %s", currentLock.ID, lock.ID)
	}

	if key == "" {
		return fmt.Errorf("key is empty")
	}
	if value == "" {
		return fmt.Errorf("value is empty for key %s", key)
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

type LockObject struct {
	ID      string
	Channel *chan error
}

func (m *MemoryStorageProvider) Lock(key string) (LockObject, error) {
	id := uuid.New().String()
	fmt.Println("\t Locking key", key, "with id", id)
	if key == "" {
		return LockObject{}, fmt.Errorf("key is empty")
	}

	lockChannel := make(chan error)
	keyLockInfo, ok := m.lockedItems.Load(key)
	if ok && keyLockInfo != nil {
		keyLock := keyLockInfo.(LockInformation)
		if time.Since(keyLock.Date) > ValidTimePeriod {
			m.lockedItems.Delete(key)
		} else if time.Since(keyLock.Date) < ValidTimePeriod {
			return LockObject{}, fmt.Errorf("key is already locked by %s", keyLock.ID)
		}
	}
	lock := LockInformation{
		ID:   id,
		Key:  key,
		Date: time.Now(),
	}
	m.lockedItems.Store(key, lock)

	lockObj := LockObject{ID: id, Channel: &lockChannel}
	go m.updateLockTime(id, key, *lockObj.Channel)
	*lockObj.Channel <- nil

	return lockObj, nil
}

func (m *MemoryStorageProvider) Unlock(key string, lock LockObject) error {
	keyLockInfo, ok := m.lockedItems.Load(key)
	if !ok {
		return fmt.Errorf("key is not locked")
	}
	keyLock := keyLockInfo.(LockInformation)

	if keyLock.ID != lock.ID {
		return fmt.Errorf("key is locked by another id: locked by: %s, unlock  by: %s", keyLock.ID, lock.ID)
	}
	fmt.Println("\t Unlocking key", key, "with id", lock.ID)
	m.lockedItems.Store(key, nil)
	m.lockedItems.Delete(key)

	time.Sleep(2 * time.Second)

	return nil
}

func (m *MemoryStorageProvider) updateLockTime(id string, key string, lockChannel chan error) {
	keyFound := true
	for keyFound {
		time.Sleep(UpdateSleepTime)
		if _, ok := m.lockedItems.Load(key); ok {
			m.lockedItems.Store(key, LockInformation{
				ID:   id,
				Key:  key,
				Date: time.Now(),
			})
		} else {
			keyFound = false
		}
		select {
		case channelValue := <-lockChannel:
			if channelValue != nil {
				return
			}
		default:
		}
	}
}
