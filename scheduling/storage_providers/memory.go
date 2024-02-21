package scheduling

import (
	"fmt"
	"sort"
	"strings"
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
	storage     map[string]string
	lockedItems map[string]LockInformation
}

func NewMemoryStorageProvider() *MemoryStorageProvider {
	return &MemoryStorageProvider{storage: make(map[string]string), lockedItems: make(map[string]LockInformation)}
}

func (m *MemoryStorageProvider) Set(key string, value string, lock LockObject) error {
	lockInfo, ok := m.lockedItems[key]
	if !ok {
		return fmt.Errorf("key is not locked")
	} else if lockInfo.ID != lock.ID {
		return fmt.Errorf("key is locked by another id")
	}

	if key == "" {
		return fmt.Errorf("key is empty")
	}
	if value == "" {
		return fmt.Errorf("value is empty for key %s", key)
	}
	m.storage[key] = value
	return nil
}

func (m *MemoryStorageProvider) Get(key string, prefix bool) ([]string, error) {
	if key == "" {
		return nil, fmt.Errorf("key is empty")
	}
	if !prefix {
		value, ok := m.storage[key]
		if !ok {
			return nil, &KeyNotFoundError{Key: key}
		}
		return []string{value}, nil
	}

	var result []string
	var keys []string
	for k, _ := range m.storage {
		if strings.HasPrefix(k, key) {
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)

	for _, k := range keys {
		if strings.HasPrefix(k, key) {
			result = append(result, m.storage[k])
		}
	}
	if len(result) == 0 {
		return nil, &KeyNotFoundError{Key: key}
	}
	return result, nil
}

func (m *MemoryStorageProvider) ListKeys(prefix string) ([]string, error) {
	var result []string
	for k, _ := range m.storage {
		if strings.HasPrefix(k, prefix) {
			result = append(result, k)
		}
	}
	sort.Strings(result)

	return result, nil
}

type LockObject struct {
	ID      string
	Channel *chan error
}

func (m *MemoryStorageProvider) Lock(key string) (LockObject, error) {
	id := uuid.New().String()
	if key == "" {
		return LockObject{}, fmt.Errorf("key is empty")
	}

	lockChannel := make(chan error)
	keyLockInfo, ok := m.lockedItems[key]
	if ok && time.Since(keyLockInfo.Date) > ValidTimePeriod {
		delete(m.lockedItems, key)
	} else if ok && time.Since(keyLockInfo.Date) < ValidTimePeriod {
		return LockObject{}, fmt.Errorf("key is already locked by %s", keyLockInfo.ID)
	}
	m.lockedItems[key] = LockInformation{
		ID:   id,
		Key:  key,
		Date: time.Now(),
	}

	lockObj := LockObject{ID: id, Channel: &lockChannel}
	go m.updateLockTime(id, key, *lockObj.Channel)
	*lockObj.Channel <- nil

	return lockObj, nil
}

func (m *MemoryStorageProvider) Unlock(key string, lock LockObject) error {
	keyLockInfo, ok := m.lockedItems[key]
	if !ok {
		return fmt.Errorf("key is not locked")
	}
	if keyLockInfo.ID != lock.ID {
		return fmt.Errorf("key is locked by another id")
	}
	delete(m.lockedItems, key)
	return nil
}

func (m *MemoryStorageProvider) updateLockTime(id string, key string, lockChannel chan error) {
	keyFound := true
	for keyFound {
		time.Sleep(UpdateSleepTime)
		if _, ok := m.lockedItems[key]; ok {
			m.lockedItems[key] = LockInformation{
				ID:   id,
				Key:  key,
				Date: time.Now(),
			}
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
