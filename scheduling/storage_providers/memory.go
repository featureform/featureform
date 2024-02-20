package scheduling

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type LockInformation struct {
	ID   string
	Key  string
	Date time.Time
}

type MemoryStorageProvider struct {
	storage     map[string]string
	lockedItems map[string]LockInformation
}

func NewMemoryStorageProvider() *MemoryStorageProvider {
	return &MemoryStorageProvider{storage: make(map[string]string)}
}

func (m *MemoryStorageProvider) Set(key string, value string) error {
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

func (m *MemoryStorageProvider) Lock(id, key string) error {
	if m.lockedItems == nil {
		m.lockedItems = make(map[string]LockInformation)
	}
	keyLockInfo, ok := m.lockedItems[key]
	if ok && keyLockInfo.ID == id {
		return nil
	} else if ok {
		return fmt.Errorf("key is already locked by %s", keyLockInfo.ID)
	}
	m.lockedItems[key] = LockInformation{
		ID:   id,
		Key:  key,
		Date: time.Now(),
	}
	return nil
}

func (m *MemoryStorageProvider) Unlock(id, key string) error {
	keyLockInfo, ok := m.lockedItems[key]
	if !ok {
		return fmt.Errorf("key is not locked")
	}
	if keyLockInfo.ID != id {
		return fmt.Errorf("key is locked by another id")
	}
	delete(m.lockedItems, key)
	return nil
}
