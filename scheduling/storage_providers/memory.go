package scheduling

import (
	"fmt"
	"sort"
	"strings"
)

type StorageProvider interface {
	Set(key string, value string) error
	Get(key string, prefix bool) ([]string, error)
	ListKeys(prefix string) ([]string, error)
	Lock(key string) error
	Unlock(key string) error
}

type MemoryStorageProvider struct {
	storage     map[string]string
	lockedItems map[string]bool
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

func (m *MemoryStorageProvider) Lock(key string) error {
	if m.lockedItems == nil {
		m.lockedItems = make(map[string]bool)
	}
	if _, ok := m.lockedItems[key]; ok {
		return fmt.Errorf("key is already locked")
	}
	m.lockedItems[key] = true
	return nil
}

func (m *MemoryStorageProvider) Unlock(key string) error {
	if _, ok := m.lockedItems[key]; !ok {
		return fmt.Errorf("key is not locked")
	}
	delete(m.lockedItems, key)
	return nil
}

// KeyNotFoundError represents an error when a key is not found.
type KeyNotFoundError struct {
	Key string
}

// Error returns the error message for KeyNotFoundError.
func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("Key not found: %s", e.Key)
}
