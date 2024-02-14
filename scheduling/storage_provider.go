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
}

type MemoryStorageProvider struct {
	storage map[string]string
}

func (m *MemoryStorageProvider) Set(key string, value string) error {
	if m.storage == nil || len(m.storage) == 0 {
		m.storage = make(map[string]string)
	}
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	if value == "" {
		return fmt.Errorf("value cannot be empty")
	}
	m.storage[key] = value
	return nil
}

func (m *MemoryStorageProvider) Get(key string, prefix bool) ([]string, error) {
	if !prefix {
		if key == "" {
			return nil, fmt.Errorf("key cannot be empty")
		}
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

// KeyNotFoundError represents an error when a key is not found.
type KeyNotFoundError struct {
	Key string
}

// Error returns the error message for KeyNotFoundError.
func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("Key not found: %s", e.Key)
}
