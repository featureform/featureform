package scheduling

import (
	"fmt"
	"strings"
)

type MemoryStorageProvider struct {
	storage map[string]string
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
	for k, v := range m.storage {
		if strings.HasPrefix(k, key) {
			result = append(result, v)
		}
	}
	if len(result) == 0 {
		return nil, &KeyNotFoundError{Key: key}
	}
	return result, nil
}
