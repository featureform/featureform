package storage

import (
	"fmt"
	"strings"

	"github.com/featureform/fferr"
)

func NewMemoryStorageImplementation() (memoryStorageImplementation, error) {
	return memoryStorageImplementation{
		storage: make(map[string]string),
	}, nil
}

type memoryStorageImplementation struct {
	storage map[string]string
}

func (m *memoryStorageImplementation) Set(key string, value string) error {
	if key == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("cannot set an empty key"))
	}

	m.storage[key] = value

	return nil
}

func (m *memoryStorageImplementation) Get(key string) (string, error) {
	if key == "" {
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("key is empty"))
	}

	value, ok := m.storage[key]
	if !ok {
		return "", fferr.NewKeyNotFoundError(key, nil)
	}

	return value, nil
}

func (m *memoryStorageImplementation) List(prefix string) (map[string]string, error) {
	result := make(map[string]string)

	for key, value := range m.storage {
		if strings.HasPrefix(key, prefix) {
			result[key] = value
		}
	}

	return result, nil
}

func (m *memoryStorageImplementation) Delete(key string) (string, error) {
	if key == "" {
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("key is empty"))
	}

	value, ok := m.storage[key]
	if !ok {
		return "", fferr.NewKeyNotFoundError(key, nil)
	}

	delete(m.storage, key)

	return value, nil
}
