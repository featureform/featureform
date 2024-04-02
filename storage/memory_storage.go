package storage

import (
	"fmt"
	"strings"
	"sync"

	"github.com/featureform/fferr"
)

func NewMemoryStorageImplementation() (memoryStorageImplementation, error) {
	return memoryStorageImplementation{
		storage: &sync.Map{},
	}, nil
}

type memoryStorageImplementation struct {
	storage *sync.Map
}

func (m *memoryStorageImplementation) Set(key string, value string) error {
	if key == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("cannot set an empty key"))
	}

	m.storage.Store(key, value)

	return nil
}

func (m *memoryStorageImplementation) Get(key string) (string, error) {
	if key == "" {
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("key is empty"))
	}

	value, ok := m.storage.Load(key)
	if !ok {
		return "", fferr.NewKeyNotFoundError(key, nil)
	}

	return value.(string), nil
}

func (m *memoryStorageImplementation) List(prefix string) (map[string]string, error) {
	result := make(map[string]string)

	m.storage.Range(func(key, value interface{}) bool {
		if strings.HasPrefix(key.(string), prefix) {
			result[key.(string)] = value.(string)
		}
		return true
	})

	return result, nil
}

func (m *memoryStorageImplementation) Delete(key string) (string, error) {
	if key == "" {
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("key is empty"))
	}

	value, ok := m.storage.LoadAndDelete(key)
	if !ok {
		return "", fferr.NewKeyNotFoundError(key, nil)
	}

	return value.(string), nil
}
