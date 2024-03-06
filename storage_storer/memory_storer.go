package storage_storer

import (
	"fmt"
	"strings"

	"github.com/featureform/fferr"
)

type MemoryStorerImplementation struct {
	Storage map[string]string
}

func (m *MemoryStorerImplementation) Set(key string, value string) fferr.GRPCError {
	if key == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("key is empty"))
	}

	m.Storage[key] = value

	return nil
}

func (m *MemoryStorerImplementation) Get(key string) (string, fferr.GRPCError) {
	if key == "" {
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("key is empty"))
	}

	value, ok := m.Storage[key]
	if !ok {
		return "", fferr.NewKeyNotFoundError(key, nil)
	}

	return value, nil
}

func (m *MemoryStorerImplementation) List(prefix string) (map[string]string, fferr.GRPCError) {
	result := make(map[string]string)

	for key, value := range m.Storage {
		if strings.HasPrefix(key, prefix) {
			result[key] = value
		}
	}

	return result, nil
}

func (m *MemoryStorerImplementation) Delete(key string) (string, fferr.GRPCError) {
	if key == "" {
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("key is empty"))
	}

	value, ok := m.Storage[key]
	if !ok {
		return "", fferr.NewKeyNotFoundError(key, nil)
	}

	delete(m.Storage, key)

	return value, nil
}
