package scheduling

import (
	"context"
	"fmt"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type StorageProvider interface {
	Set(key string, value string) error
	Get(key string, prefix bool) ([]string, error)
}

type MemoryStorageProvider struct {
	storage map[string]string
}

func NewMemoryStorageProvider() *MemoryStorageProvider {
	return &MemoryStorageProvider{storage: make(map[string]string)}
}

func (m *MemoryStorageProvider) Set(key string, value string) error {
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
	if key == "" {
		return nil, fmt.Errorf("key cannot be empty")
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

type ETCDStorageProvider struct {
	client *clientv3.Client
	ctx    context.Context
}

func NewETCDStorageProvider(client *clientv3.Client, ctx context.Context) *ETCDStorageProvider {
	return &ETCDStorageProvider{client: client, ctx: ctx}
}

func (etcd *ETCDStorageProvider) Set(key string, value string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	if value == "" {
		return fmt.Errorf("value cannot be empty")
	}
	_, err := etcd.client.Put(etcd.ctx, key, value)
	return err
}

func (etcd *ETCDStorageProvider) Get(key string, prefix bool) ([]string, error) {
	if key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}
	if !prefix {
		resp, err := etcd.client.Get(etcd.ctx, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get key %s: %w", key, err)
		}
		if len(resp.Kvs) == 0 {
			return nil, &KeyNotFoundError{Key: key}
		}
		return []string{string(resp.Kvs[0].Value)}, nil
	}

	resp, err := etcd.client.Get(etcd.ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get keys with prefix %s: %w", key, err)
	}

	var result []string
	for _, kv := range resp.Kvs {
		result = append(result, string(kv.Value))
	}
	if len(result) == 0 {
		return nil, &KeyNotFoundError{Key: key}
	}
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
