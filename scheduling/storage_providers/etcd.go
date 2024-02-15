package scheduling

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

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
