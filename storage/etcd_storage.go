package storage

import (
	"context"
	"fmt"
	"net/url"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func NewETCDStorageImplementation() (metadataStorageImplementation, error) {
	etcdHost := helpers.GetEnv("ETCD_HOST", "localhost")
	etcdPort := helpers.GetEnv("ETCD_PORT", "2379")

	etcdHostPort := fmt.Sprintf("%s:%s", etcdHost, etcdPort)

	etcdURL := url.URL{
		Scheme: "http",
		Host:   etcdHostPort,
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{etcdURL.String()},
	})
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to create etcd client: %w", err))
	}

	return &etcdStorageImplementation{
		client: client,
		ctx:    context.Background(),
	}, nil
}

type etcdStorageImplementation struct {
	client *clientv3.Client
	ctx    context.Context
}

func (etcd *etcdStorageImplementation) Set(key string, value string) error {
	if key == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("cannot set an empty key"))
	}

	_, err := etcd.client.Put(etcd.ctx, key, value)
	if err != nil {
		return fferr.NewInternalError(fmt.Errorf("failed to set key %s: %w", key, err))
	}

	return nil
}

func (etcd *etcdStorageImplementation) Get(key string) (string, error) {
	if key == "" {
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("cannot get an empty key"))
	}

	resp, err := etcd.client.Get(etcd.ctx, key)
	if err != nil {
		return "", fferr.NewInternalError(fmt.Errorf("failed to get key %s: %w", key, err))
	}
	if len(resp.Kvs) == 0 {
		return "", fferr.NewKeyNotFoundError(key, nil)
	}

	return string(resp.Kvs[0].Value), nil
}

func (etcd *etcdStorageImplementation) List(prefix string) (map[string]string, error) {
	resp, err := etcd.client.Get(etcd.ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to get keys with prefix %s: %w", prefix, err))
	}

	if len(resp.Kvs) == 0 {
		return nil, fferr.NewKeyNotFoundError(prefix, nil)
	}

	result := make(map[string]string)
	for _, kv := range resp.Kvs {
		result[string(kv.Key)] = string(kv.Value)
	}

	return result, nil
}

func (etcd *etcdStorageImplementation) Delete(key string) (string, error) {
	if key == "" {
		return "", fferr.NewInvalidArgumentError(fmt.Errorf("cannot delete empty key"))
	}

	resp, err := etcd.client.Get(etcd.ctx, key)
	if err != nil {
		return "", fferr.NewInternalError(fmt.Errorf("failed to get key %s: %w", key, err))
	}
	if len(resp.Kvs) == 0 {
		return "", fferr.NewKeyNotFoundError(key, nil)
	}

	_, err = etcd.client.Delete(etcd.ctx, key)
	if err != nil {
		return "", fferr.NewInternalError(fmt.Errorf("failed to delete key %s: %w", key, err))
	}

	return string(resp.Kvs[0].Value), nil
}
