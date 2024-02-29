package scheduling

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/featureform/helpers"
)

const (
	LOCKPREFIX = "/ff_lock" // Lock prefix, don't end with / as it will be added in the code
)

type ETCDStorageProvider struct {
	client       *clientv3.Client
	ctx          context.Context
	lockMutex    sync.Mutex
	updateTicker *time.Ticker
}

func NewETCDStorageProvider() (*ETCDStorageProvider, error) {
	etcdHost := helpers.GetEnv("ETCD_HOST", "localhost")
	etcdPort := helpers.GetEnv("ETCD_PORT", "2379")
	etcdUsername := helpers.GetEnv("ETCD_USERNAME", "")
	etcdPassword := helpers.GetEnv("ETCD_PASSWORD", "")

	address := fmt.Sprintf("%s:%s", etcdHost, etcdPort)

	etcdConfig := clientv3.Config{
		Endpoints:   []string{address},
		DialTimeout: time.Second * 10,
		Username:    etcdUsername,
		Password:    etcdPassword,
	}

	client, err := clientv3.New(etcdConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	etcd := ETCDStorageProvider{
		client:       client,
		ctx:          context.Background(),
		updateTicker: time.NewTicker(UpdateSleepTime),
		lockMutex:    sync.Mutex{},
	}
	return &etcd, nil
}

func (etcd *ETCDStorageProvider) Set(key string, value string, lock LockObject) error {
	if key == "" {
		return fmt.Errorf("key is empty")
	}
	if value == "" {
		return fmt.Errorf("value is empty for key %s", key)
	}

	// get lock and check if it is valid
	lockKeyPath := etcd.getLockPathForKey(key)
	resp, err := etcd.client.Get(etcd.ctx, lockKeyPath)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", lockKeyPath, err)
	}

	if len(resp.Kvs) == 0 {
		return fmt.Errorf("key is not locked")
	}

	lockInfo := LockInformation{}
	if err := lockInfo.Unmarshal(resp.Kvs[0].Value); err != nil {
		return fmt.Errorf("failed to unmarshal lock information: %w", err)
	}
	if lockInfo.ID != lock.ID {
		return fmt.Errorf("key is locked by another id: locked by: %s, unlock  by: %s", lockInfo.ID, lock.ID)
	}

	_, err = etcd.client.Put(etcd.ctx, key, value)
	return err
}

func (etcd *ETCDStorageProvider) Get(key string, prefix bool) (map[string]string, error) {
	if key == "" {
		return nil, fmt.Errorf("key is empty")
	}

	result := make(map[string]string)
	if !prefix {
		resp, err := etcd.client.Get(etcd.ctx, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get key %s: %w", key, err)
		}
		if len(resp.Kvs) == 0 {
			return nil, &KeyNotFoundError{Key: key}
		}

		result[key] = string(resp.Kvs[0].Value)
		return result, nil
	}

	resp, err := etcd.client.Get(etcd.ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get keys with prefix %s: %w", key, err)
	}

	for _, kv := range resp.Kvs {
		result[string(kv.Key)] = string(kv.Value)
	}
	if len(result) == 0 {
		return nil, &KeyNotFoundError{Key: key}
	}
	return result, nil
}

func (etcd *ETCDStorageProvider) Delete(key string, lock LockObject) error {
	if key == "" {
		return fmt.Errorf("key is empty")
	}

	// get lock and check if it is valid
	lockKeyPath := etcd.getLockPathForKey(key)
	resp, err := etcd.client.Get(etcd.ctx, lockKeyPath)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", lockKeyPath, err)
	}

	if len(resp.Kvs) == 0 {
		return fmt.Errorf("key is not locked")
	}

	lockInfo := LockInformation{}
	if err := lockInfo.Unmarshal(resp.Kvs[0].Value); err != nil {
		return fmt.Errorf("failed to unmarshal lock information: %w", err)
	}
	if lockInfo.ID != lock.ID {
		return fmt.Errorf("key is locked by another id: locked by: %s, unlock  by: %s", lockInfo.ID, lock.ID)
	}

	_, err = etcd.client.Delete(etcd.ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}

	_, err = etcd.client.Delete(etcd.ctx, lockKeyPath)
	if err != nil {
		return fmt.Errorf("failed to delete key %s: %w", lockKeyPath, err)
	}

	// close the lock channel
	closeOnce(*lock.Channel)
	return nil
}

func (etcd *ETCDStorageProvider) ListKeys(prefix string) ([]string, error) {
	resp, err := etcd.client.Get(etcd.ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get keys with prefix %s: %w", prefix, err)
	}

	var result []string
	for _, kv := range resp.Kvs {
		result = append(result, string(kv.Key))
	}
	sort.Strings(result)

	return result, nil
}

func (etcd *ETCDStorageProvider) Lock(key string) (LockObject, error) {
	if key == "" {
		return LockObject{}, fmt.Errorf("key is empty")
	}

	lockKeyPath := etcd.getLockPathForKey(key)

	etcd.lockMutex.Lock()
	defer etcd.lockMutex.Unlock()

	if _, err := etcd.getLockedItem(lockKeyPath); err == nil {
		return LockObject{}, fmt.Errorf("key is already locked")
	}

	id := uuid.New().String()
	lock := LockInformation{
		ID:   id,
		Key:  key,
		Date: time.Now().UTC(),
	}

	data, err := lock.Marshal()
	if err != nil {
		return LockObject{}, fmt.Errorf("failed to marshal lock information: %w", err)
	}

	if _, err := etcd.client.Put(etcd.ctx, lockKeyPath, string(data)); err != nil {
		return LockObject{}, fmt.Errorf("failed to put lock information: %w", err)
	}

	lockChannel := make(chan error)
	go etcd.updateLockTime(id, lockKeyPath, lockChannel)
	return LockObject{ID: id, Channel: &lockChannel}, nil
}

func (etcd *ETCDStorageProvider) Unlock(key string, lock LockObject) error {
	if key == "" {
		return fmt.Errorf("key is empty")
	}

	lockKeyPath := etcd.getLockPathForKey(key)

	etcd.lockMutex.Lock()
	defer etcd.lockMutex.Unlock()

	lockInfo, err := etcd.getLockedItem(lockKeyPath)
	if err != nil {
		return fmt.Errorf("failed to get locked item: %w", err)
	}

	if lockInfo.ID != lock.ID {
		return fmt.Errorf("key is locked by another id: locked by: %s, unlock by: %s", lockInfo.ID, lock.ID)
	}

	if _, err := etcd.client.Delete(etcd.ctx, lockKeyPath); err != nil {
		return fmt.Errorf("failed to delete key %s: %w", lockKeyPath, err)
	}

	// Close the lock channel
	closeOnce(*lock.Channel)

	return nil
}

func (etcd *ETCDStorageProvider) getLockedItem(key string) (LockInformation, error) {
	resp, err := etcd.client.Get(etcd.ctx, key)
	if err != nil {
		return LockInformation{}, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	if len(resp.Kvs) == 0 {
		return LockInformation{}, fmt.Errorf("key is not locked")
	}

	lock := LockInformation{}
	if err := lock.Unmarshal(resp.Kvs[0].Value); err != nil {
		return LockInformation{}, fmt.Errorf("failed to unmarshal lock information: %w", err)
	}

	return lock, nil
}

func (etcd *ETCDStorageProvider) updateLockTime(id string, key string, lockChannel chan error) {
	for {
		select {
		case <-lockChannel:
			// Received signal to stop
			return
		case <-etcd.updateTicker.C:
			// Continue updating lock time
			lockInfo, err := etcd.getLockedItem(key)
			if err != nil {
				// Key no longer exists, stop updating
				return
			}

			if lockInfo.ID == id {
				// Update lock time
				lockInfo.Date = time.Now().UTC()
				data, err := lockInfo.Marshal()
				if err != nil {
					return
				}

				if _, err := etcd.client.Put(etcd.ctx, key, string(data)); err != nil {
					return
				}
			}
		}
	}
}

func (etcd *ETCDStorageProvider) getLockPathForKey(key string) string {
	return fmt.Sprintf("/%s/%s", LOCKPREFIX, strings.TrimLeft(key, "/"))
}
