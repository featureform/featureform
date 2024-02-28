package scheduling

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	LOCKPREFIX = "/ff_lock" // Lock prefix, don't end with / as it will be added in the code
)

type ETCDStorageProvider struct {
	client *clientv3.Client
	ctx    context.Context
}

func NewETCDStorageProvider(client *clientv3.Client, ctx context.Context) *ETCDStorageProvider {
	return &ETCDStorageProvider{client: client, ctx: ctx}
}

func (etcd *ETCDStorageProvider) Set(key string, value string, lock LockObject) error {
	if key == "" {
		return fmt.Errorf("key is empty")
	}
	if value == "" {
		return fmt.Errorf("value is empty for key %s", key)
	}

	// get lock and check if it is valid
	lockKeyPath := fmt.Sprintf("/%s/%s", LOCKPREFIX, strings.TrimLeft(key, "/"))
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
	lockKeyPath := fmt.Sprintf("/%s/%s", LOCKPREFIX, strings.TrimLeft(key, "/"))
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
	if lock.Channel != nil {
		close(*lock.Channel)
	}
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
	lockKeyPath := fmt.Sprintf("/%s/%s", LOCKPREFIX, strings.TrimLeft(key, "/"))

	// check if the key is already locked
	resp, err := etcd.client.Get(etcd.ctx, lockKeyPath)
	if err != nil {
		return LockObject{}, fmt.Errorf("failed to get key %s: %w", lockKeyPath, err)
	}

	if len(resp.Kvs) != 0 {
		lock := LockInformation{}
		if err := lock.Unmarshal(resp.Kvs[0].Value); err != nil {
			return LockObject{}, fmt.Errorf("failed to unmarshal lock information: %w", err)
		}
		if time.Since(lock.Date) < ValidTimePeriod {
			return LockObject{}, fmt.Errorf("key is already locked by: %s", lock.ID)
		}
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

	// unmarshal the lock information
	newLock := LockInformation{}
	if err := newLock.Unmarshal([]byte(data)); err != nil {
		return LockObject{}, fmt.Errorf("failed to unmarshal lock information: %w", err)
	}
	fmt.Println("Old Lock: ", lock.Date, "unmarshal lock: ", newLock.Date, lock, newLock)

	_, err = etcd.client.Put(etcd.ctx, lockKeyPath, string(data))
	if err != nil {
		return LockObject{}, fmt.Errorf("failed to put lock information: %w", err)
	}

	lockChannel := make(chan error)
	go etcd.updateLockTime(id, lockKeyPath, lockChannel)
	lockObject := LockObject{ID: id, Channel: &lockChannel}

	return lockObject, nil
}

func (etcd *ETCDStorageProvider) Unlock(key string, lock LockObject) error {
	if key == "" {
		return fmt.Errorf("key is empty")
	}

	lockKeyPath := fmt.Sprintf("/%s/%s", LOCKPREFIX, strings.TrimLeft(key, "/"))

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

	_, err = etcd.client.Delete(etcd.ctx, lockKeyPath)
	if err != nil {
		return fmt.Errorf("failed to delete key %s: %w", lockKeyPath, err)
	}

	// close the lock channel
	if lock.Channel != nil {
		close(*lock.Channel)
	}
	return nil
}

func (etcd *ETCDStorageProvider) updateLockTime(id string, key string, lockChannel chan error) {
	ticker := time.NewTicker(UpdateSleepTime)
	defer ticker.Stop()

	for {
		time.Sleep(UpdateSleepTime)

		select {
		case <-lockChannel:
			// Received signal to stop
			if lockChannel != nil {
				return
			}
		case <-ticker.C:
			// Continue updating lock time
			lockInfo, err := etcd.client.Get(etcd.ctx, key)
			if err != nil {
				// Key no longer exists, stop updating
				return
			}

			lock := LockInformation{}
			if err := lock.Unmarshal(lockInfo.Kvs[0].Value); err != nil {
				return
			}
			if lock.ID == id {
				// Update lock time
				lock.Date = time.Now()
				data, err := lock.Marshal()
				if err != nil {
					return
				}

				_, err = etcd.client.Put(etcd.ctx, key, string(data))
				if err != nil {
					return
				}
			}
		}
	}
}
