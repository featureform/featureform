package ffsync

import (
	"context"
	"fmt"
	"net/url"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type etcdKey struct {
	id        string
	key       string
	lockMutex *concurrency.Mutex
}

func (k etcdKey) ID() string {
	return k.id
}

func (k etcdKey) Key() string {
	return k.key
}

func NewETCDLocker() (Locker, error) {
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

	ctx := context.Background()
	lease, err := client.Grant(ctx, int64(ValidTimePeriod))
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to grant lease: %w", err))
	}

	leaseKeepAliveChan, err := client.KeepAlive(ctx, lease.ID)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to keep alive lease: %w", err))
	}

	go func() {
		for {
			select {
			case _, ok := <-leaseKeepAliveChan:
				if !ok {
					return
				}
			}
		}
	}()

	session, err := concurrency.NewSession(client, concurrency.WithLease(lease.ID))
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to create session: %w", err))
	}

	return &etcdLocker{
		client:  client,
		ctx:     ctx,
		session: session,
		lease:   lease,
	}, nil
}

type etcdLocker struct {
	client  *clientv3.Client
	session *concurrency.Session
	ctx     context.Context
	lease   *clientv3.LeaseGrantResponse
}

func (m *etcdLocker) Lock(key string) (Key, error) {
	if key == "" {
		return nil, fferr.NewLockEmptyKeyError()
	}

	id := uuid.New().String()

	lockMutex := concurrency.NewMutex(m.session, key)
	if err := lockMutex.Lock(m.ctx); err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to lock key %s: %w", key, err))
	}

	lockKey := etcdKey{
		id:        id,
		key:       key,
		lockMutex: lockMutex,
	}

	return lockKey, nil
}

func (m *etcdLocker) Unlock(key Key) error {
	if key.Key() == "" {
		return fferr.NewUnlockEmptyKeyError()
	}

	etcdKey, ok := key.(etcdKey)
	if !ok {
		return fferr.NewInternalError(fmt.Errorf("key is not an etcd key: %v", key.Key()))
	}

	if err := etcdKey.lockMutex.Unlock(m.ctx); err != nil {
		return fferr.NewInternalError(fmt.Errorf("failed to unlock key %s: %w", key.Key(), err))
	}

	return nil
}
