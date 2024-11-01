// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package ffsync

import (
	"context"
	"fmt"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	"github.com/featureform/logging"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const etcdLoggerKey = "etcdLogger"

type etcdKey struct {
	id        string
	key       string
	lockMutex *concurrency.Mutex
	lease     *clientv3.LeaseGrantResponse
	session   *concurrency.Session
}

func (k etcdKey) ID() string {
	return k.id
}

func (k etcdKey) Key() string {
	return k.key
}

func NewETCDLocker(config helpers.ETCDConfig) (Locker, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{config.URL()},
		Username:  config.Username,
		Password:  config.Password,
	})
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to create etcd client: %w", err))
	}

	ctx := context.Background()

	return &etcdLocker{
		client: client,
		ctx:    ctx,
		logger: logging.NewLogger("ffsync.etcdLocker"),
	}, nil
}

type etcdLocker struct {
	client *clientv3.Client
	ctx    context.Context
	logger logging.Logger
}

func (m *etcdLocker) cancelableWaitTime(key string) error {
	startTime := time.Now()
	for {
		if hasExceededWaitTime(startTime) {
			return fferr.NewExceededWaitTimeError("etcd", key)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// blockingLock will attempt to lock a key and wait if the key is already locked
func (m *etcdLocker) blockingLock(ctx context.Context, lockMutex *concurrency.Mutex, key string) error {
	logger := ctx.Value(etcdLoggerKey).(*zap.SugaredLogger)
	logger.Debug("Locking Key with wait")
	var err error
	go func() {
		if err = m.cancelableWaitTime(key); err != nil {
			m.ctx.Done()
		}
	}()

	logger.Debug("Attempting to lock mutex")
	if err := lockMutex.Lock(m.ctx); err != nil {
		logger.Error("Failed to lock key, because of error", "error", err.Error())
		return fferr.NewInternalError(fmt.Errorf("failed to lock key %s: %w", key, err))
	}
	return nil
}

// nonBlockingLock will attempt to lock a key and will return a KeyAlreadyLockedError if the key is
// already locked
func (m *etcdLocker) nonBlockingLock(ctx context.Context, lockMutex *concurrency.Mutex, key string) error {
	logger := ctx.Value(etcdLoggerKey).(*zap.SugaredLogger)
	logger.Debug("Locking Key without wait")
	if err := lockMutex.TryLock(m.ctx); err != nil {
		if err == concurrency.ErrLocked {
			logger.Error("Failed to lock key, key is already locked")
			return fferr.NewKeyAlreadyLockedError(key, lockMutex.Key(), nil)
		} else {
			logger.Errorw("Failed to lock key", "error", err, "key", key, "lock_id", lockMutex.Key())
			return fferr.NewInternalError(fmt.Errorf("failed to lock key %s: %w", key, err))
		}
	}
	return nil
}

func (m *etcdLocker) Lock(ctx context.Context, key string, wait bool) (Key, error) {
	logger := m.logger.With("key", key, "wait", wait, "request_id", ctx.Value("request_id"))
	ctx = context.WithValue(ctx, etcdLoggerKey, logger)
	logger.Debug("Locking Key")

	if key == "" {
		return nil, fferr.NewLockEmptyKeyError()
	}

	lease, err := m.client.Grant(ctx, ValidTimePeriod.Duration().Milliseconds())
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to grant lease: %w", err))
	}

	leaseKeepAliveChan, err := m.client.KeepAlive(ctx, lease.ID)
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

	session, err := concurrency.NewSession(m.client, concurrency.WithLease(lease.ID))
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to create session: %w", err))
	}

	lockMutex := concurrency.NewMutex(session, "key/"+key)
	if wait {
		if err := m.blockingLock(ctx, lockMutex, key); err != nil {
			return nil, err
		}
	} else {
		if err := m.nonBlockingLock(ctx, lockMutex, key); err != nil {
			return nil, err
		}
	}

	lockKey := etcdKey{
		id:        lockMutex.Key(),
		key:       key,
		lockMutex: lockMutex,
		lease:     lease,
		session:   session,
	}
	logger.Debug("Key locked successfully")
	return lockKey, nil
}

func (m *etcdLocker) Unlock(ctx context.Context, key Key) error {
	logger := m.logger.With("key", key.Key(), "request_id", ctx.Value("request_id"))
	logger.Debug("Unlocking key")

	if key == nil {
		return fferr.NewInternalError(fmt.Errorf("cannot unlock a nil key"))
	}
	if key.Key() == "" {
		return fferr.NewUnlockEmptyKeyError()
	}

	etcdKey, ok := key.(etcdKey)
	if !ok {
		return fferr.NewInternalError(fmt.Errorf("key is not an etcd key: %v", key.Key()))
	}

	logger.Debug("Attempting to unlock mutex")
	if err := etcdKey.lockMutex.Unlock(m.ctx); err != nil {
		logger.Error("Failed to unlock key, because of error", "error", err.Error())
		return fferr.NewInternalError(fmt.Errorf("failed to unlock key %s: %w", key.Key(), err))
	}

	m.client.Revoke(m.ctx, etcdKey.lease.ID)
	etcdKey.session.Close()
	logger.Debug("Key unlocked successfully")

	return nil
}

func (m *etcdLocker) Close() {
	m.client.Close()
}
