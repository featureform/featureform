// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package ffsync

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers/etcd"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type OrderedId interface {
	Equals(other any) bool
	Less(other any) bool
	String() string
	Value() interface{} // Value returns the underlying value of the ordered ID
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(data []byte) (OrderedId, error)
}

const etcd_id_key = "/FFSync/ID" // Key for the etcd ID generator, will add namespace to the end

type Uint64OrderedId uint64

func (id Uint64OrderedId) Equals(other any) bool {
	if otherID, ok := other.(Uint64OrderedId); ok {
		return id == otherID
	}
	return false
}

func (id Uint64OrderedId) Less(other any) bool {
	if otherID, ok := other.(Uint64OrderedId); ok {
		return id < otherID
	}
	return false
}

func (id Uint64OrderedId) String() string {
	return fmt.Sprint(id.Value())
}

func (id Uint64OrderedId) Value() interface{} {
	return uint64(id)
}

func (id Uint64OrderedId) UnmarshalJSON(data []byte) (OrderedId, error) {
	var tmp uint64
	if err := json.Unmarshal(data, &tmp); err != nil {
		return Uint64OrderedId(0), fferr.NewInternalError(fmt.Errorf("failed to unmarshal uint64OrderedId: %v", err))
	}
	return Uint64OrderedId(tmp), nil
}

func (id Uint64OrderedId) MarshalJSON() ([]byte, error) {
	return json.Marshal(uint64(id))
}

type OrderedIdGenerator interface {
	NextId(ctx context.Context, namespace string) (OrderedId, error)
	Close()
}

func NewMemoryOrderedIdGenerator() (OrderedIdGenerator, error) {
	return &memoryIdGenerator{
		counters: make(map[string]*Uint64OrderedId),
		mu:       sync.Mutex{},
	}, nil
}

type memoryIdGenerator struct {
	counters map[string]*Uint64OrderedId
	mu       sync.Mutex
}

func (m *memoryIdGenerator) NextId(ctx context.Context, namespace string) (OrderedId, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Initialize the counter if it doesn't exist
	if _, ok := m.counters[namespace]; !ok {
		var initialId Uint64OrderedId = 0
		m.counters[namespace] = &initialId
	}

	// Atomically increment the counter and return the next ID
	nextId := atomic.AddUint64((*uint64)(m.counters[namespace]), 1)
	return Uint64OrderedId(nextId), nil
}

func (m *memoryIdGenerator) Close() {
	// No-op
}

func NewETCDOrderedIdGenerator(config etcd.Config) (OrderedIdGenerator, error) {
	timeout := func() time.Duration {
		if config.DialTimeout == 0 {
			return 5 * time.Second
		}
		return config.DialTimeout
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{config.URL()},
		Username:    config.Username,
		Password:    config.Password,
		DialTimeout: timeout(),
	})
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to create etcd client: %w", err))
	}

	return &etcdIdGenerator{
		client: client,
		ctx:    context.Background(),
	}, nil
}

type etcdIdGenerator struct {
	client *clientv3.Client
	ctx    context.Context
}

func (etcd *etcdIdGenerator) NextId(ctx context.Context, namespace string) (OrderedId, error) {
	if namespace == "" {
		return nil, fferr.NewInternalError(fmt.Errorf("cannot generate ID for empty namespace"))
	}

	// Lock the namespace to prevent concurrent ID generation
	// Create a session
	// ETCD locks by session, so we when a lock tried to overwrite an old lock, it allowed it because etcd saw it as the session owner unlocking its own key.
	// Therefore, it is necessary to create a new session for each lock. This is the same issue we saw with the ETCD lockers.
	session, err := concurrency.NewSession(etcd.client)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to create etcd session: %w", err))
	}
	defer session.Close()

	lockKey := createLockKey(etcd_id_key, namespace)
	lockMutex := concurrency.NewMutex(session, lockKey)
	if err := lockMutex.Lock(etcd.ctx); err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to lock key %s: %w", lockKey, err))
	}
	defer lockMutex.Unlock(etcd.ctx)

	// Get the current ID
	resp, err := etcd.client.Get(etcd.ctx, lockKey)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to get key %s: %w", lockKey, err))
	}

	// Initialize the ID if it doesn't exist
	idNotInitialized := len(resp.Kvs) == 0
	var nextId uint64
	if idNotInitialized {
		nextId = 1
	} else {
		nextIdStr := string(resp.Kvs[0].Value)
		nextId, err = strconv.ParseUint(nextIdStr, 10, 64)
		if err != nil {
			return nil, fferr.NewInternalError(fmt.Errorf("failed to parse ID as uint64: %s: %v", nextIdStr, err))
		}
		nextId++
	}

	// Update the ID
	if _, err := etcd.client.Put(etcd.ctx, lockKey, fmt.Sprint(nextId)); err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to set key %s: %w", lockKey, err))
	}

	return (Uint64OrderedId)(nextId), nil
}

func (etcd *etcdIdGenerator) Close() {
	etcd.client.Close()
}

func NewPSQLOrderedIdGenerator(
	ctx context.Context, connPool *postgres.Pool,
) (OrderedIdGenerator, error) {
	const tableName = "ff_ordered_id"
	logger := logging.GetLoggerFromContext(ctx).WithValues(map[string]any{
		"ordered-id-table-name": tableName,
	})
	// Create the id table if it doesn't exist
	// TODO(ali) move this to goose
	logger.Info("Creating ordered id table in postgres if it doesn't exist")
	tableCreationSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (namespace VARCHAR(2048) PRIMARY KEY, current_id BIGINT)", postgres.Sanitize(tableName))
	logger.Debugw("OrderedID table creation query", "query", tableCreationSQL)
	if _, err := connPool.Exec(context.Background(), tableCreationSQL); err != nil {
		logger.Errorw(
			"Failed to create OrderedID table in postgres",
			"query", tableCreationSQL, "error", err,
		)
		return nil, fferr.NewInternalErrorf("failed to create table %s: %w", tableName, err)
	}
	logger.Infow("Successfully created OrderedIDGenerator for postgres")
	return &pgIdGenerator{
		connPool:  connPool,
		tableName: tableName,
		logger:    logger,
	}, nil
}

type pgIdGenerator struct {
	connPool  *postgres.Pool
	tableName string
	logger    logging.Logger
}

func (pg *pgIdGenerator) NextId(ctx context.Context, namespace string) (OrderedId, error) {
	logger := pg.logger.WithRequestIDFromContext(ctx).With("ordered-id-namespace", namespace)
	logger.Debug("Getting next ordered ID from postgres")
	if namespace == "" {
		errMsg := "cannot generate ID for empty namespace"
		logger.Error(errMsg)
		return nil, fferr.NewInternalErrorf(errMsg)
	}

	var nextId int64
	upsertIdQuery := pg.upsertIdQuery()
	logger.Debugw("Running next ID postgres query", "query", upsertIdQuery)
	if err := pg.connPool.QueryRow(ctx, upsertIdQuery, namespace, 1).Scan(&nextId); err != nil {
		errMsg := "failed to get next ID in namespace"
		logger.Errorw(errMsg, "err", err)
		return nil, fferr.NewInternalErrorf("%s %s: %w", errMsg, namespace, err)
	}
	logger.Debugw("Got next ID", "next-id", nextId)
	id := Uint64OrderedId(nextId)
	return id, nil
}

func (pg *pgIdGenerator) Close() {
	// No-op
}

// SQL Queries
func (pg *pgIdGenerator) upsertIdQuery() string {
	sanitizedTableName := postgres.Sanitize(pg.tableName)
	return fmt.Sprintf("INSERT INTO %s (namespace, current_id) VALUES ($1, $2) ON CONFLICT (namespace) DO UPDATE SET current_id = %s.current_id + 1 RETURNING current_id", sanitizedTableName, sanitizedTableName)
}

func createLockKey(prefix, namespace string) string {
	return fmt.Sprintf("%s/%s", strings.TrimSuffix(prefix, "/"), strings.TrimPrefix(namespace, "/"))
}
