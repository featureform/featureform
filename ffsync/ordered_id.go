package ffsync

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type OrderedId interface {
	Equals(other OrderedId) bool
	Less(other OrderedId) bool
	String() string
	FromString(id string) error
	Value() interface{} // Value returns the underlying value of the ordered ID
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(data []byte) error
}

const etcd_id_key = "FFSync/ID" // Key for the etcd ID generator, will add namespace to the end

type Uint64OrderedId uint64

func (id *Uint64OrderedId) Equals(other OrderedId) bool {
	return id.Value() == other.Value()
}

func (id *Uint64OrderedId) Less(other OrderedId) bool {
	if otherID, ok := other.(*Uint64OrderedId); ok {
		return *id < *otherID
	}
	return false
}

func (id *Uint64OrderedId) String() string {
	return fmt.Sprint(id.Value())
}

func (id *Uint64OrderedId) FromString(strID string) error {
	tmp, err := strconv.ParseUint(strID, 10, 64)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	*id = Uint64OrderedId(tmp)
	return nil
}

func (id *Uint64OrderedId) Value() interface{} {
	return uint64(*id)
}

func (id *Uint64OrderedId) UnmarshalJSON(data []byte) error {
	var tmp uint64
	if err := json.Unmarshal(data, &tmp); err != nil {
		return fferr.NewInternalError(fmt.Errorf("failed to unmarshal uint64OrderedId: %v", err))
	}

	*id = Uint64OrderedId(tmp)
	return nil
}

func (id *Uint64OrderedId) MarshalJSON() ([]byte, error) {
	return json.Marshal(uint64(*id))
}

type OrderedIdGenerator interface {
	NextId(namespace string) (OrderedId, error)
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

func (m *memoryIdGenerator) NextId(namespace string) (OrderedId, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Initialize the counter if it doesn't exist
	if _, ok := m.counters[namespace]; !ok {
		var initialId Uint64OrderedId = 0
		m.counters[namespace] = &initialId
	}

	// Atomically increment the counter and return the next ID
	nextId := atomic.AddUint64((*uint64)(m.counters[namespace]), 1)
	return (*Uint64OrderedId)(&nextId), nil
}

func (m *memoryIdGenerator) Close() {
	// No-op
}

func NewETCDOrderedIdGenerator(config helpers.ETCDConfig) (OrderedIdGenerator, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{config.URL()},
	})
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to create etcd client: %w", err))
	}

	session, err := concurrency.NewSession(client)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to create etcd session: %w", err))
	}

	return &etcdIdGenerator{
		client:  client,
		session: session,
		ctx:     context.Background(),
	}, nil
}

type etcdIdGenerator struct {
	client  *clientv3.Client
	session *concurrency.Session
	ctx     context.Context
}

func (etcd *etcdIdGenerator) NextId(namespace string) (OrderedId, error) {
	if namespace == "" {
		return nil, fferr.NewInternalError(fmt.Errorf("cannot generate ID for empty namespace"))
	}

	// Lock the namespace to prevent concurrent ID generation
	lockKey := createLockKey(etcd_id_key, namespace)
	lockMutex := concurrency.NewMutex(etcd.session, lockKey)
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
		nextId, err := strconv.ParseUint(nextIdStr, 10, 64)
		if err != nil {
			return nil, fferr.NewInternalError(fmt.Errorf("failed to parse ID as uint64: %s: %v", nextIdStr, err))
		}
		nextId++
	}

	// Update the ID
	if _, err := etcd.client.Put(etcd.ctx, lockKey, fmt.Sprint(nextId)); err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to set key %s: %w", lockKey, err))
	}

	return (*Uint64OrderedId)(&nextId), nil
}

func (etcd *etcdIdGenerator) Close() {
	etcd.session.Close()
	etcd.client.Close()
}

func NewRDSOrderedIdGenerator(config helpers.RDSConfig) (OrderedIdGenerator, error) {
	const tableName = "ff_ordered_id"

	db, err := helpers.NewRDSPoolConnection(config)
	if err != nil {
		return nil, err
	}

	// acquire a connection pool
	connection, err := db.Acquire(context.Background())
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to acquire connection from the database pool: %w", err))
	}

	// ping the database
	err = connection.Ping(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to ping the database: %w", err)
	}

	// Create the id table if it doesn't exist
	tableCreationSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (namespace VARCHAR(2048) PRIMARY KEY, current_id BIGINT)", helpers.SanitizePostgres(tableName))
	_, err = db.Exec(context.Background(), tableCreationSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	return &rdsIdGenerator{
		db:         db,
		tableName:  tableName,
		connection: connection,
	}, nil
}

type rdsIdGenerator struct {
	db         *pgxpool.Pool
	tableName  string
	connection *pgxpool.Conn
}

func (rds *rdsIdGenerator) NextId(namespace string) (OrderedId, error) {
	if namespace == "" {
		return nil, fmt.Errorf("cannot generate ID for empty namespace")
	}

	var nextId int64
	upsertIdQuery := rds.upsertIdQuery()
	err := rds.db.QueryRow(context.Background(), upsertIdQuery, namespace, 1).Scan(&nextId)
	if err != nil {
		return nil, fmt.Errorf("failed to update key %s: %w", namespace, err)
	}

	id := Uint64OrderedId(nextId)

	return &id, nil
}

func (rds *rdsIdGenerator) Close() {
	rds.connection.Release()
	rds.db.Close()
}

// SQL Queries
func (rds *rdsIdGenerator) upsertIdQuery() string {
	return fmt.Sprintf("INSERT INTO %s (namespace, current_id) VALUES ($1, $2) ON CONFLICT (namespace) DO UPDATE SET current_id = %s.current_id + 1 RETURNING current_id", helpers.SanitizePostgres(rds.tableName), helpers.SanitizePostgres(rds.tableName))
}

func createLockKey(prefix, namespace string) string {
	return fmt.Sprintf("%s/%s", strings.TrimSuffix(prefix, "/"), strings.TrimPrefix(namespace, "/"))
}
