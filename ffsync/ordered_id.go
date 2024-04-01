package ffsync

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type OrderedId interface {
	Equals(other OrderedId) bool
	Less(other OrderedId) bool
	String() string
	Value() interface{} // Value returns the underlying value of the ordered ID
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(data []byte) error
}

type Uint64OrderedId uint64

const etcd_id_key = "FFSync/ID/" // Key for the etcd ID generator, will add namespace to the end

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

func NewETCDOrderedIdGenerator() (OrderedIdGenerator, error) {
	etcdHost := helpers.GetEnv("ETCD_HOST", "localhost")
	etcdPort := helpers.GetEnv("ETCD_PORT", "2379")

	etcdURL := fmt.Sprintf("http://%s:%s", etcdHost, etcdPort)

	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{etcdURL},
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
	lockKey := etcd_id_key + namespace
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
	var nextId uint64
	if len(resp.Kvs) == 0 {
		nextId = 1
	} else {
		if err := json.Unmarshal(resp.Kvs[0].Value, &nextId); err != nil {
			return nil, fferr.NewInternalError(fmt.Errorf("failed to unmarshal ID: %w", err))
		}
		nextId++
	}

	// Update the ID
	nextIdBytes, err := json.Marshal(nextId)
	if err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to marshal ID: %w", err))
	}
	if _, err := etcd.client.Put(etcd.ctx, lockKey, string(nextIdBytes)); err != nil {
		return nil, fferr.NewInternalError(fmt.Errorf("failed to set key %s: %w", lockKey, err))
	}

	return (*Uint64OrderedId)(&nextId), nil
}

func (etcd *etcdIdGenerator) Close() {
	etcd.session.Close()
	etcd.client.Close()
}

func NewRDSOrderedIdGenerator() (OrderedIdGenerator, error) {
	const tableName = "ff_ordered_id"

	db, err := helpers.NewRDSPoolConnection()
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
	tableCreationSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (namespace VARCHAR(255) PRIMARY KEY, current_id BIGINT)", tableName)
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

	tx, err := rds.db.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())

	var nextId int64
	updateSQL := rds.updateIdQuery()
	err = tx.QueryRow(context.Background(), updateSQL, namespace).Scan(&nextId)
	if err == pgx.ErrNoRows {
		nextId = 1
		insertSQL := rds.insertIdQuery()
		_, err = tx.Exec(context.Background(), insertSQL, namespace, nextId)
		if err != nil {
			return nil, fmt.Errorf("failed to insert new namespace %s: %w", namespace, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("failed to update key %s: %w", namespace, err)
	}

	err = tx.Commit(context.Background())
	if err != nil {
		return nil, fmt.Errorf("transaction commit failed: %w", err)
	}

	id := Uint64OrderedId(nextId)

	return &id, nil
}

func (rds *rdsIdGenerator) Close() {
	rds.connection.Release()
	rds.db.Close()
}

// SQL Queries
func (rds *rdsIdGenerator) updateIdQuery() string {
	return fmt.Sprintf("UPDATE %s SET current_id = current_id + 1 WHERE namespace = $1 RETURNING current_id", rds.tableName)
}

func (rds *rdsIdGenerator) insertIdQuery() string {
	return fmt.Sprintf("INSERT INTO %s (namespace, current_id) VALUES ($1, $2)", rds.tableName)
}
