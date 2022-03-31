package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type postgresTables struct {
	conn *pgx.Conn
	ctx  context.Context
}

type postgresOfflineStore struct {
	conn   *pgx.Conn
	ctx    context.Context
	tables postgresTables
	BaseProvider
}

// create() creates an index that tracks currently
// active resource tables.
func (table postgresTables) create() error {
	return errors.New("create() not implemented")
}

// get() takes a ResourceID and returns an OfflineTable.
// Returns nil if the table is not found and returns an error
// if there is an error fetching the table.
func (table postgresTables) get(id ResourceID) (OfflineTable, error) {
	return nil, errors.New("get() not implemented")
}

// set() adds the table to
func (table postgresTables) set(t OfflineTable) error {
	return errors.New("set() not implemented")
}

type PostgresConfig struct {
	Host string
	Port string
}

func (pg *PostgresConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, pg)
	if err != nil {
		return err
	}
	return nil
}

func postgresOfflineStoreFactory(config SerializedConfig) (Provider, error) {
	pg := PostgresConfig{}
	if err := pg.Deserialize(config); err != nil {
		return nil, errors.New("invalid postgres config")
	}
	store, err := NewPostgresOfflineStore(pg)
	if err != nil {
		return nil, err
	}
	return store, nil
}

// NewPostgresOfflineStore creates a connection to a postgres database
// and initializes a table to track currently active Resource tables.
func NewPostgresOfflineStore(pg PostgresConfig) (*postgresOfflineStore, error) {
	url := fmt.Sprintf("%s:%s", pg.Host, pg.Port)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url) //Change this
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	tables := postgresTables{
		conn: conn,
		ctx:  ctx,
	}
	if err := tables.create(); err != nil {
		return nil, err
	}
	return &postgresOfflineStore{
		conn:   conn,
		ctx:    ctx,
		tables: tables,
	}, nil
}

// Clarify what this method should return
func (store *postgresOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

// CreateResourceTable creates a new Resource table.
// Returns a table if it does not already exist and stores the table ID in the resource index table.
// Returns an error if the table already exists or if table is the wrong type.
func (store *postgresOfflineStore) CreateResourceTable(id ResourceID) (OfflineTable, error) {
	if err := id.Check(); err != nil { // modify this after rebasing
		return nil, err
	}
	var n int64
	// Can replace this to use the resource table index
	err := store.conn.QueryRow(store.ctx, "select 1 from information_schema.tables where table_name=$1", id.Name).Scan(&n)
	if err == nil {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	//check if ctx is needed
	table, err := newPostgresOfflineTable(store.conn, store.ctx, id.Name)
	if err != nil {
		return nil, err
	}
	if err := store.tables.set(table); err != nil {
		return nil, err
	}
	return table, nil
}

func (store *postgresOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	table, err := store.tables.get(id)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, &TableNotFound{id.Name, id.Variant}
	}
	return table, nil
}

type postgresOfflineTable struct {
	conn *pgx.Conn
	ctx  context.Context
	name string
}

func newPostgresOfflineTable(conn *pgx.Conn, ctx context.Context, name string) (*postgresOfflineTable, error) {
	_, err := conn.Query(ctx, "CREATE TABLE $1 ("+
		"Entity VARCHAR ( 255 ) PRIMARY KEY,"+
		"Value JSON NOT NULL,"+
		"TS TIMESTAMP NOT NULL,)")
	if err != nil {
		return nil, err
	}
	return &postgresOfflineTable{
		conn: conn,
		name: name,
	}, nil
}

func (table *postgresOfflineTable) Write(rec ResourceRecord) error {
	if err := rec.Check(); err != nil {
		return err
	}
	rows, err := table.conn.Query(table.ctx, "SELECT * FROM $1 WHERE Entity=$2 AND TS=$3", table.name, rec.Entity, rec.TS)
	if err != nil {
		return err
	}
	for rows.Next() {

	}
	return nil
}

func (table *postgresOfflineTable) serialize(rec ResourceRecord) ([]byte, error) {
	msg, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (table *postgresOfflineTable) deserialize(msg []byte) (ResourceRecord, error) {
	rec := ResourceRecord{}
	if err := json.Unmarshal(msg, &rec); err != nil {
		return ResourceRecord{}, err
	}
	return rec, nil
}
