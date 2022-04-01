package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type postgresResourceTables struct {
	conn *pgx.Conn
	ctx  context.Context
}

type postgresOfflineStore struct {
	conn   *pgx.Conn
	ctx    context.Context
	tables postgresResourceTables
	BaseProvider
}

// create() creates an index that tracks currently
// active resource tables.
func (table postgresResourceTables) create() error {
	return errors.New("create() not implemented")
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
	tables := postgresResourceTables{
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

func (store *postgresOfflineStore) getTableName(id ResourceID) string {
	return fmt.Sprintf("resource_%s", id.Name)
}

func (store *postgresOfflineStore) tableExists(id ResourceID) (bool, error) {
	var n int64
	err := store.conn.QueryRow(store.ctx, "select 1 from information_schema.tables where table_name=$1", store.getTableName(id)).Scan(&n)
	if err == pgx.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
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

	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if exists {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	//check if ctx is needed
	tableName := store.getTableName(id)
	table, err := newPostgresOfflineTable(store.conn, store.ctx, tableName)
	if err != nil {
		return nil, err
	}
	return table, nil
}

func (store *postgresOfflineStore) GetResourceTable(id ResourceID) (OfflineTable, error) {
	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if !exists {
		return nil, &TableNotFound{id.Name, id.Variant}
	}
	return &postgresOfflineTable{
		conn: store.conn,
		ctx:  store.ctx,
		name: store.getTableName(id),
	}, nil
}

type postgresOfflineTable struct {
	conn *pgx.Conn
	ctx  context.Context
	name string
}

func newPostgresOfflineTable(conn *pgx.Conn, ctx context.Context, name string) (*postgresOfflineTable, error) {
	_, err := conn.Query(ctx, ""+
		"CREATE TABLE $1 ("+
		"entity VARCHAR ( 255 ) PRIMARY KEY,"+
		"value JSON NOT NULL,"+
		"TS TIMESTAMP NOT NULL)")
	if err != nil {
		return nil, err
	}
	return &postgresOfflineTable{
		conn: conn,
		name: name,
	}, nil
}

// Check logic on this one
func (table *postgresOfflineTable) Write(rec ResourceRecord) error {
	if err := rec.Check(); err != nil {
		return err
	}
	_, err := table.conn.Query(table.ctx, ""+
		"IF EXISTS (SELECT * FROM $1 WHERE entity=$2 AND TS=$3)"+
		"BEGIN"+
		"UPDATE $1 SET value=$4 WHERE Entity=$2 AND TS=$3"+
		"END"+
		"ELSE"+
		"BEGIN"+
		"INSERT INTO $1 VALUES ($2, $4, $3)"+
		"END", table.name, rec.Entity, rec.TS, rec.Value)
	if err != nil {
		return err
	}
	return nil
}

//func (table *postgresOfflineTable) serialize(rec ResourceRecord) ([]byte, error) {
//	msg, err := json.Marshal(rec)
//	if err != nil {
//		return nil, err
//	}
//	return msg, nil
//}
//
//func (table *postgresOfflineTable) deserialize(msg []byte) (ResourceRecord, error) {
//	rec := ResourceRecord{}
//	if err := json.Unmarshal(msg, &rec); err != nil {
//		return ResourceRecord{}, err
//	}
//	return rec, nil
//}
