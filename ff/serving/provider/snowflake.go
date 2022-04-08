package provider

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	db "github.com/jackc/pgx/v4"
	_ "github.com/snowflakedb/gosnowflake"
)

type SnowflakeConfig struct {
	Username     string
	Password     string
	Organization string
	Database     string
}

func (sf *SnowflakeConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, sf)
	if err != nil {
		return err
	}
	return nil
}

func (sf *SnowflakeConfig) Serialize() []byte {
	conf, err := json.Marshal(sf)
	if err != nil {
		panic(err)
	}
	return conf
}

type snowflakeOfflineStore struct {
	db *sql.DB
	BaseProvider
}

func snowflakeOfflineStoreFactory(config SerializedConfig) (Provider, error) {
	sc := SnowflakeConfig{}
	if err := sc.Deserialize(config); err != nil {
		return nil, errors.New("invalid snowflake config")
	}

	store, err := NewSnowflakeOfflineStore(sc)
	if err != nil {
		return nil, err
	}
	return store, nil
}

// NewSnowflakeOfflineStore creates a connection to a snowflake database
// and initializes a table to track currently active Resource tables.
func NewSnowflakeOfflineStore(sc SnowflakeConfig) (*snowflakeOfflineStore, error) {
	url := fmt.Sprintf("%s:%s@%s/%s", sc.Username, sc.Password, sc.Organization, sc.Database)
	db, err := sql.Open("snowflake", url)
	if err != nil {
		return nil, err
	}
	return &snowflakeOfflineStore{
		db: db,
	}, nil
}

func (store *snowflakeOfflineStore) getResourceTableName(id ResourceID) string {
	var idType string
	if id.Type == Feature {
		idType = "feature"
	} else {
		idType = "label"
	}
	return fmt.Sprintf("featureform_resource_%s_%s_%s", idType, id.Name, id.Variant)
}

func (store *snowflakeOfflineStore) getMaterializationTableName(ftID MaterializationID) string {
	return fmt.Sprintf("featureform_materialization_%s", ftID)
}

func (store *snowflakeOfflineStore) getTrainingSetName(id ResourceID) string {
	return fmt.Sprintf("featureform_trainingset_%s_%s", id.Name, id.Variant)
}

func (store *snowflakeOfflineStore) tableExists(id ResourceID) (bool, error) {
	var n int64
	var tableName string
	if id.check(Feature, Label) == nil {
		tableName = store.getResourceTableName(id)
	} else if id.check(TrainingSet) == nil {
		tableName = store.getTrainingSetName(id)
	}
	err := store.db.QueryRow("SELECT 1 FROM information_schema.tables WHERE table_name=$1", tableName).Scan(&n)
	if err == db.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (store *snowflakeOfflineStore) AsOfflineStore() (OfflineStore, error) {
	return store, nil
}

// CreateResourceTable creates a new Resource table.
// Returns a table if it does not already exist and stores the table ID in the resource index table.
// Returns an error if the table already exists or if table is the wrong type.
func (store *snowflakeOfflineStore) CreateResourceTable(id ResourceID) (OfflineTable, error) {
	if err := id.check(Feature, Label); err != nil {
		return nil, err
	}

	if exists, err := store.tableExists(id); err != nil {
		return nil, err
	} else if exists {
		return nil, &TableAlreadyExists{id.Name, id.Variant}
	}
	tableName := store.getResourceTableName(id)
	table, err := NewSnowflakeOfflineTable(store.db, tableName)
	if err != nil {
		return nil, err
	}
	return table, nil
}

type snowflakeOfflineTable struct {
	db   *sql.DB
	name string
}

func NewSnowflakeOfflineTable(db *sql.DB, name string) (*snowflakeOfflineTable, error) {
	tableCreateQry := fmt.Sprintf("CREATE TABLE %s (entity VARCHAR, value JSONB, ts timestamptz, UNIQUE (entity, ts))", sanitize(name))
	_, err := db.Exec(tableCreateQry)
	if err != nil {
		return nil, err
	}
	return &snowflakeOfflineTable{
		db:   db,
		name: name,
	}, nil
}
