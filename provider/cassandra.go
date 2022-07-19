// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"encoding/json"
	"fmt"

	"github.com/gocql/gocql"
	sn "github.com/mrz1836/go-sanitize"
)

type cassandraTableKey struct {
	Keyspace, Feature, Variant string
}

func (t cassandraTableKey) String() string {
	marshalled, err := json.Marshal(t)
	if err != nil {
		return err.Error()
	}
	return string(marshalled)
}

type cassandraOnlineStore struct {
	session  *gocql.Session
	keyspace string
	BaseProvider
}

type cassandraOnlineTable struct {
	session   *gocql.Session
	key       cassandraTableKey
	valueType ValueType
}

func cassandraOnlineStoreFactory(serialized SerializedConfig) (Provider, error) {
	cassandraConfig := &CassandraConfig{}
	if err := cassandraConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	if cassandraConfig.Keyspace == "" {
		cassandraConfig.Keyspace = "Featureform_table__"
	}

	return NewCassandraOnlineStore(cassandraConfig)
}

func NewCassandraOnlineStore(options *CassandraConfig) (*cassandraOnlineStore, error) {
	cassandraCluster := gocql.NewCluster(options.Addr)
	cassandraCluster.Authenticator = gocql.PasswordAuthenticator{
		Username: options.Username,
		Password: options.Password,
	}
	err := cassandraCluster.Consistency.UnmarshalText([]byte(options.Consistency))
	if err != nil {
		return nil, err
	}
	newSession, err := cassandraCluster.CreateSession()
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class' : 'SimpleStrategy','replication_factor' : %d }", options.Keyspace, options.Replication)
	err = newSession.Query(query).WithContext(ctx).Exec()
	cassandraCluster.Keyspace = options.Keyspace
	if err != nil {
		return nil, err
	}

	query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (tableName text PRIMARY KEY, tableType text)", fmt.Sprintf("%s.featureform__metadata", options.Keyspace))
	err = newSession.Query(query).WithContext(ctx).Exec()
	if err != nil {
		return nil, err
	}

	return &cassandraOnlineStore{newSession, options.Keyspace, BaseProvider{
		ProviderType:   CassandraOnline,
		ProviderConfig: options.Serialized(),
	},
	}, nil
}

func (store *cassandraOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func GetTableName(keyspace, feature, variant string) string {
	tableName := fmt.Sprintf("%s.featureform__%s__%s", sn.Custom(keyspace, "[^a-zA-Z0-9_]"), sn.Custom(feature, "[^a-zA-Z0-9_]"), sn.Custom(variant, "[^a-zA-Z0-9_]"))
	return tableName
}

func GetMetadataTableName(keyspace string) string {
	metadataTableName := fmt.Sprintf("%s.featureform__metadata", keyspace)
	return metadataTableName
}

func (store *cassandraOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	tableName := GetTableName(store.keyspace, feature, variant)
	vType := cassandraTypeMap[string(valueType)]
	key := cassandraTableKey{store.keyspace, feature, variant}
	getTable, _ := store.GetTable(feature, variant)
	if getTable != nil {
		return nil, &TableAlreadyExists{feature, variant}
	}

	metadataTableName := GetMetadataTableName(store.keyspace)
	query := fmt.Sprintf("INSERT INTO %s (tableName, tableType) VALUES (?, ?)", metadataTableName)
	err := store.session.Query(query, tableName, string(valueType)).WithContext(ctx).Exec()
	if err != nil {
		return nil, err
	}

	query = fmt.Sprintf("CREATE TABLE %s (entity text PRIMARY KEY, value %s)", tableName, vType)
	err = store.session.Query(query).WithContext(ctx).Exec()
	if err != nil {
		return nil, err
	}

	table := &cassandraOnlineTable{
		session:   store.session,
		key:       key,
		valueType: valueType,
	}

	return table, nil

}

func (store *cassandraOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	tableName := GetTableName(store.keyspace, feature, variant)
	key := cassandraTableKey{store.keyspace, feature, variant}

	var vType string
	metadataTableName := GetMetadataTableName(store.keyspace)
	query := fmt.Sprintf("SELECT tableType FROM %s WHERE tableName = '%s'", metadataTableName, tableName)
	err := store.session.Query(query).WithContext(ctx).Scan(&vType)
	if err == gocql.ErrNotFound {
		return nil, &TableNotFound{feature, variant}
	}
	if err != nil {
		return nil, err
	}

	table := &cassandraOnlineTable{
		session:   store.session,
		key:       key,
		valueType: ValueType(vType),
	}

	return table, nil
}

func (store *cassandraOnlineStore) DeleteTable(feature, variant string) error {
	tableName := GetTableName(store.keyspace, feature, variant)
	metadataTableName := GetMetadataTableName(store.keyspace)
	query := fmt.Sprintf("DELETE FROM %s WHERE tableName = '%s' IF EXISTS", metadataTableName, tableName)
	err := store.session.Query(query).WithContext(ctx).Exec()
	if err != nil {
		return err
	}
	query = fmt.Sprintf("DROP TABLE [IF EXISTS] %s", tableName)
	err = store.session.Query(query).WithContext(ctx).Exec()
	if err != nil {
		return err
	}

	return nil
}

func (table cassandraOnlineTable) Set(entity string, value interface{}) error {
	key := table.key
	tableName := GetTableName(key.Keyspace, key.Feature, key.Variant)

	query := fmt.Sprintf("INSERT INTO %s (entity, value) VALUES (?, ?)", tableName)
	err := table.session.Query(query, entity, value).WithContext(ctx).Exec()
	if err != nil {
		return err
	}

	return nil
}

func (table cassandraOnlineTable) Get(entity string) (interface{}, error) {

	key := table.key
	tableName := GetTableName(key.Keyspace, key.Feature, key.Variant)

	var ptr interface{}
	switch table.valueType {
	case Int:
		ptr = new(int)
	case Int64:
		ptr = new(int64)
	case Float32:
		ptr = new(float32)
	case Float64:
		ptr = new(float64)
	case Bool:
		ptr = new(bool)
	case String, NilType:
		ptr = new(string)
	default:
		return nil, fmt.Errorf("data type not recognized")
	}

	query := fmt.Sprintf("SELECT value FROM %s WHERE entity = '%s'", tableName, entity)
	err := table.session.Query(query).WithContext(ctx).Scan(ptr)
	if err == gocql.ErrNotFound {
		return nil, &EntityNotFound{entity}
	}
	if err != nil {
		return nil, err
	}

	var val interface{}
	switch casted := ptr.(type) {
	case *int:
		val = *casted
	case *int64:
		val = *casted
	case *float32:
		val = *casted
	case *float64:
		val = *casted
	case *bool:
		val = *casted
	case *string:
		val = *casted
	default:
		return nil, fmt.Errorf("data type not recognized")
	}
	return val, nil

}
