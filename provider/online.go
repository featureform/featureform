// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/gocql/gocql"
	sn "github.com/mrz1836/go-sanitize"
)

const (
	LocalOnline     Type = "LOCAL_ONLINE"
	RedisOnline          = "REDIS_ONLINE"
	CassandraOnline      = "CASSANDRA_ONLINE"
)

var ctx = context.Background()

var cassandraTypeMap = map[string]string{
	"string":  "text",
	"int":     "int",
	"int64":   "bigint",
	"float32": "float",
	"float64": "double",
	"bool":    "boolean",
}

type OnlineStore interface {
	GetTable(feature, variant string) (OnlineStoreTable, error)
	CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error)
	Provider
}

type OnlineStoreTable interface {
	Set(entity string, value interface{}) error
	Get(entity string) (interface{}, error)
}

type TableNotFound struct {
	Feature, Variant string
}

func (err *TableNotFound) Error() string {
	return fmt.Sprintf("Table %s Variant %s not found.", err.Feature, err.Variant)
}

type TableAlreadyExists struct {
	Feature, Variant string
}

func (err *TableAlreadyExists) Error() string {
	return fmt.Sprintf("Table %s Variant %s already exists.", err.Feature, err.Variant)
}

type EntityNotFound struct {
	Entity string
}

func (err *EntityNotFound) Error() string {
	return fmt.Sprintf("Entity %s not found.", err.Entity)
}

type tableKey struct {
	feature, variant string
}

type redisTableKey struct {
	Prefix, Feature, Variant string
}

type cassandraTableKey struct {
	Keyspace, Feature, Variant string
}

func (t redisTableKey) String() string {
	marshalled, _ := json.Marshal(t)
	return string(marshalled)
}

func (t cassandraTableKey) String() string {
	marshalled, err := json.Marshal(t)
	if err != nil {
		return err.Error()
	}
	return string(marshalled)
}

func localOnlineStoreFactory(SerializedConfig) (Provider, error) {
	return NewLocalOnlineStore(), nil
}

type localOnlineStore struct {
	tables map[tableKey]localOnlineTable
	BaseProvider
}

type redisOnlineStore struct {
	client *redis.Client
	prefix string
	BaseProvider
}

type cassandraOnlineStore struct {
	session  *gocql.Session
	keyspace string
	BaseProvider
}

func NewLocalOnlineStore() *localOnlineStore {
	return &localOnlineStore{
		make(map[tableKey]localOnlineTable),
		BaseProvider{
			ProviderType:   LocalOnline,
			ProviderConfig: []byte{},
		},
	}
}

func redisOnlineStoreFactory(serialized SerializedConfig) (Provider, error) {
	redisConfig := &RedisConfig{}
	if err := redisConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	if redisConfig.Prefix == "" {
		redisConfig.Prefix = "Featureform_table__"
	}
	return NewRedisOnlineStore(redisConfig), nil
}

func cassandraOnlineStoreFactory(serialized SerializedConfig) (Provider, error) {
	cassandraConfig := &CassandraConfig{}
	if err := cassandraConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	if cassandraConfig.keyspace == "" {
		cassandraConfig.keyspace = "Featureform_table__"
	}

	return NewCassandraOnlineStore(cassandraConfig)
}

func NewRedisOnlineStore(options *RedisConfig) *redisOnlineStore {
	redisOptions := &redis.Options{
		Addr: options.Addr,
	}
	redisClient := redis.NewClient(redisOptions)
	return &redisOnlineStore{redisClient, options.Prefix, BaseProvider{
		ProviderType:   RedisOnline,
		ProviderConfig: options.Serialized(),
	},
	}
}

func NewCassandraOnlineStore(options *CassandraConfig) (*cassandraOnlineStore, error) {

	//Create cluster, session
	cassandraCluster := gocql.NewCluster(options.Addr)
	newSession, err := cassandraCluster.CreateSession()
	if err != nil {
		return nil, err
	}

	//Create and use keyspace
	query := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class' : 'SimpleStrategy','replication_factor' : 3}", options.keyspace)
	err = newSession.Query(query).WithContext(ctx).Exec()
	cassandraCluster.Keyspace = options.keyspace
	if err != nil {
		return nil, err
	}

	// Create Metadata Table
	query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (tableName text PRIMARY KEY, tableType text)", fmt.Sprintf("%s.tableMetadata", options.keyspace))
	err = newSession.Query(query).WithContext(ctx).Exec()
	if err != nil {
		return nil, err
	}

	return &cassandraOnlineStore{newSession, options.keyspace, BaseProvider{
		ProviderType:   CassandraOnline,
		ProviderConfig: options.Serialized(),
	},
	}, nil
}

func (store *localOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *redisOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *cassandraOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *localOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	table, has := store.tables[tableKey{feature, variant}]
	if !has {
		return nil, &TableNotFound{feature, variant}
	}
	return table, nil
}

func (store *localOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	key := tableKey{feature, variant}
	if _, has := store.tables[key]; has {
		return nil, &TableAlreadyExists{feature, variant}
	}
	table := make(localOnlineTable)
	store.tables[key] = table
	return table, nil
}

func (store *redisOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	key := redisTableKey{store.prefix, feature, variant}
	vType, err := store.client.HGet(ctx, fmt.Sprintf("%s__tables", store.prefix), key.String()).Result()

	if err != nil {
		return nil, &TableNotFound{feature, variant}
	}
	table := &redisOnlineTable{client: store.client, key: key, valueType: ValueType(vType)}
	return table, nil
}

func (store *redisOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	key := redisTableKey{store.prefix, feature, variant}
	exists, err := store.client.HExists(ctx, fmt.Sprintf("%s__tables", store.prefix), key.String()).Result()
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, &TableAlreadyExists{feature, variant}
	}
	if err := store.client.HSet(ctx, fmt.Sprintf("%s__tables", store.prefix), key.String(), string(valueType)).Err(); err != nil {
		return nil, err
	}
	table := &redisOnlineTable{client: store.client, key: key, valueType: valueType}
	return table, nil

}

func (store *cassandraOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {

	//Create table Name, key and check if it exists
	tableName := fmt.Sprintf("%s.table%s", store.keyspace, sn.Custom(feature, "[^a-zA-Z0-9_]"))
	vType := cassandraTypeMap[string(valueType)]
	key := cassandraTableKey{store.keyspace, feature, variant}
	getTable, _ := store.GetTable(feature, variant)
	if getTable != nil {
		return nil, &TableAlreadyExists{feature, variant}
	}

	//Update Metadata
	metadataTableName := fmt.Sprintf("%s.tableMetadata", store.keyspace)
	query := fmt.Sprintf("INSERT INTO %s (tableName, tableType) VALUES (?, ?)", metadataTableName)
	err := store.session.Query(query, tableName, string(valueType)).WithContext(ctx).Exec()
	if err != nil {
		return nil, err
	}

	// Create Table
	query = fmt.Sprintf("CREATE TABLE %s (entity text PRIMARY KEY, value %s)", tableName, vType)
	err = store.session.Query(query).WithContext(ctx).Exec()
	if err != nil {
		return nil, fmt.Errorf("valuetype: %s, vtype %s, %s", string(valueType), vType, err)
	}

	table := &cassandraOnlineTable{
		session:   store.session,
		key:       key,
		valueType: valueType,
	}

	return table, nil

}

func (store *cassandraOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {

	store.session.SetConsistency(gocql.One)

	tableName := fmt.Sprintf("%s.table%s", store.keyspace, sn.Custom(feature, "[^a-zA-Z0-9_]"))
	key := cassandraTableKey{store.keyspace, feature, variant}

	var vType string
	metadataTableName := fmt.Sprintf("%s.tableMetadata", store.keyspace)
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

type localOnlineTable map[string]interface{}

type redisOnlineTable struct {
	client    *redis.Client
	key       redisTableKey
	valueType ValueType
}

type cassandraOnlineTable struct {
	session   *gocql.Session
	key       cassandraTableKey
	valueType ValueType
}

func (table localOnlineTable) Set(entity string, value interface{}) error {
	table[entity] = value
	return nil
}

func (table localOnlineTable) Get(entity string) (interface{}, error) {
	val, has := table[entity]
	if !has {
		return nil, &EntityNotFound{entity}
	}
	return val, nil
}

func (table redisOnlineTable) Set(entity string, value interface{}) error {
	val := table.client.HSet(ctx, table.key.String(), entity, value)
	if val.Err() != nil {
		return val.Err()
	}
	return nil
}

func (table redisOnlineTable) Get(entity string) (interface{}, error) {
	val := table.client.HGet(ctx, table.key.String(), entity)
	if val.Err() != nil {
		return nil, &EntityNotFound{entity}
	}
	var result interface{}
	var err error
	switch table.valueType {
	case NilType, String:
		result, err = val.Result()
	case Int:
		result, err = val.Int()
	case Int64:
		result, err = val.Int64()
	case Float32:
		result, err = val.Float32()
	case Float64:
		result, err = val.Float64()
	case Bool:
		result, err = val.Bool()
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (table cassandraOnlineTable) Set(entity string, value interface{}) error {

	table.session.SetConsistency(gocql.One)
	key := table.key
	tableName := fmt.Sprintf("%s.table%s", key.Keyspace, sn.Custom(key.Feature, "[^a-zA-Z0-9_]"))
	query := fmt.Sprintf("INSERT INTO %s (entity, value) VALUES (?, ?)", tableName)
	err := table.session.Query(query, entity, value).WithContext(ctx).Exec()
	if err != nil {
		return err
	}

	return nil
}

func (table cassandraOnlineTable) Get(entity string) (interface{}, error) {

	key := table.key
	tableName := fmt.Sprintf("%s.table%s", key.Keyspace, sn.Custom(key.Feature, "[^a-zA-Z0-9_]"))

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
		return nil, fmt.Errorf("Data type not recognized")
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
		return nil, fmt.Errorf("Data type not recognized")
	}
	return val, nil

}
