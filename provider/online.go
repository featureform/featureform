// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

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
	cluster  *gocql.ClusterConfig
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
	cassandraConfig := &CassandraConfig{} //-> An empty struct
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

	//Create cluster and session
	cassandraCluster := gocql.NewCluster(options.Addr)
	newSession, err := cassandraCluster.CreateSession()
	if err != nil {
		return nil, err
	}

	//Create keyspace
	query := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class' : 'SimpleStrategy','replication_factor' : 1}", options.keyspace)
	err = newSession.Query(query).WithContext(ctx).Exec()
	if err != nil {
		return nil, err
	}

	// Create Metadata Table
	query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (tableName text PRIMARY KEY, tableType text)", fmt.Sprintf("%s.tableMetadata", options.keyspace))
	err = newSession.Query(query).WithContext(ctx).Exec()
	if err != nil {
		return nil, err
	}

	//Assign keyspace to cluster and use it
	cassandraCluster.Keyspace = options.keyspace

	// Store the above entities into an Online Store struct
	return &cassandraOnlineStore{cassandraCluster, newSession, options.keyspace, BaseProvider{
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

// CASSANDRA CREATE TABLE
func (store *cassandraOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {

	//Create table Name
	tableName := fmt.Sprintf("%s.table%s", store.keyspace, sn.Custom(feature, "[^a-zA-Z0-9_]"))

	//Create table key
	key := cassandraTableKey{store.keyspace, feature, variant}
	keyName := sn.Custom(key.String(), "[^a-zA-Z0-9_]")

	//Check if table exists
	getTable, _ := store.GetTable(feature, variant)
	if getTable != nil {
		return nil, &TableAlreadyExists{feature, variant}
	}

	// Create Table
	query := fmt.Sprintf("CREATE TABLE %s (entity text PRIMARY KEY, value text)", tableName)
	err := store.session.Query(query).WithContext(ctx).Exec()
	if err != nil {
		return nil, err
	}

	//return the table
	table := &cassandraOnlineTable{
		cluster:   store.cluster,
		session:   store.session,
		key:       key,
		valueType: valueType,
	}

	//Update Metadata
	metadataTableName := fmt.Sprintf("%s.tableMetadata", store.keyspace)
	query = fmt.Sprintf("INSERT INTO %s (tableName, tableType) VALUES (?, ?)", metadataTableName)
	err = table.session.Query(query, keyName, string(valueType)).WithContext(ctx).Exec()
	if err != nil {
		return nil, err
	}

	return table, nil

}

// CASSANDRA GET TABLE

func (store *cassandraOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {

	store.session.SetConsistency(gocql.One)

	//Get table name, key and check if it exists
	tableName := fmt.Sprintf("%s.table%s", store.keyspace, sn.Custom(feature, "[^a-zA-Z0-9_]"))
	key := cassandraTableKey{store.keyspace, feature, variant}
	keyName := sn.Custom(key.String(), "[^a-zA-Z0-9_]")
	scanner := store.session.Query(fmt.Sprintf("SELECT * FROM %s", tableName)).WithContext(ctx).Iter().Scanner()

	if scanner.Err() != nil {
		return nil, &TableNotFound{feature, variant}
	}

	//Select the vType
	var vType string
	metadataTableName := fmt.Sprintf("%s.tableMetadata", store.keyspace)
	query := fmt.Sprintf("SELECT tableType FROM %s WHERE tableName = '%s'", metadataTableName, keyName)
	err := store.session.Query(query).WithContext(ctx).Scan(&vType)
	if err != nil {
		return nil, err
	}

	table := &cassandraOnlineTable{
		cluster:   store.cluster,
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
	cluster   *gocql.ClusterConfig
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

// CASSANDRA SET ENTITY
func (table cassandraOnlineTable) Set(entity string, value interface{}) error {

	table.session.SetConsistency(gocql.One)

	//Get table key
	key := table.key

	//Get table Name
	tableName := fmt.Sprintf("%s.table%s", key.Keyspace, sn.Custom(key.Feature, "[^a-zA-Z0-9_]"))

	//Set entity
	val := fmt.Sprintf("%v", value)
	query := fmt.Sprintf("INSERT INTO %s (entity, value) VALUES (?, ?)", tableName)
	err := table.session.Query(query, entity, val).WithContext(ctx).Exec()
	if err != nil {
		return err
	}

	return nil
}

// CASSANDRA GET ENTITY
func (table cassandraOnlineTable) Get(entity string) (interface{}, error) {

	//Get table key
	key := table.key

	//Get table Name
	tableName := fmt.Sprintf("%s.table%s", key.Keyspace, sn.Custom(key.Feature, "[^a-zA-Z0-9_]"))

	//Get value
	var val string
	query := fmt.Sprintf("SELECT value FROM %s WHERE entity = '%s'", tableName, entity)
	err := table.session.Query(query).WithContext(ctx).Scan(&val)
	if err != nil {
		return nil, &EntityNotFound{entity}
	}

	switch table.valueType {
	case Int:
		return strconv.Atoi(val)
	case Int64:
		return strconv.ParseInt(val, 10, 64)
	// Set float 32 as strconv only outputs float 64
	case Float32:
		var result32 float32
		result64, err := strconv.ParseFloat(val, 32)
		if err != nil {
			return nil, err
		}
		result32 = float32(result64)
		return result32, err
	case Float64:
		return strconv.ParseFloat(val, 64)
	case Bool:
		return strconv.ParseBool(val)
	}

	return val, nil

}
