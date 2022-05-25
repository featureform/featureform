// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
)

const (
	LocalOnline Type = "LOCAL_ONLINE"
	RedisOnline      = "REDIS_ONLINE"
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

func (t redisTableKey) String() string {
	marshalled, _ := json.Marshal(t)
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

func (store *localOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *redisOnlineStore) AsOnlineStore() (OnlineStore, error) {
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

type localOnlineTable map[string]interface{}

type redisOnlineTable struct {
	client    *redis.Client
	key       redisTableKey
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
