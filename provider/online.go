// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

const (
	LocalOnline     Type = "LOCAL_ONLINE"
	RedisOnline          = "REDIS_ONLINE"
	CassandraOnline      = "CASSANDRA_ONLINE"
	FirestoreOnline      = "FIRESTORE_ONLINE"
	DynamoDBOnline       = "DYNAMODB_ONLINE"
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
	DeleteTable(feature, variant string) error
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

type CustomError struct {
	ErrorMessage string
}

func (err *CustomError) Error() string {
	return err.ErrorMessage
}

func localOnlineStoreFactory(SerializedConfig) (Provider, error) {
	return NewLocalOnlineStore(), nil
}

type localOnlineStore struct {
	tables map[tableKey]localOnlineTable
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

func (store *localOnlineStore) AsOnlineStore() (OnlineStore, error) {
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

func (store *localOnlineStore) DeleteTable(feaute, variant string) error {
	return nil
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
