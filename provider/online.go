// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

const (
	LocalOnline     Type = "LOCAL_ONLINE"
	RedisOnline          = "REDIS_ONLINE"
	CassandraOnline      = "CASSANDRA_ONLINE"
	FirestoreOnline      = "FIRESTORE_ONLINE"
	DynamoDBOnline       = "DYNAMODB_ONLINE"
	BlobOnline           = "BLOB_ONLINE"
)

var ctx = context.Background()

type OnlineStore interface {
	GetTable(feature, variant string) (OnlineStoreTable, error)
	CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error)
	DeleteTable(feature, variant string) error
	Close() error
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

//type localOnlineTable struct {
//	values localOnlineTable
//	vType  ValueType
//}

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
	if err := valueType.isValid(); err != nil {
		return nil, err
	}
	key := tableKey{feature, variant}
	if _, has := store.tables[key]; has {
		return nil, &TableAlreadyExists{feature, variant}
	}
	table := localOnlineTable{
		make(localOnlineTableValues),
		valueType,
	}
	store.tables[key] = table
	return table, nil
}

func (store *localOnlineStore) DeleteTable(feaute, variant string) error {
	return nil
}

func (store *localOnlineStore) Close() error {
	return nil
}

type localOnlineTableValues map[string]interface{}

type localOnlineTable struct {
	values map[string]interface{}
	vType  ValueType
}

type redisOnlineTable struct {
	client    *redis.Client
	key       redisTableKey
	valueType ValueType
}

func (table localOnlineTable) Set(entity string, value interface{}) error {
	if !table.vType.doesMatch(value) {
		return fmt.Errorf("value does not match table type: given %T, table type: %s", value, table.vType)
	}
	table.values[entity] = value
	return nil
}

func (table localOnlineTable) Get(entity string) (interface{}, error) {
	val, has := table.values[entity]
	if !has {
		return nil, &EntityNotFound{entity}
	}
	return table.castValue(val, table.vType)
}

func (table localOnlineTable) castValue(value interface{}, vType ValueType) (interface{}, error) {
	switch vType {
	case NilType, String:
		str := fmt.Sprintf("%v", value)
		return str, nil
	case Int:
		if val, ok := value.(int); !ok {
			return nil, fmt.Errorf("could not cast value %v type: %T to %s", value, value, Int)
		} else {
			return val, nil
		}
	case Int32:
		if val, ok := value.(int32); !ok {
			return nil, fmt.Errorf("could not cast value %v type: %T to %s", value, value, Int32)
		} else {
			return val, nil
		}
	case Int64:
		if val, ok := value.(int64); !ok {
			return nil, fmt.Errorf("could not cast value %v type: %T to %s", value, value, Int64)
		} else {
			return int64(val), nil
		}
	case Float32:
		if val, ok := value.(float32); !ok {
			return nil, fmt.Errorf("could not cast value %v type: %T to %s", value, value, Float32)
		} else {
			return val, nil
		}
	case Float64:
		if val, ok := value.(float64); !ok {
			return nil, fmt.Errorf("could not cast value %v type: %T to %s", value, value, Float64)
		} else {
			return val, nil
		}
	case Bool:
		if val, ok := value.(bool); !ok {
			return nil, fmt.Errorf("could not cast value %v type: %T to %s", value, value, Bool)
		} else {
			return val, nil
		}
	case Timestamp, Datetime:
		if val, ok := value.(time.Time); !ok {
			return nil, fmt.Errorf("could not cast value %v type: %T to %s", value, value, Datetime)
		} else {
			return time.Time(val), nil
		}
	default:
		val := fmt.Sprintf("%v", value)
		return val, nil
	}
}
