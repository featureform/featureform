package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/go-redis/redis/v8"
)

type redisTableKey struct {
	Prefix, Feature, Variant string
}

func (t redisTableKey) String() string {
	marshalled, _ := json.Marshal(t)
	return string(marshalled)
}

type redisOnlineStore struct {
	client *redis.Client
	prefix string
	BaseProvider
}

func redisOnlineStoreFactory(serialized pc.SerializedConfig) (Provider, error) {
	redisConfig := &pc.RedisConfig{}
	if err := redisConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	if redisConfig.Prefix == "" {
		redisConfig.Prefix = "Featureform_table__"
	}
	return NewRedisOnlineStore(redisConfig), nil
}

func NewRedisOnlineStore(options *pc.RedisConfig) *redisOnlineStore {
	redisOptions := &redis.Options{
		Addr:     options.Addr,
		Password: options.Password,
		DB:       options.DB,
		PoolSize: 100000,
	}
	redisClient := redis.NewClient(redisOptions)
	return &redisOnlineStore{redisClient, options.Prefix, BaseProvider{
		ProviderType:   pt.RedisOnline,
		ProviderConfig: options.Serialized(),
	},
	}
}

func (store *redisOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *redisOnlineStore) Close() error {
	// from the docs:
	//
	// "It is rare to Close a Client, as the Client is meant to be
	// long-lived and shared between many goroutines.""
	//
	return store.client.Close()
}

func (store *redisOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	key := redisTableKey{store.prefix, feature, variant}
	vType, err := store.client.HGet(context.TODO(), fmt.Sprintf("%s__tables", store.prefix), key.String()).Result()
	if err != nil {
		return nil, err
	}
	table := &redisOnlineTable{client: store.client, key: key, valueType: ValueType(vType)}
	return table, nil
}

func (store *redisOnlineStore) CreateTable(feature, variant string, valueType ValueType) (OnlineStoreTable, error) {
	key := redisTableKey{store.prefix, feature, variant}
	exists, err := store.client.HExists(context.TODO(), fmt.Sprintf("%s__tables", store.prefix), key.String()).Result()
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, &TableAlreadyExists{feature, variant}
	}
	if err := store.client.HSet(context.TODO(), fmt.Sprintf("%s__tables", store.prefix), key.String(), string(valueType)).Err(); err != nil {
		return nil, err
	}
	table := &redisOnlineTable{client: store.client, key: key, valueType: valueType}
	return table, nil
}

func (store *redisOnlineStore) DeleteTable(feature, variant string) error {
	return nil
}

func (table redisOnlineTable) Set(entity string, value interface{}) error {
	val := table.client.HSet(context.TODO(), table.key.String(), entity, value)
	if val.Err() != nil {
		return val.Err()
	}
	return nil
}

func (table redisOnlineTable) Get(entity string) (interface{}, error) {
	clients, err := table.client.Info(context.Background(), "clients").Result()
	if err != nil {
		return nil, err
	}
	fmt.Println(clients)
	val := table.client.HGet(context.TODO(), table.key.String(), entity)
	if val.Err() != nil {
		return nil, fmt.Errorf("could not get value for entity: %s: %w", entity, val.Err())
	}
	var result interface{}
	switch table.valueType {
	case NilType, String:
		result, err = val.Result()
	case Int:
		result, err = val.Int()
	case Int32:
		if res, err := val.Int(); err == nil {
			result = int32(res)
		}
	case Int64:
		result, err = val.Int64()
	case Float32:
		result, err = val.Float32()
	case Float64:
		result, err = val.Float64()
	case Bool:
		result, err = val.Bool()
	case Timestamp, Datetime: // Maintains compatibility with previously create timestamp tables
		var t time.Time
		t, err = val.Time()
		if err == nil {
			result = t.Local()
		}
	default:
		result, err = val.Result()
	}
	if err != nil {
		return nil, fmt.Errorf("could not cast value: %s to %s: %w", val.String(), table.valueType, err)
	}
	return result, nil
}
