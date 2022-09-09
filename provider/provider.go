// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"encoding/json"
	"fmt"
)

func init() {
	unregisteredFactories := map[Type]Factory{
		LocalOnline:      localOnlineStoreFactory,
		RedisOnline:      redisOnlineStoreFactory,
		CassandraOnline:  cassandraOnlineStoreFactory,
		FirestoreOnline:  firestoreOnlineStoreFactory,
		DynamoDBOnline:   dynamodbOnlineStoreFactory,
		MemoryOffline:    memoryOfflineStoreFactory,
		PostgresOffline:  postgresOfflineStoreFactory,
		SnowflakeOffline: snowflakeOfflineStoreFactory,
		RedshiftOffline:  redshiftOfflineStoreFactory,
		BigQueryOffline:  bigQueryOfflineStoreFactory,
		SparkOffline:     sparkOfflineStoreFactory,
	}
	for name, factory := range unregisteredFactories {
		if err := RegisterFactory(name, factory); err != nil {
			panic(err)
		}
	}
}

type SerializedConfig []byte

type SerializedTableSchema []byte

type RedisConfig struct {
	Prefix   string
	Addr     string
	Password string
	DB       int
}

func (r RedisConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return config
}

func (r *RedisConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, r)
	if err != nil {
		return err
	}
	return nil
}

type CassandraConfig struct {
	Keyspace    string
	Addr        string
	Username    string
	Password    string
	Consistency string
	Replication int
}

func (r CassandraConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return config
}

func (r *CassandraConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, r)
	if err != nil {
		return err
	}
	return nil
}

type DynamodbConfig struct {
	Prefix    string
	Region    string
	AccessKey string
	SecretKey string
}

func (r DynamodbConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return config
}

func (r *DynamodbConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, r)
	if err != nil {
		return err
	}
	return nil
}

type FirestoreConfig struct {
	Collection  string
	ProjectID   string
	Credentials map[string]interface{}
}

func (r FirestoreConfig) Serialize() SerializedConfig {
	config, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return config
}

func (r *FirestoreConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, r)
	if err != nil {
		return err
	}
	return nil
}

type Provider interface {
	AsOnlineStore() (OnlineStore, error)
	AsOfflineStore() (OfflineStore, error)
	Type() Type
	Config() SerializedConfig
}

type BaseProvider struct {
	ProviderType   Type
	ProviderConfig SerializedConfig
}

func (provider BaseProvider) AsOnlineStore() (OnlineStore, error) {
	return nil, fmt.Errorf("%T cannot be used as an OnlineStore", provider)
}

func (provider BaseProvider) AsOfflineStore() (OfflineStore, error) {
	return nil, fmt.Errorf("%T cannot be used as an OfflineStore", provider)
}

func (provider BaseProvider) Type() Type {
	return provider.ProviderType
}

func (provider BaseProvider) Config() SerializedConfig {
	return provider.ProviderConfig
}

type Factory func(SerializedConfig) (Provider, error)

type Type string

var factories map[Type]Factory = make(map[Type]Factory)

func RegisterFactory(t Type, f Factory) error {
	if _, has := factories[t]; has {
		return fmt.Errorf("%s provider factory already exists", t)
	}
	factories[t] = f
	return nil
}

func Get(t Type, config SerializedConfig) (Provider, error) {
	f, has := factories[t]
	if !has {
		return nil, fmt.Errorf("no provider of type: %s", t)
	}
	return f(config)
}
