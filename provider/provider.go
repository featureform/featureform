// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"fmt"

	pc "github.com/featureform/provider/provider_config"
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
		K8sOffline:       k8sOfflineStoreFactory,
		BlobOnline:       blobOnlineStoreFactory,
		MongoDBOnline:    mongoOnlineStoreFactory,
	}
	for name, factory := range unregisteredFactories {
		if err := RegisterFactory(name, factory); err != nil {
			panic(err)
		}
	}
}

type SerializedTableSchema []byte

type Provider interface {
	AsOnlineStore() (OnlineStore, error)
	AsOfflineStore() (OfflineStore, error)
	Type() Type
	Config() pc.SerializedConfig
}

type BaseProvider struct {
	ProviderType   Type
	ProviderConfig pc.SerializedConfig
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

func (provider BaseProvider) Config() pc.SerializedConfig {
	return provider.ProviderConfig
}

type Factory func(pc.SerializedConfig) (Provider, error)

type Type string

var factories map[Type]Factory = make(map[Type]Factory)

func RegisterFactory(t Type, f Factory) error {
	if _, has := factories[t]; has {
		return fmt.Errorf("%s provider factory already exists", t)
	}
	factories[t] = f
	return nil
}

func Get(t Type, config pc.SerializedConfig) (Provider, error) {
	f, has := factories[t]
	if !has {
		return nil, fmt.Errorf("no provider of type: %s", t)
	}
	return f(config)
}
