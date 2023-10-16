// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"fmt"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
)

func init() {
	unregisteredFactories := map[pt.Type]Factory{
		pt.LocalOnline:      localOnlineStoreFactory,
		pt.RedisOnline:      redisOnlineStoreFactory,
		pt.CassandraOnline:  cassandraOnlineStoreFactory,
		pt.FirestoreOnline:  firestoreOnlineStoreFactory,
		pt.DynamoDBOnline:   dynamodbOnlineStoreFactory,
		pt.PineconeOnline:   pineconeOnlineStoreFactory,
		pt.MemoryOffline:    memoryOfflineStoreFactory,
		pt.MySqlOffline:     memoryOfflineStoreFactory,
		pt.PostgresOffline:  postgresOfflineStoreFactory,
		pt.SnowflakeOffline: snowflakeOfflineStoreFactory,
		pt.RedshiftOffline:  redshiftOfflineStoreFactory,
		pt.BigQueryOffline:  bigQueryOfflineStoreFactory,
		pt.SparkOffline:     sparkOfflineStoreFactory,
		pt.K8sOffline:       k8sOfflineStoreFactory,
		pt.BlobOnline:       blobOnlineStoreFactory,
		pt.MongoDBOnline:    mongoOnlineStoreFactory,
		pt.UNIT_TEST:        unitTestStoreFactory,
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
	Type() pt.Type
	Config() pc.SerializedConfig
	Check() (bool, error)
}

type BaseProvider struct {
	ProviderType   pt.Type
	ProviderConfig pc.SerializedConfig
}

func (provider BaseProvider) AsOnlineStore() (OnlineStore, error) {
	return nil, fmt.Errorf("%T cannot be used as an OnlineStore", provider)
}

func (provider BaseProvider) AsOfflineStore() (OfflineStore, error) {
	return nil, fmt.Errorf("%T cannot be used as an OfflineStore", provider)
}

func (provider BaseProvider) Type() pt.Type {
	return provider.ProviderType
}

func (provider BaseProvider) Config() pc.SerializedConfig {
	return provider.ProviderConfig
}

func (provider BaseProvider) Check() (bool, error) {
	return false, fmt.Errorf("provider health check not implemented")
}

type Factory func(pc.SerializedConfig) (Provider, error)

var factories map[pt.Type]Factory = make(map[pt.Type]Factory)

func RegisterFactory(t pt.Type, f Factory) error {
	if _, has := factories[t]; has {
		return fmt.Errorf("%s provider factory already exists", t)
	}
	factories[t] = f
	return nil
}

func Get(t pt.Type, config pc.SerializedConfig) (Provider, error) {
	f, has := factories[t]
	if !has {
		return nil, fmt.Errorf("no provider of type: %s", t)
	}
	return f(config)
}
