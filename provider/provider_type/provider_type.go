// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider_type

import (
	"sync"

	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/logging"
)

type Type string

func (t Type) String() string {
	return string(t)
}

const (
	// Online
	LocalOnline     Type = "LOCAL_ONLINE"
	RedisOnline     Type = "REDIS_ONLINE"
	CassandraOnline Type = "CASSANDRA_ONLINE"
	FirestoreOnline Type = "FIRESTORE_ONLINE"
	DynamoDBOnline  Type = "DYNAMODB_ONLINE"
	BlobOnline      Type = "BLOB_ONLINE"
	MongoDBOnline   Type = "MONGODB_ONLINE"
	PineconeOnline  Type = "PINECONE_ONLINE"

	// Offline
	MemoryOffline     Type = "MEMORY_OFFLINE"
	MySqlOffline      Type = "MYSQL_OFFLINE"
	PostgresOffline   Type = "POSTGRES_OFFLINE"
	ClickHouseOffline Type = "CLICKHOUSE_OFFLINE"
	SnowflakeOffline  Type = "SNOWFLAKE_OFFLINE"
	RedshiftOffline   Type = "REDSHIFT_OFFLINE"
	SparkOffline      Type = "SPARK_OFFLINE"
	BigQueryOffline   Type = "BIGQUERY_OFFLINE"
	K8sOffline        Type = "K8S_OFFLINE"
	S3                Type = "S3"
	GCS               Type = "GCS"
	HDFS              Type = "HDFS"
	AZURE             Type = "AZURE"
	UNIT_TEST         Type = "UNIT_TEST"

	NONE Type = "NONE"
)

var AllProviderTypes = []Type{
	LocalOnline,
	RedisOnline,
	CassandraOnline,
	FirestoreOnline,
	DynamoDBOnline,
	BlobOnline,
	MongoDBOnline,
	MemoryOffline,
	MySqlOffline,
	PineconeOnline,
	PostgresOffline,
	ClickHouseOffline,
	SnowflakeOffline,
	RedshiftOffline,
	SparkOffline,
	BigQueryOffline,
	K8sOffline,
	S3,
	GCS,
	HDFS,
	AZURE,
	UNIT_TEST,
}

func GetOnlineTypes() []Type {
	return []Type{LocalOnline, RedisOnline, CassandraOnline, FirestoreOnline, DynamoDBOnline, BlobOnline, MongoDBOnline, PineconeOnline}
}

func GetOfflineTypes() []Type {
	return []Type{MemoryOffline, MySqlOffline, PostgresOffline, ClickHouseOffline, SnowflakeOffline, RedshiftOffline, SparkOffline, BigQueryOffline, K8sOffline}
}

func GetFileTypes() []Type {
	return []Type{S3, GCS, HDFS, AZURE}
}

var (
	converterRegistry = make(map[Type]types.ValueConverter[any])
	registryLock      = sync.RWMutex{}
)

// RegisterConverter registers a converter for a provider type
func RegisterConverter(t Type, converter types.ValueConverter[any]) {
	logger := logging.GlobalLogger
	logger.Infof("Registering converter for provider type %s", t.String())
	registryLock.Lock()
	defer registryLock.Unlock()
	converterRegistry[t] = converter
}

// GetConverter retrieves a converter for a provider type
func GetConverter(t Type) (types.ValueConverter[any], error) {
	registryLock.RLock()
	defer registryLock.RUnlock()

	converter, exists := converterRegistry[t]
	if !exists {
		return nil, fferr.NewInternalErrorf("No converter found for provider type %s", t.String())
	}
	return converter, nil
}
