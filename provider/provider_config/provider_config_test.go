package provider_config

import (
	"fmt"
	"testing"

	pt "github.com/featureform/provider/provider_type"
	"github.com/stretchr/testify/assert"
)

var providerMap = map[string]string{
	"LOCAL_ONLINE":      "LocalConfig",
	"REDIS_ONLINE":      "RedisConfig",
	"CASSANDRA_ONLINE":  "CassandraConfig",
	"FIRESTORE_ONLINE":  "FirestoreConfig",
	"DYNAMODB_ONLINE":   "DynamodbConfig",
	"BLOB_ONLINE":       "OnlineBlobConfig",
	"MONGODB_ONLINE":    "MongoDbConfig",
	"PINECONE_ONLINE":   "PineconeConfig",
	"POSTGRES_OFFLINE":  "PostgresConfig",
	"SNOWFLAKE_OFFLINE": "SnowflakeConfig",
	"REDSHIFT_OFFLINE":  "RedshiftConfig",
	"SPARK_OFFLINE":     "SparkConfig",
	"BIGQUERY_OFFLINE":  "BigQueryConfig",
	"K8S_OFFLINE":       "K8sConfig",
	"S3":                "S3StoreConfig",
	"GCS":               "GCSFileStoreConfig",
	"HDFS":              "HDFSConfig",
	"AZURE":             "AzureFileStoreConfig",
	"MEMORY_OFFLINE":    "MemoryConfig",
}

func TestAllProviderTypesHasMapEntry(t *testing.T) {
	proList := pt.AllProviderTypes

	for _, currentProvider := range proList {
		foundEntry := providerMap[string(currentProvider)]
		errorMsg := fmt.Sprintf("This provider type (%s) is not present in the providerMap.", currentProvider)
		assert.NotEmpty(t, foundEntry, errorMsg)
	}
}
