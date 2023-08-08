package provider_config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
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

func getConnectionConfigs() ([]byte, error) {
	connectionConfigs, err := os.ReadFile("../connection/connection_configs.json")
	if err != nil {
		return nil, err
	}
	return connectionConfigs, nil
}

func TestRedis(t *testing.T) {
	connectionConfigs, err := getConnectionConfigs()
	if err != nil {
		println(err)
		t.FailNow()
	}

	var jsonDict map[string]interface{}
	if err = json.Unmarshal(connectionConfigs, &jsonDict); err != nil {
		println(err)
		t.FailNow()
	}

	config := jsonDict["RedisConfig"].(map[string]interface{})
	addr := config["Addr"].(string)
	password := config["Password"].(string)
	db := int(config["DB"].(float64))

	instance := RedisConfig{
		Addr:     addr,
		Password: password,
		DB:       db,
	}

	assert.NotNil(t, instance)
}

func TestGCS(t *testing.T) {
	connectionConfigs, err := getConnectionConfigs()
	if err != nil {
		println(err)
		t.FailNow()
	}

	var jsonDict map[string]interface{}
	if err = json.Unmarshal(connectionConfigs, &jsonDict); err != nil {
		println(err)
		t.FailNow()
	}

	config := jsonDict["PineconeConfig"].(map[string]interface{})
	projectId := strconv.FormatFloat(config["ProjectID"].(float64), 'f', -1, 64)
	env := config["Environment"].(string)
	apiKey := config["ApiKey"].(string)

	instance := PineconeConfig{
		ProjectID:   projectId,
		Environment: env,
		ApiKey:      apiKey,
	}

	assert.NotNil(t, instance)
}
