package provider_type

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
	MemoryOffline    Type = "MEMORY_OFFLINE"
	PostgresOffline  Type = "POSTGRES_OFFLINE"
	SnowflakeOffline Type = "SNOWFLAKE_OFFLINE"
	RedshiftOffline  Type = "REDSHIFT_OFFLINE"
	SparkOffline     Type = "SPARK_OFFLINE"
	BigQueryOffline  Type = "BIGQUERY_OFFLINE"
	K8sOffline       Type = "K8S_OFFLINE"
	S3               Type = "S3"
	GCS              Type = "GCS"
	HDFS             Type = "HDFS"
	AZURE            Type = "AZURE"
	UNIT_TEST        Type = "UNIT_TEST"
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
	PineconeOnline,
	PostgresOffline,
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
