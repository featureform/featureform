package provider_type

type Type string

const (
	// Online
	LocalOnline     Type = "LOCAL_ONLINE"
	RedisOnline     Type = "REDIS_ONLINE"
	CassandraOnline Type = "CASSANDRA_ONLINE"
	FirestoreOnline Type = "FIRESTORE_ONLINE"
	DynamoDBOnline  Type = "DYNAMODB_ONLINE"
	BlobOnline      Type = "BLOB_ONLINE"
	MongoDBOnline   Type = "MONGODB_ONLINE"
	// Offline
	MemoryOffline    Type = "MEMORY_OFFLINE"
	PostgresOffline  Type = "POSTGRES_OFFLINE"
	SnowflakeOffline Type = "SNOWFLAKE_OFFLINE"
	RedshiftOffline  Type = "REDSHIFT_OFFLINE"
	SparkOffline     Type = "SPARK_OFFLINE"
	BigQueryOffline  Type = "BIGQUERY_OFFLINE"
	K8sOffline       Type = "K8S_OFFLINE"
)
