package metadata

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
)

const updateSuffix = "-2"

type testCase struct {
	name         string
	valid        bool
	providerType pt.Type
}

func TestProviderConfigUpdates(t *testing.T) {
	args := []testCase{
		{
			name:         "Valid BigQuery Configuration Update",
			valid:        true,
			providerType: pt.BigQueryOffline,
		},
		{
			name:         "Invalid BigQuery Configuration Update",
			valid:        false,
			providerType: pt.BigQueryOffline,
		},
		{
			name:         "Valid Cassandra Configuration Update",
			valid:        true,
			providerType: pt.CassandraOnline,
		},
		{
			name:         "Invalid Cassandra Configuration Update",
			valid:        false,
			providerType: pt.CassandraOnline,
		},
		{
			name:         "Valid DynamoDB Configuration Update",
			valid:        true,
			providerType: pt.DynamoDBOnline,
		},
		{
			name:         "Invalid DynamoDB Configuration Update",
			valid:        false,
			providerType: pt.DynamoDBOnline,
		},
		{
			name:         "Valid Firestore Configuration Update",
			valid:        true,
			providerType: pt.FirestoreOnline,
		},
		{
			name:         "Invalid Firestore Configuration Update",
			valid:        false,
			providerType: pt.FirestoreOnline,
		},
		{
			name:         "Valid MongoDB Configuration Update",
			valid:        true,
			providerType: pt.MongoDBOnline,
		},
		{
			name:         "Invalid MongoDB Configuration Update",
			valid:        false,
			providerType: pt.MongoDBOnline,
		},
		{
			name:         "Valid PostgreSQL Configuration Update",
			valid:        true,
			providerType: pt.PostgresOffline,
		},
		{
			name:         "Invalid PostgreSQL Configuration Update",
			valid:        false,
			providerType: pt.PostgresOffline,
		},
		{
			name:         "Valid Redis Configuration Update",
			valid:        true,
			providerType: pt.RedisOnline,
		},
		{
			name:         "Invalid Redis Configuration Update",
			valid:        false,
			providerType: pt.RedisOnline,
		},
		{
			name:         "Valid Snowflake Configuration Update",
			valid:        true,
			providerType: pt.SnowflakeOffline,
		},
		{
			name:         "Invalid Snowflake Configuration Update",
			valid:        false,
			providerType: pt.SnowflakeOffline,
		},
		{
			name:         "Valid Redshift Configuration Update",
			valid:        true,
			providerType: pt.RedshiftOffline,
		},
		{
			name:         "Invalid Redshift Configuration Update",
			valid:        false,
			providerType: pt.RedshiftOffline,
		},
		{
			name:         "Valid K8s Configuration Update",
			valid:        true,
			providerType: pt.K8sOffline,
		},
		{
			name:         "Invalid K8s Configuration Update",
			valid:        false,
			providerType: pt.K8sOffline,
		},
		{
			name:         "Valid Spark Configuration Update",
			valid:        true,
			providerType: pt.SparkOffline,
		},
		{
			name:         "Invalid Spark Configuration Update",
			valid:        false,
			providerType: pt.SparkOffline,
		},
	}
	for _, c := range args {
		t.Run(c.name, func(t *testing.T) {
			switch c.providerType {
			case pt.BigQueryOffline:
				testBigQueryConfigUpdates(t, c.providerType, c.valid)
			case pt.CassandraOnline:
				testCassandraConfigUpdates(t, c.providerType, c.valid)
			case pt.DynamoDBOnline:
				testDynamoConfigUpdates(t, c.providerType, c.valid)
			case pt.FirestoreOnline:
				testFirestoreConfigUpdates(t, c.providerType, c.valid)
			case pt.MongoDBOnline:
				testMongoConfigUpdates(t, c.providerType, c.valid)
			case pt.PostgresOffline:
				testPostgresConfigUpdates(t, c.providerType, c.valid)
			case pt.RedisOnline:
				testRedisConfigUpdates(t, c.providerType, c.valid)
			case pt.SnowflakeOffline:
				testSnowflakeConfigUpdates(t, c.providerType, c.valid)
			case pt.RedshiftOffline:
				testRedshiftConfigUpdates(t, c.providerType, c.valid)
			case pt.K8sOffline:
				testK8sConfigUpdates(t, c.providerType, c.valid)
			case pt.SparkOffline:
				testSparkConfigUpdates(t, c.providerType, c.valid)
			}
		})
	}
}

func testBigQueryConfigUpdates(t *testing.T, providerType pt.Type, valid bool) {
	exCreds, err := getGCPExampleCreds()
	if err != nil {
		t.Errorf("Failed to get GCP example creds due to error: %v", err)
	}
	projectId := "featureform-gcp"
	datasetId := "transactions-ds"

	configA := pc.BigQueryConfig{
		ProjectId:   projectId,
		DatasetId:   datasetId,
		Credentials: exCreds,
	}
	a := configA.Serialize()

	if valid {
		exCreds["client_email"] = "test@featureform.com"
	} else {
		projectId += updateSuffix
		datasetId += updateSuffix
	}
	configB := pc.BigQueryConfig{
		ProjectId:   projectId,
		DatasetId:   datasetId,
		Credentials: exCreds,
	}
	b := configB.Serialize()

	actual, err := isValidBigQueryConfigUpdate(a, b)
	assertConfigUpdateResult(t, valid, actual, err, providerType)
}

func testCassandraConfigUpdates(t *testing.T, providerType pt.Type, valid bool) {
	keyspace := "transactions"
	addr := "0.0.0.0:9042"
	username := "featureformer"
	password := "password"
	consistency := "THREE"
	replication := 3

	configA := pc.CassandraConfig{
		Keyspace:    keyspace,
		Addr:        addr,
		Username:    username,
		Password:    password,
		Consistency: consistency,
		Replication: replication,
	}
	a := configA.Serialized()

	if valid {
		username += updateSuffix
		password += updateSuffix
		consistency = "FOUR"
		replication = 4
	} else {
		keyspace += updateSuffix
		addr = "127.0.0.1:9042"
	}

	configB := pc.CassandraConfig{
		Keyspace:    keyspace,
		Addr:        addr,
		Username:    username,
		Password:    password,
		Consistency: consistency,
		Replication: replication,
	}
	b := configB.Serialized()

	actual, err := isValidCassandraConfigUpdate(a, b)
	assertConfigUpdateResult(t, valid, actual, err, providerType)
}

func testDynamoConfigUpdates(t *testing.T, providerType pt.Type, valid bool) {
	prefix := "Featureform_table__"
	region := "us-east-1"
	accessKey := "root"
	secretKey := "secret"

	configA := pc.DynamodbConfig{
		Prefix:    prefix,
		Region:    region,
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
	a := configA.Serialized()

	if valid {
		accessKey += updateSuffix
		secretKey += updateSuffix
	} else {
		region = "us-west-2"
	}

	configB := pc.DynamodbConfig{
		Prefix:    prefix,
		Region:    region,
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
	b := configB.Serialized()

	actual, err := isValidDynamoConfigUpdate(a, b)
	assertConfigUpdateResult(t, valid, actual, err, providerType)
}

func testFirestoreConfigUpdates(t *testing.T, providerType pt.Type, valid bool) {
	exCreds, err := getGCPExampleCreds()
	if err != nil {
		t.Errorf("Failed to get GCP example creds due to error: %v", err)
	}
	projectId := "featureform-gcp"
	collection := "transactions-ds"

	configA := pc.FirestoreConfig{
		ProjectID:   projectId,
		Collection:  collection,
		Credentials: exCreds,
	}
	a := configA.Serialize()

	if valid {
		exCreds["client_email"] = "test@featureform.com"
	} else {
		projectId += updateSuffix
		collection += updateSuffix
	}

	configB := pc.FirestoreConfig{
		ProjectID:   projectId,
		Collection:  collection,
		Credentials: exCreds,
	}
	b := configB.Serialize()

	actual, err := isValidBigQueryConfigUpdate(a, b)
	assertConfigUpdateResult(t, valid, actual, err, providerType)
}

func testMongoConfigUpdates(t *testing.T, providerType pt.Type, valid bool) {
	host := "0.0.0.0"
	port := "27017"
	username := "root"
	password := "password"
	database := "mongo"
	throughput := 1000

	configA := pc.MongoDBConfig{
		Host:       host,
		Port:       port,
		Username:   username,
		Password:   password,
		Database:   database,
		Throughput: throughput,
	}
	a := configA.Serialized()

	if valid {
		username += updateSuffix
		password += updateSuffix
		port = "23456"
		throughput = 2000
	} else {
		host = "127.0.0.1"
		database += updateSuffix
	}

	configB := pc.MongoDBConfig{
		Host:       host,
		Port:       port,
		Username:   username,
		Password:   password,
		Database:   database,
		Throughput: throughput,
	}
	b := configB.Serialized()

	actual, err := isValidMongoConfigUpdate(a, b)
	assertConfigUpdateResult(t, valid, actual, err, providerType)
}

func testPostgresConfigUpdates(t *testing.T, providerType pt.Type, valid bool) {
	host := "0.0.0.0"
	port := "5432"
	username := "postgres"
	password := "password"
	database := "postgres"

	configA := pc.PostgresConfig{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
		Database: database,
	}
	a := configA.Serialize()

	if valid {
		username += updateSuffix
		password += updateSuffix
		port = "5433"
	} else {
		host = "127.0.0.1"
		database += updateSuffix
	}

	configB := pc.PostgresConfig{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
		Database: database,
	}
	b := configB.Serialize()

	actual, err := isValidPostgresConfigUpdate(a, b)
	assertConfigUpdateResult(t, valid, actual, err, providerType)
}

func testRedisConfigUpdates(t *testing.T, providerType pt.Type, valid bool) {
	addr := "0.0.0.0 :=6379"
	password := "password"
	db := 0

	configA := pc.RedisConfig{
		Addr:     addr,
		Password: password,
		DB:       db,
	}
	a := configA.Serialized()

	if valid {
		password += updateSuffix
	} else {
		addr = "127.0.0.1:6379"
		db = 1
	}

	configB := pc.RedisConfig{
		Addr:     addr,
		Password: password,
		DB:       db,
	}
	b := configB.Serialized()

	actual, err := isValidRedisConfigUpdate(a, b)
	assertConfigUpdateResult(t, valid, actual, err, providerType)
}

func testSnowflakeConfigUpdates(t *testing.T, providerType pt.Type, valid bool) {
	username := "featureformer"
	password := "password"
	accountLocator := "xy12345.snowflakecomputing.com"
	organization := "featureform"
	account := "featureform-test"
	database := "transactions_db"
	schema := "fraud"
	warehouse := "ff_wh_xs"
	role := "sysadmin"

	configA := pc.SnowflakeConfig{
		Username:       username,
		Password:       password,
		AccountLocator: accountLocator,
		Organization:   organization,
		Account:        account,
		Database:       database,
		Schema:         schema,
		Warehouse:      warehouse,
		Role:           role,
	}
	a := configA.Serialize()

	if valid {
		username += updateSuffix
		password += updateSuffix
		role += updateSuffix
	} else {
		account += updateSuffix
		organization += updateSuffix
		accountLocator = "za54321.snowflakecomputing.com"
		database += updateSuffix
		schema += updateSuffix
		warehouse += updateSuffix
	}

	configB := pc.SnowflakeConfig{
		Username:       username,
		Password:       password,
		AccountLocator: accountLocator,
		Organization:   organization,
		Account:        account,
		Database:       database,
		Schema:         schema,
		Warehouse:      warehouse,
		Role:           role,
	}
	b := configB.Serialize()

	actual, err := isValidSnowflakeConfigUpdate(a, b)
	assertConfigUpdateResult(t, valid, actual, err, providerType)
}

func testRedshiftConfigUpdates(t *testing.T, providerType pt.Type, valid bool) {
	endpoint := "0.0.0.0"
	port := "5439"
	username := "root"
	password := "password"
	database := "default"

	configA := pc.RedshiftConfig{
		Endpoint: endpoint,
		Port:     port,
		Username: username,
		Password: password,
		Database: database,
	}
	a := configA.Serialize()

	if valid {
		username += updateSuffix
		password += updateSuffix
		port = "5440"
	} else {
		endpoint = "127.0.0.1"
		database += updateSuffix
	}

	configB := pc.RedshiftConfig{
		Endpoint: endpoint,
		Port:     port,
		Username: username,
		Password: password,
		Database: database,
	}
	b := configB.Serialize()

	actual, err := isValidRedshiftConfigUpdate(a, b)
	assertConfigUpdateResult(t, valid, actual, err, providerType)
}

func testK8sConfigUpdates(t *testing.T, providerType pt.Type, valid bool) {
	dockerImage := "featureformcom:latest"
	accountName := "accountname"
	accountKey := "acountkey"
	containerName := "containername"
	path := "containerpath"

	configA := pc.K8sConfig{
		ExecutorType: pc.K8s,
		ExecutorConfig: pc.ExecutorConfig{
			DockerImage: dockerImage,
		},
		StoreType: pc.Azure,
		StoreConfig: &pc.AzureFileStoreConfig{
			AccountName:   accountName,
			AccountKey:    accountKey,
			ContainerName: containerName,
			Path:          path,
		},
	}
	a, err := configA.Serialize()
	if err != nil {
		t.Errorf("failed to serialize config a due to %v", err)
	}

	if valid {
		dockerImage += updateSuffix
		accountKey += updateSuffix
	} else {
		accountName += updateSuffix
		containerName += updateSuffix
		path += updateSuffix
	}

	configB := pc.K8sConfig{
		ExecutorType: pc.K8s,
		ExecutorConfig: pc.ExecutorConfig{
			DockerImage: dockerImage,
		},
		StoreType: pc.Azure,
		StoreConfig: &pc.AzureFileStoreConfig{
			AccountName:   accountName,
			AccountKey:    accountKey,
			ContainerName: containerName,
			Path:          path,
		},
	}
	b, err := configB.Serialize()
	if err != nil {
		t.Errorf("failed to serialize config b due to %v", err)
	}

	actual, err := isValidK8sConfigUpdate(a, b)
	assertConfigUpdateResult(t, valid, actual, err, providerType)
}

func testSparkConfigUpdates(t *testing.T, providerType pt.Type, valid bool) {
	awsAccessKeyId := "awskey"
	awSSecretKey := "awssecret"
	clusterRegion := "us-east-1"
	clusterName := "featureform-clst"
	bucketRegion := "us-east-1"
	bucketPath := "https://featureform.s3.us-east-1.amazonaws.com/transactions"
	path := "https://featureform.s3.us-east-1.amazonaws.com/transactions"

	configA := pc.SparkConfig{
		ExecutorType: pc.EMR,
		ExecutorConfig: &pc.EMRConfig{
			Credentials:   pc.AWSCredentials{AWSAccessKeyId: awsAccessKeyId, AWSSecretKey: awSSecretKey},
			ClusterRegion: clusterRegion,
			ClusterName:   clusterName,
		},
		StoreType: pc.S3,
		StoreConfig: &pc.S3FileStoreConfig{
			Credentials:  pc.AWSCredentials{AWSAccessKeyId: awsAccessKeyId, AWSSecretKey: awSSecretKey},
			BucketRegion: bucketRegion,
			BucketPath:   bucketPath,
			Path:         path,
		},
	}
	a, err := configA.Serialize()
	if err != nil {
		t.Errorf("failed to serialize config a due to %v", err)
	}

	if valid {
		awsAccessKeyId += updateSuffix
		awSSecretKey += updateSuffix
	} else {
		clusterRegion = "us-west-2"
		bucketRegion = "us-west-2"
	}

	configB := pc.SparkConfig{
		ExecutorType: pc.EMR,
		ExecutorConfig: &pc.EMRConfig{
			Credentials:   pc.AWSCredentials{AWSAccessKeyId: awsAccessKeyId, AWSSecretKey: awSSecretKey},
			ClusterRegion: clusterRegion,
			ClusterName:   clusterName,
		},
		StoreType: pc.S3,
		StoreConfig: &pc.S3FileStoreConfig{
			Credentials:  pc.AWSCredentials{AWSAccessKeyId: awsAccessKeyId, AWSSecretKey: awSSecretKey},
			BucketRegion: bucketRegion,
			BucketPath:   bucketPath,
			Path:         path,
		},
	}
	b, err := configB.Serialize()
	if err != nil {
		t.Errorf("failed to serialize config a due to %v", err)
	}

	actual, err := isValidSparkConfigUpdate(a, b)
	assertConfigUpdateResult(t, valid, actual, err, providerType)
}

// ARRANGE FUNCTIONS
func getGCPExampleCreds() (map[string]interface{}, error) {
	gcpCredsBytes, err := ioutil.ReadFile("../provider/test_files/gcp_creds.json")
	if err != nil {
		return nil, err
	}
	var credsDict map[string]interface{}
	if err = json.Unmarshal(gcpCredsBytes, &credsDict); err != nil {
		return nil, err
	}
	return credsDict, nil
}

// ASSERT FUNCTIONS
func assertConfigUpdateResult(t *testing.T, expected, actual bool, actualErr error, providerType pt.Type) {
	if actualErr != nil {
		t.Errorf("Encountered error checking %v config update: %v", providerType, actualErr)
	}
	if expected != actual {
		t.Errorf("Expected %v for %v config update but received %v instead", expected, providerType, actual)
	}
}
