package health

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"

	fs "github.com/featureform/filestore"
	"github.com/featureform/metadata"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
	"go.uber.org/zap/zaptest"
)

var providerType = flag.String("provider", "", "provider type under test")

type testMember struct {
	providerDef metadata.ProviderDef
}

func checkEnv(envVar string) string {
	value, has := os.LookupEnv(envVar)
	if !has {
		panic(fmt.Sprintf("Environment variable not found: %s", envVar))
	}
	return value
}

func initSpark(t *testing.T, executorType pc.SparkExecutorType, storeType fs.FileStoreType) (pc.SerializedConfig, pc.SparkConfig) {
	var executorConfig pc.SparkExecutorConfig

	switch executorType {
	case pc.SparkGeneric:
		executorConfig = &pc.SparkGenericConfig{
			Master:        os.Getenv("GENERIC_SPARK_MASTER"),
			DeployMode:    os.Getenv("GENERIC_SPARK_DEPLOY_MODE"),
			PythonVersion: os.Getenv("GENERIC_SPARK_PYTHON_VERSION"),
		}
	case pc.Databricks:
		executorConfig = &pc.DatabricksConfig{
			Host:    os.Getenv("DATABRICKS_HOST"),
			Token:   os.Getenv("DATABRICKS_TOKEN"),
			Cluster: os.Getenv("DATABRICKS_CLUSTER"),
		}
	case pc.EMR:
		executorConfig = &pc.EMRConfig{
			Credentials: pc.AWSCredentials{
				AWSAccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
				AWSSecretKey:   os.Getenv("AWS_SECRET_KEY"),
			},
			ClusterRegion: os.Getenv("AWS_EMR_CLUSTER_REGION"),
			ClusterName:   os.Getenv("AWS_EMR_CLUSTER_ID"),
		}
	default:
		t.Fatalf("Invalid executor type: %v", executorType)
	}

	var fileStoreConfig pc.SparkFileStoreConfig
	switch storeType {
	case fs.S3:
		fileStoreConfig = &pc.S3FileStoreConfig{
			Credentials: pc.AWSCredentials{
				AWSAccessKeyId: os.Getenv("AWS_ACCESS_KEY_ID"),
				AWSSecretKey:   os.Getenv("AWS_SECRET_KEY"),
			},
			BucketRegion: os.Getenv("S3_BUCKET_REGION"),
			BucketPath:   os.Getenv("S3_BUCKET_PATH"),
			Path:         os.Getenv(""),
		}
	case fs.GCS:
		credsFile := os.Getenv("GCP_CREDENTIALS_FILE")
		content, err := ioutil.ReadFile(credsFile)
		if err != nil {
			t.Errorf("Error when opening file: %v", err)
		}
		var creds map[string]interface{}
		err = json.Unmarshal(content, &creds)
		if err != nil {
			t.Errorf("Error during Unmarshal() creds: %v", err)
		}

		fileStoreConfig = &pc.GCSFileStoreConfig{
			BucketName: os.Getenv("GCS_BUCKET_NAME"),
			BucketPath: "",
			Credentials: pc.GCPCredentials{
				ProjectId: os.Getenv("GCP_PROJECT_ID"),
				JSON:      creds,
			},
		}
	case fs.Azure:
		fileStoreConfig = &pc.AzureFileStoreConfig{
			AccountName:   os.Getenv("AZURE_ACCOUNT_NAME"),
			AccountKey:    os.Getenv("AZURE_ACCOUNT_KEY"),
			ContainerName: os.Getenv("AZURE_CONTAINER_NAME"),
			Path:          os.Getenv("AZURE_CONTAINER_PATH"),
		}
	default:
		t.Fatalf("Invalid store type: %v", storeType)
	}

	var sparkConfig = pc.SparkConfig{
		ExecutorType:   executorType,
		ExecutorConfig: executorConfig,
		StoreType:      storeType,
		StoreConfig:    fileStoreConfig,
	}

	serializedConfig, err := sparkConfig.Serialize()
	if err != nil {
		t.Fatalf("Cannot serialize Spark config with %s executor and %s files tore: %v", executorType, storeType, err)
	}
	return serializedConfig, sparkConfig
}

func initProvider(t *testing.T, providerType pt.Type, executorType pc.SparkExecutorType, storeType fs.FileStoreType) pc.SerializedConfig {
	switch providerType {
	case pt.RedisOnline:
		port := checkEnv("REDIS_INSECURE_PORT")

		redisConfig := pc.RedisConfig{
			Addr: fmt.Sprintf("%s:%s", "0.0.0.0", port),
		}
		return redisConfig.Serialized()
	case pt.PostgresOffline:
		db := checkEnv("POSTGRES_DB")
		user := checkEnv("POSTGRES_USER")
		password := checkEnv("POSTGRES_PASSWORD")

		postgresConfig := pc.PostgresConfig{
			Host:     "0.0.0.0",
			Port:     "5432",
			Database: db,
			Username: user,
			Password: password,
			SSLMode:  "disable",
		}
		return postgresConfig.Serialize()
	case pt.SparkOffline:
		serializedConfig, _ := initSpark(t, executorType, storeType)
		return serializedConfig
	case pt.DynamoDBOnline:
		key := checkEnv("DYNAMODB_KEY")
		secret := checkEnv("DYNAMODB_SECRET")
		region := checkEnv("DYNAMODB_REGION")

		dynamodbConfig := pc.DynamodbConfig{
			AccessKey: key,
			SecretKey: secret,
			Region:    region,
		}

		return dynamodbConfig.Serialized()
	case pt.SnowflakeOffline:
		user := checkEnv("SNOWFLAKE_USERNAME")
		password := checkEnv("SNOWFLAKE_PASSWORD")
		account := checkEnv("SNOWFLAKE_ACCOUNT")
		org := checkEnv("SNOWFLAKE_ORG")

		snowflakeConfig := pc.SnowflakeConfig{
			Username:     user,
			Password:     password,
			Account:      account,
			Organization: org,
			Database:     "SNOWFLAKE_SAMPLE_DATA",
			Schema:       "TPCH_SF1",
			Role:         "PUBLIC",
		}

		return snowflakeConfig.Serialize()
	default:
		panic(fmt.Sprintf("Unsupported provider type: %s", providerType))
	}

}

func TestHealth_Check(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	err := godotenv.Load("../.env")
	if err != nil {
		t.Fatal(err)
	}

	os.Setenv("TZ", "UTC")

	tests := []testMember{}

	if *providerType == "redis" || *providerType == "" {
		config := initProvider(t, pt.RedisOnline, "", "")
		tests = append(tests, testMember{
			providerDef: metadata.ProviderDef{
				Name:             "redis",
				Type:             string(pt.RedisOnline),
				SerializedConfig: config,
				Software:         "redis",
				Tags:             metadata.Tags{},
				Properties:       metadata.Properties{},
			},
		})
	}

	if *providerType == "postgres" || *providerType == "" {
		config := initProvider(t, pt.PostgresOffline, "", "")
		tests = append(tests, testMember{
			providerDef: metadata.ProviderDef{
				Name:             "postgres",
				Type:             string(pt.PostgresOffline),
				SerializedConfig: config,
				Software:         "postgres",
				Tags:             metadata.Tags{},
				Properties:       metadata.Properties{},
			},
		})
	}

	if *providerType == "spark-databricks-s3" || *providerType == "" {
		config := initProvider(t, pt.SparkOffline, pc.Databricks, fs.S3)
		tests = append(tests, testMember{
			providerDef: metadata.ProviderDef{
				Name:             "spark-databricks-s3",
				Type:             string(pt.SparkOffline),
				SerializedConfig: config,
				Software:         "spark",
				Tags:             metadata.Tags{},
				Properties:       metadata.Properties{},
			},
		})
	}

	if *providerType == "spark-databricks-abs" || *providerType == "" {
		config := initProvider(t, pt.SparkOffline, pc.Databricks, fs.Azure)
		tests = append(tests, testMember{
			providerDef: metadata.ProviderDef{
				Name:             "spark-databricks-abs",
				Type:             string(pt.SparkOffline),
				SerializedConfig: config,
				Software:         "spark",
				Tags:             metadata.Tags{},
				Properties:       metadata.Properties{},
			},
		})
	}

	if *providerType == "spark-emr-s3" || *providerType == "" {
		config := initProvider(t, pt.SparkOffline, pc.EMR, fs.S3)
		tests = append(tests, testMember{
			providerDef: metadata.ProviderDef{
				Name:             "spark-emr-s3",
				Type:             string(pt.SparkOffline),
				SerializedConfig: config,
				Software:         "spark",
				Tags:             metadata.Tags{},
				Properties:       metadata.Properties{},
			},
		})
	}

	if *providerType == "dynamodb" || *providerType == "" {
		config := initProvider(t, pt.DynamoDBOnline, "", "")
		tests = append(tests, testMember{
			providerDef: metadata.ProviderDef{
				Name:             "dynamodb",
				Type:             string(pt.DynamoDBOnline),
				SerializedConfig: config,
				Software:         "dynamodb",
				Tags:             metadata.Tags{},
				Properties:       metadata.Properties{},
			},
		})
	}

	if *providerType == "snowflake" || *providerType == "" {
		config := initProvider(t, pt.SnowflakeOffline, "", "")
		tests = append(tests, testMember{
			providerDef: metadata.ProviderDef{
				Name:             "snowflake",
				Type:             string(pt.SnowflakeOffline),
				SerializedConfig: config,
				Software:         "snowflake",
				Tags:             metadata.Tags{},
				Properties:       metadata.Properties{},
			},
		})
	}

	server, addr := initMetadataServer(t)

	fmt.Printf("Metadata server listening on %s\n", addr)

	client := initClient(t, addr)

	health := NewHealth(client)

	for _, test := range tests {
		if err := client.Create(context.Background(), test.providerDef); err != nil {
			t.Fatalf("Failed to create provider: %s", err)
		}
		t.Run(string(test.providerDef.Type), func(t *testing.T) {
			isHealthy, err := health.Check(test.providerDef.Name)
			if err != nil {
				t.Fatalf("Failed to check provider health: %s", err)
			}
			if !isHealthy {
				t.Fatalf("Provider is not healthy")
			}
		})
	}

	if err := server.Stop(); err != nil {
		t.Fatalf("Failed to stop metadata server: %s", err)
	}
}

func initMetadataServer(t *testing.T) (*metadata.MetadataServer, string) {
	logger := zaptest.NewLogger(t)
	config := &metadata.Config{
		Logger:          logger.Sugar(),
		StorageProvider: metadata.LocalStorageProvider{},
	}
	server, err := metadata.NewMetadataServer(config)
	if err != nil {
		panic(err)
	}
	// listen on a random port
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	go func() {
		if err := server.ServeOnListener(lis); err != nil {
			panic(err)
		}
	}()
	return server, lis.Addr().String()
}

func initClient(t *testing.T, addr string) *metadata.Client {
	logger := zaptest.NewLogger(t).Sugar()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		t.Fatalf("Failed to create client: %s", err)
	}
	return client
}
