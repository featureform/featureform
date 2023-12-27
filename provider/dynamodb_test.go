package provider

import (
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
	"os"
	"testing"
)

func TestOnlineStoreDynamoDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	dynamoAccessKey, ok := os.LookupEnv("DYNAMO_ACCESS_KEY")
	if !ok {
		t.Fatalf("missing DYNAMO_ACCESS_KEY variable")
	}
	dynamoSecretKey, ok := os.LookupEnv("DYNAMO_SECRET_KEY")
	if !ok {
		t.Fatalf("missing DYNAMO_SECRET_KEY variable")
	}
	dynamoConfig := &pc.DynamodbConfig{
		Region:    "us-east-1",
		AccessKey: dynamoAccessKey,
		SecretKey: dynamoSecretKey,
	}

	store, err := GetOnlineStore(pt.DynamoDBOnline, dynamoConfig.Serialized())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OnlineStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
}
