package provider

import (
	"testing"

	"github.com/featureform/helpers"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
)

func TestOnlineStoreDynamoDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}
	secrets := GetSecrets("testing/dynamodb")
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	dynamoAccessKey := helpers.GetEnv("DYNAMO_ACCESS_KEY", secrets["DYNAMO_ACCESS_KEY"])

	dynamoSecretKey := helpers.GetEnv("DYNAMO_SECRET_KEY", secrets["DYNAMO_SECRET_KEY"])

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
