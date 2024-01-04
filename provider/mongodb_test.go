package provider

import (
	"testing"

	"github.com/featureform/helpers"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
)

func TestOnlineStoreMongoDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	secrets := GetSecrets("/testing/mongodb")
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	host := helpers.GetEnv("MONGODB_HOST", secrets["MONGODB_HOST"])

	port := helpers.GetEnv("MONGODB_PORT", secrets["MONGODB_PORT"])

	username := helpers.GetEnv("MONGODB_USERNAME", secrets["MONGODB_USERNAME"])

	password := helpers.GetEnv("MONGODB_PASSWORD", secrets["MONGODB_PASSWORD"])

	database := helpers.GetEnv("MONGODB_DATABASE", secrets["MONGODB_DATABASE"])

	mongoConfig := &pc.MongoDBConfig{
		Host:       host,
		Port:       port,
		Username:   username,
		Password:   password,
		Database:   database,
		Throughput: 1000,
	}

	store, err := GetOnlineStore(pt.MongoDBOnline, mongoConfig.Serialized())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OnlineStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
}
