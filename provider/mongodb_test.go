package provider

import (
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
	"os"
	"testing"
)

func TestOnlineStoreMongoDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	host, ok := os.LookupEnv("MONGODB_HOST")
	if !ok {
		t.Fatalf("missing MONGODB_HOST variable")
	}
	port, ok := os.LookupEnv("MONGODB_PORT")
	if !ok {
		t.Fatalf("missing MONGODB_PORT variable")
	}
	username, ok := os.LookupEnv("MONGODB_USERNAME")
	if !ok {
		t.Fatalf("missing MONGODB_USERNAME variable")
	}
	password, ok := os.LookupEnv("MONGODB_PASSWORD")
	if !ok {
		t.Fatalf("missing MONGODB_PASSWORD variable")
	}
	database, ok := os.LookupEnv("MONGODB_DATABASE")
	if !ok {
		t.Fatalf("missing MONGODB_DATABASE variable")
	}
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
