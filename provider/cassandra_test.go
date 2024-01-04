package provider

import (
	"testing"

	"github.com/featureform/helpers"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
)

func TestOnlineStoreCassandra(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}
	secrets := GetSecrets("/testing/cassandra")
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	cassandraUsername := helpers.GetEnv("CASSANDRA_USER", secrets["CASSANDRA_USER"])

	cassandraPassword := helpers.GetEnv("CASSANDRA_PASSWORD", secrets["CASSANDRA_PASSWORD"])

	cassandraAddr := "localhost:9042"
	cassandraConfig := &pc.CassandraConfig{
		Addr:        cassandraAddr,
		Username:    cassandraUsername,
		Consistency: "ONE",
		Password:    cassandraPassword,
		Replication: 3,
	}

	store, err := GetOnlineStore(pt.CassandraOnline, cassandraConfig.Serialized())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OnlineStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
}
