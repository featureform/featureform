package provider

import (
	"testing"

	"github.com/featureform/helpers"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
)

func TestOfflineStorePostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	secrets := GetSecrets("/testing/postgres")
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}

	db := helpers.GetEnv("POSTGRES_DB", secrets["POSTGRES_DB"])

	user := helpers.GetEnv("POSTGRES_USER", secrets["POSTGRES_USER"])

	password := helpers.GetEnv("POSTGRES_PASSWORD", secrets["POSTGRES_PASSWORD"])

	postgresConfig := pc.PostgresConfig{
		Host:     "localhost",
		Port:     "5432",
		Database: db,
		Username: user,
		Password: password,
		SSLMode:  "disable",
	}

	store, err := GetOfflineStore(pt.PostgresOffline, postgresConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OfflineStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
	test.RunSQL()
}
