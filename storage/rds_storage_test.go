package storage

import (
	"testing"

	"github.com/featureform/helpers"
)

func TestRDSMetadataStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	host := helpers.GetEnv("POSTGRES_HOST", "localhost")
	port := helpers.GetEnv("POSTGRES_PORT", "5432")
	username := helpers.GetEnv("POSTGRES_USER", "postgres")
	password := helpers.GetEnv("POSTGRES_PASSWORD", "mysecretpassword")
	dbName := helpers.GetEnv("POSTGRES_DB", "postgres")
	sslMode := helpers.GetEnv("POSTGRES_SSL_MODE", "disable")

	config := helpers.RDSConfig{
		Host:     host,
		Port:     port,
		User:     username,
		Password: password,
		DBName:   dbName,
		SSLMode:  sslMode,
	}

	etcdStorage, err := NewRDSStorageImplementation(config, "test_table")
	if err != nil {
		t.Fatalf("Failed to create RDS storage: %v", err)
	}

	test := MetadataStorageTest{
		t:       t,
		storage: etcdStorage,
	}
	test.Run()
}
