package storage

import (
	"testing"

	"github.com/featureform/helpers"
)

func TestETCDMetadataStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	etcdHost := helpers.GetEnv("ETCD_HOST", "localhost")
	etcdPort := helpers.GetEnv("ETCD_PORT", "2379")

	etcdConfig := helpers.ETCDConfig{
		Host: etcdHost,
		Port: etcdPort,
	}

	etcdStorage, err := NewETCDStorageImplementation(etcdConfig)
	if err != nil {
		t.Fatalf("Failed to create ETCD storage: %v", err)
	}

	test := MetadataStorageTest{
		t:       t,
		storage: etcdStorage,
	}
	test.Run()
}
