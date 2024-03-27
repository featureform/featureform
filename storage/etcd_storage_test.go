package storage

import (
	"testing"
)

func TestETCDMetadataStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	etcdStorage, err := NewETCDStorageImplementation()
	if err != nil {
		t.Fatalf("Failed to create ETCD storage: %v", err)
	}

	test := MetadataStorageTest{
		t:       t,
		storage: etcdStorage,
	}
	test.Run()
}
