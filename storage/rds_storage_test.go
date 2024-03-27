package storage

import (
	"testing"
)

func TestRDSMetadataStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	etcdStorage, err := NewRDSStorageImplementation("test_table")
	if err != nil {
		t.Fatalf("Failed to create RDS storage: %v", err)
	}

	test := MetadataStorageTest{
		t:       t,
		storage: etcdStorage,
	}
	test.Run()
}
