package storage

import "testing"

func TestMemoryMetadataStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	storage, _ := NewMemoryStorageImplementation()

	test := MetadataStorageTest{
		t:       t,
		storage: &storage,
	}
	test.Run()
}
