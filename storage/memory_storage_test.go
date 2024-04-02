package storage

import "testing"

func TestMemoryMetadataStorage(t *testing.T) {
	storage, err := NewMemoryStorageImplementation()
	if err != nil {
		t.Fatalf("Failed to create Memory storage: %v", err)
	}

	test := MetadataStorageTest{
		t:       t,
		storage: &storage,
	}
	test.Run()
}
