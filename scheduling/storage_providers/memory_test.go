package scheduling

import (
	"testing"
)

func TestStorageProviderMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	storage, err := NewStorageProvider(MemoryStorageProviderType)
	if err != nil {
		t.Fatalf("failed to create memory storage provider: %v", err)
	}

	test := StorageProviderTest{
		t:       t,
		storage: storage,
	}
	test.Run()
}
