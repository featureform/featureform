package scheduling

import (
	"testing"
)

func TestStorageProviderETCD(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	storage, err := NewStorageProvider(ETCDStorageProviderType)
	if err != nil {
		t.Fatalf("failed to create etcd storage provider: %v", err)
	}

	// Clean up all keys
	keys, err := storage.ListKeys("")
	if err != nil {
		t.Fatalf("unable to list keys: %v", err)
	}
	for _, key := range keys {
		lockObject, err := storage.Lock(key)
		if err != nil {
			t.Fatalf("could not lock key: %v", err)
		}
		err = storage.Delete(key, lockObject)
		if err != nil {
			t.Fatalf("could not set key: %v", err)
		}
	}

	test := StorageProviderTest{
		t:       t,
		storage: storage,
	}
	test.Run()
}
