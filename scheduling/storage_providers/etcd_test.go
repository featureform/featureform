package scheduling

import (
	"testing"
)

func TestStorageProviderETCD(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	storage, err := NewStorageProvider("etcd")
	if err != nil {
		t.Fatalf("failed to create etcd storage provider: %v", err)
	}

	test := StorageProviderTest{
		t:       t,
		storage: storage,
	}
	test.Run()
}
