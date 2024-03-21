package scheduling

import (
	"testing"
)

func TestStorageProviderMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	// storage := NewMemoryStorageProvider()

	// test := StorageProviderTest{
	// 	t:       t,
	// 	storage: storage,
	// }
	// test.Run()
}
