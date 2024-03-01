package storage_storer

import "testing"

func TestMemoryMetadataStorer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	storage := &MemoryStorerImplementation{
		storage: make(map[string]string),
	}

	test := MetadataStorerTest{
		t:       t,
		storage: storage,
	}
	test.Run()
}
