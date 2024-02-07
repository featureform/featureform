package provider

import (
	pt "github.com/featureform/provider/provider_type"
	"testing"
)

func TestOfflineStoreMemory(t *testing.T) {
	store, err := GetOfflineStore(pt.MemoryOffline, []byte{})
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OfflineStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
}
