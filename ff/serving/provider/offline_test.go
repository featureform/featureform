package provider

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/google/uuid"
)

func TestOfflineStores(t *testing.T) {
	testFns := map[string]func(*testing.T, OfflineStore){
		"CreateGetTable":     testCreateGetOfflineTable,
		"TableAlreadyExists": testOfflineTableAlreadyExists,
		"TableNotFound":      testOfflineTableNotFound,
		"InvalidResourceIDs": testInvalidResourceIDs,
	}
	testList := []struct {
		t               Type
		c               SerializedConfig
		integrationTest bool
	}{
		{MemoryOffline, []byte{}, false},
	}
	for _, testItem := range testList {
		if testing.Short() && testItem.integrationTest {
			t.Logf("Skipping %s, because it is an integration test", testItem.t)
			continue
		}
		for name, fn := range testFns {
			provider, err := Get(testItem.t, testItem.c)
			if err != nil {
				t.Fatalf("Failed to get provider %s: %s", testItem.t, err)
			}
			store, err := provider.AsOfflineStore()
			if err != nil {
				t.Fatalf("Failed to use provider %s as OfflineStore: %s", testItem.t, err)
			}
			testName := fmt.Sprintf("%s_%s", testItem.t, name)
			t.Run(testName, func(t *testing.T) {
				fn(t, store)
			})
		}
	}
}

func randomResourceID() ResourceID {
	possibleTypes := []OfflineResourceType{Label, Feature}
	randType := possibleTypes[rand.Intn(2)]
	return ResourceID{
		Name:    uuid.NewString(),
		Variant: uuid.NewString(),
		Type:    randType,
	}
}

func testCreateGetOfflineTable(t *testing.T, store OfflineStore) {
	id := randomResourceID()
	if tab, err := store.CreateResourceTable(id); tab == nil || err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if tab, err := store.GetResourceTable(id); tab == nil || err != nil {
		t.Fatalf("Failed to get table: %s", err)
	}
}

func testOfflineTableAlreadyExists(t *testing.T, store OfflineStore) {
	id := randomResourceID()
	if _, err := store.CreateResourceTable(id); err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if _, err := store.CreateResourceTable(id); err == nil {
		t.Fatalf("Succeeded in creating table twice")
	} else if casted, valid := err.(*TableAlreadyExists); !valid {
		t.Fatalf("Wrong error for table already exists: %T", err)
	} else if casted.Error() == "" {
		t.Fatalf("TableAlreadyExists has empty error message")
	}
}

func testOfflineTableNotFound(t *testing.T, store OfflineStore) {
	id := randomResourceID()
	if _, err := store.GetResourceTable(id); err == nil {
		t.Fatalf("Succeeded in getting non-existant table")
	} else if casted, valid := err.(*TableNotFound); !valid {
		t.Fatalf("Wrong error for table not found: %T", err)
	} else if casted.Error() == "" {
		t.Fatalf("TableNotFound has empty error message")
	}
}

func testInvalidResourceIDs(t *testing.T, store OfflineStore) {
	invalidIds := []ResourceID{
		{Type: Feature},
		{Name: uuid.NewString()},
	}
	for _, id := range invalidIds {
		if _, err := store.CreateResourceTable(id); err == nil {
			t.Fatalf("Succeeded in creating invalid ResourceID: %v", id)
		}
	}
}
