package metadata

import (
	"github.com/featureform/serving/metadata/search"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"testing"
)

// type ResourceLookup interface {
// 	Lookup(ResourceID) (Resource, error)
// 	Has(ResourceID) (bool, error)
// 	Set(ResourceID, Resource) error
// 	Submap([]ResourceID) (ResourceLookup, error)
// 	ListForType(ResourceType) ([]Resource, error)
// 	List() ([]Resource, error)

func TestMethodsEmpty(t *testing.T) {
	lookup := make(localResourceLookup)
	testId := ResourceID{
		Name:    "Fraud",
		Variant: "default",
		Type:    ResourceType("CSV"),
	}
	if _, err := lookup.Lookup(testId); err == nil {
		t.Fatalf("Failed to return a Lookup error because %s", err)
	}
	containsId, err := lookup.Has(testId)
	if err != nil {
		t.Fatalf("Failed to run Has function %s", err)
	}
	if containsId == true {
		t.Fatalf("Failed to return false in Has function for the empty case")
	}
	listAll, err := lookup.List()
	if err != nil {
		t.Fatalf("Failed to list all resources prior to setting a resource %s", err)
	}
	if len(listAll) > 0 {
		t.Fatalf("Failed to get the correct number of results = 0")
	}
	var resourceType ResourceType = "CSV"
	listByType, errListForType := lookup.ListForType(resourceType)
	if errListForType != nil {
		t.Fatalf("Failed to list resources (by type) prior to setting a resource %s", errListForType)
	}
	if len(listByType) > 0 {
		t.Fatalf("Failed to get the correct number of results (0)")
	}
}

type mockResource struct {
	id ResourceID
}

func (res *mockResource) ID() ResourceID {
	return res.id
}

func (res *mockResource) Dependencies(ResourceLookup) (ResourceLookup, error) {
	return nil, nil
}

func (res *mockResource) Notify(ResourceLookup, operation, Resource) error {
	return nil
}

func (res *mockResource) Proto() proto.Message {
	return res.id.Proto()
}

func TestMethodsOneResource(t *testing.T) {
	lookup := make(localResourceLookup)
	testId := ResourceID{
		Name:    "Fraud",
		Variant: "default",
		Type:    ResourceType("CSV"),
	}
	v := mockResource{
		id: testId,
	}
	if err := lookup.Set(testId, &v); err != nil {
		t.Fatalf("Failed to set resource %s", err)
	}
	if _, err := lookup.Lookup(testId); err != nil {
		t.Fatalf("Failed to return a Lookup error because %s", err)
	}
	containsId, err := lookup.Has(testId)
	if err != nil {
		t.Fatalf("Failed to run Has function %s", err)
	}
	if containsId == false {
		t.Fatalf("Failed to return true in Has function for the 1 resource case")
	}
	listAll, err := lookup.List()
	if err != nil {
		t.Fatalf("Failed to list all resources for the 1 resource case %s", err)
	}
	if len(listAll) != 1 {
		t.Fatalf("Failed to get the correct number of results = 1")
	}
	var resourceType ResourceType = "CSV"
	listByType, errListForType := lookup.ListForType(resourceType)
	if errListForType != nil {
		t.Fatalf("Failed to list resources (by type) for the 1 resource case %s", errListForType)
	}
	if len(listByType) != 1 {
		t.Fatalf("Failed to get the correct number of results (1)")
	}

}

func TestMethodsThreeResource(t *testing.T) {
	lookup := make(localResourceLookup)
	testIds := []ResourceID{
		{
			Name:    "Fraud",
			Variant: "default",
			Type:    ResourceType("CSV"),
		}, {
			Name:    "Time",
			Variant: "second",
			Type:    ResourceType("Feature"),
		}, {
			Name:    "Ice-Cream",
			Variant: "third-variant",
			Type:    ResourceType("user"),
		},
	}
	for _, resource := range testIds {
		if err := lookup.Set(resource, &mockResource{id: resource}); err != nil {
			t.Fatalf("Failed to Set %s", err)
		}
	}
	if _, err := lookup.Lookup(testIds[0]); err != nil {
		t.Fatalf("Failed to return a Lookup error because %s", err)
	}
	containsId, err := lookup.Has(testIds[1])
	if err != nil {
		t.Fatalf("Failed to run Has function %s", err)
	}
	if containsId == false {
		t.Fatalf("Failed to return true in Has function for the 3 resource case")
	}
	listAll, err := lookup.List()
	if err != nil {
		t.Fatalf("Failed to list all resources for the 3 resource case %s", err)
	}
	if len(listAll) != 3 {
		t.Fatalf("Failed to get the correct number of results = 3")
	}
	var resourceType ResourceType = "CSV"
	listByType, errListForType := lookup.ListForType(resourceType)
	if errListForType != nil {
		t.Fatalf("Failed to list resources (by type) for the 1 resource case %s", errListForType)
	}
	if len(listByType) != 1 {
		t.Fatalf("Failed to get the correct number of results (1)")
	}
	res, errSubmap := lookup.Submap(testIds)
	if errSubmap != nil {
		t.Fatalf("Failed to create a submap %s", errSubmap)
	}
	listsubmap, errList := res.List()
	if errList != nil {
		t.Fatalf("Failed to get list of submap %s", errList)
	}
	if len(listsubmap) != 3 {
		t.Fatalf("Failed to return a corrects submap")
	}
	resNotContain, errSubmapNotContain := lookup.Submap([]ResourceID{
		{
			Name:    "Banking",
			Variant: "Secondary",
			Type:    ResourceType("provider"),
		},
	})
	if errSubmapNotContain == nil && resNotContain != nil {
		t.Fatalf("Failed to catch error not found in submap %s", errSubmap)
	}
}

func TestMethodsTypesense(t *testing.T) {
	sugar := zap.NewExample().Sugar()
	server := Config{
		Logger: sugar,
		TypeSenseParams: &search.TypeSenseParams{
			Host:   "localhost",
			Port:   "8108",
			ApiKey: "xyz",
		},
	}
	testIds := []ResourceID{
		{
			Name:    "Fraud",
			Variant: "default",
			Type:    ResourceType("CSV"),
		}, {
			Name:    "Time",
			Variant: "second",
			Type:    ResourceType("Feature"),
		}, {
			Name:    "Ice-Cream",
			Variant: "third-variant",
			Type:    ResourceType("user"),
		},
	}
	metaTypeServer, err := NewMetadataServer(&server)
	if err != nil {
		t.Fatalf("Failed to set up typesense %s", err)
	}
	for _, resource := range testIds {
		if err := metaTypeServer.lookup.Set(resource, &mockResource{id: resource}); err != nil {
			t.Fatalf("Failed to Set %s", err)
		}
	}
}

func TestMethodsTypesenseError(t *testing.T) {
	sugar := zap.NewExample().Sugar()
	server := Config{
		Logger: sugar,
		TypeSenseParams: &search.TypeSenseParams{
			Host:   "localhost",
			Port:   "8118",
			ApiKey: "xyz",
		},
	}
	_, err := NewMetadataServer(&server)
	if err == nil {
		t.Fatalf("Failed to set up typesense %s", err)
	}
}
