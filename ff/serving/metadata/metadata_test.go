package metadata

import (
	pb "github.com/featureform/serving/metadata/proto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestMethodsEmpty(t *testing.T) {
	lookup := make(localResourceLookup)
	testId := ResourceID{
		Name:    "Fraud",
		Variant: "default",
		Type:    ENTITY,
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
		t.Fatalf("Failed to get the correct number of results %d instead of 0", len(listAll))
	}
	listByType, errListForType := lookup.ListForType(ENTITY)
	if errListForType != nil {
		t.Fatalf("Failed to list resources (by type) prior to setting a resource %s", errListForType)
	}
	if len(listByType) > 0 {
		t.Fatalf("Failed to get the correct number of results %d instead of 0", len(listByType))
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
		Type:    SOURCE,
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
		t.Fatalf("Failed to get the correct number of results %d instead of 1", len(listAll))
	}
	listByType, errListForType := lookup.ListForType(SOURCE)
	if errListForType != nil {
		t.Fatalf("Failed to list resources (by type) for the 1 resource case %s", errListForType)
	}
	if len(listByType) != 1 {
		t.Fatalf("Failed to get the correct number of results, %d instead of 1", len(listByType))
	}

}

func TestMethodsThreeResource(t *testing.T) {
	lookup := make(localResourceLookup)
	testIds := []ResourceID{
		{
			Name:    "Fraud",
			Variant: "default",
			Type:    PROVIDER,
		}, {
			Name:    "Time",
			Variant: "second",
			Type:    FEATURE,
		}, {
			Name:    "Ice-Cream",
			Variant: "third-variant",
			Type:    MODEL,
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
		t.Fatalf("Failed to get the correct number of results, %d instead of 3", len(listAll))
	}
	listByType, errListForType := lookup.ListForType(FEATURE)
	if errListForType != nil {
		t.Fatalf("Failed to list resources (by type) for the 1 resource case %s", errListForType)
	}
	if len(listByType) != 1 {
		t.Fatalf("Failed to get the correct number of results %d instead of 1", len(listByType))
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
		t.Fatalf("Failed to return a correct len of submap, %d instead of 3", len(listsubmap))
	}
	resNotContain, errSubmapNotContain := lookup.Submap([]ResourceID{
		{
			Name:    "Banking",
			Variant: "Secondary",
			Type:    TRANSFORMATION_VARIANT,
		},
	})
	if errSubmapNotContain == nil && resNotContain != nil {
		t.Fatalf("Failed to catch error not found in submap %s", errSubmap)
	}
}

func TestFeatureVariant(t *testing.T) {
	sugar := zap.NewExample().Sugar()
	server := Config{
		Logger: sugar,
	}
	metadataServer, err := NewMetadataServer(&server)
	if err != nil {
		t.Fatalf("Failed to set up metadataserver %s", err)
	}
	var r pb.Metadata_ListFeaturesServer
	var g pb.Empty
	if err := metadataServer.ListFeatures(&g, r); err != nil {
		t.Fatalf("Failed to list features (metadataserver) %s", err)
	}
}

func TestResourceIDmethods(t *testing.T) {
	resourceId := ResourceID{
		Name:    "Fraud",
		Variant: "default",
		Type:    TRAINING_SET_VARIANT,
	}
	resourceProto := resourceId.Proto()
	if resourceProto.Name != resourceId.Name {
		t.Fatalf("Failed to correctly complete resourceId.Proto (mismatch name)")
	}
	if resourceProto.Variant != resourceId.Variant {
		t.Fatalf("Failed to correctly complete resourceId.Proto (mismatch variant)")
	}
	_, hasParent := resourceId.Parent()
	if hasParent != false {
		t.Fatalf("Failed to correctly set boolean (false) for resourceId.Parent()")
	}
}

func TestResourceIDmethodsParent(t *testing.T) {
	resourceId := ResourceID{
		Name:    "Fraud",
		Variant: "default",
		Type:    TRAINING_SET_VARIANT,
	}
	parentResourceID, hasParent := resourceId.Parent()
	if hasParent != true {
		t.Fatalf("Failed to correctly set boolean (true) for resourceId.Parent()")
	}
	if parentResourceID.Name != resourceId.Name {
		t.Fatalf("Failed to correctly set resource (Name field) for resourceId.Parent()")
	}
}
