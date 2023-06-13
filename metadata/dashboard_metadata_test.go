package metadata

import (
	"testing"

	"golang.org/x/exp/slices"

	pb "github.com/featureform/metadata/proto"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

type mocker struct {
}

func (mocker) GetProvider() string {
	return "test.provider"
}

func (mocker) GetCreated() *tspb.Timestamp {
	return tspb.Now()
}

func (mocker) GetLastUpdated() *tspb.Timestamp {
	return tspb.Now()
}

func (mocker) GetTags() *pb.Tags {
	return &pb.Tags{Tag: []string{"test.active", "test.inactive"}}
}

func (mocker) GetProperties() *pb.Properties {
	propertyMock := &pb.Properties{Property: map[string]*pb.Property{}}
	propertyMock.Property["test.map.key"] = &pb.Property{Value: &pb.Property_StringValue{StringValue: "test.map.value"}}
	return &pb.Properties{Property: propertyMock.Property}
}

func getSourceVariant() *SourceVariant {
	sv := &SourceVariant{
		serialized:           &pb.SourceVariant{Name: "test.name", Variant: "test.variant"},
		fetchFeaturesFns:     fetchFeaturesFns{},
		fetchLabelsFns:       fetchLabelsFns{},
		fetchProviderFns:     fetchProviderFns{getter: mocker{}},
		fetchTrainingSetsFns: fetchTrainingSetsFns{},
		createdFn:            createdFn{getter: mocker{}},
		lastUpdatedFn:        lastUpdatedFn{getter: mocker{}},
		fetchTagsFn:          fetchTagsFn{getter: mocker{}},
		fetchPropertiesFn:    fetchPropertiesFn{getter: mocker{}},
		protoStringer:        protoStringer{},
	}
	return sv
}

func TestSourceShallowMapOK(t *testing.T) {
	sv := getSourceVariant()

	sourceVariantResource := SourceShallowMap(sv)

	assertEqual(t, sv.serialized.Name, sourceVariantResource.Name)
	assertEqual(t, sv.serialized.Variant, sourceVariantResource.Variant)
	assertEqual(t, sv.Provider(), sourceVariantResource.Provider)
	assertEqual(t, len(sv.Tags()), len(sourceVariantResource.Tags))
	assertEqual(t, slices.Contains(sourceVariantResource.Tags, "test.active"), true)
	assertEqual(t, slices.Contains(sourceVariantResource.Tags, "test.inactive"), true)
	assertEqual(t, sv.Properties()["test.map.key"], sourceVariantResource.Properties["test.map.key"])
}
