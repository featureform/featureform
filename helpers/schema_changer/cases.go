package main

import (
	"github.com/featureform/metadata"
	pb "github.com/featureform/metadata/proto"
	"github.com/featureform/provider"
	"google.golang.org/protobuf/proto"
)

type dummyProvider struct {
	serialized *pb.Provider
}

func (*dummyProvider) Notify(metadata.ResourceLookup, metadata.Operation, metadata.Resource) error {
	return nil
}
func (*dummyProvider) ID() metadata.ResourceID { return metadata.ResourceID{} }
func (*dummyProvider) Schedule() string        { return "" }
func (*dummyProvider) Dependencies(metadata.ResourceLookup) (metadata.ResourceLookup, error) {
	return nil, nil
}
func (r *dummyProvider) Proto() proto.Message               { return r.serialized }
func (*dummyProvider) UpdateStatus(pb.ResourceStatus) error { return nil }
func (*dummyProvider) UpdateSchedule(string) error          { return nil }

var k8sProviderCases = map[string]provider.K8sConfig{
	"Empty": {
		ExecutorType:   provider.K8s,
		ExecutorConfig: provider.ExecutorConfig{},
		StoreType:      provider.Azure,
		StoreConfig: provider.AzureFileStoreConfig{
			AccountName:   "",
			AccountKey:    "",
			ContainerName: "",
			Path:          "",
		},
	},
	"Simple": {
		ExecutorType:   provider.K8s,
		ExecutorConfig: provider.ExecutorConfig{},
		StoreType:      provider.Azure,
		StoreConfig: provider.AzureFileStoreConfig{
			AccountName:   "Account Name",
			AccountKey:    "Account Key",
			ContainerName: "Container Name",
			Path:          "Path",
		},
	},
}
