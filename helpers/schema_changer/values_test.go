package main

import (
	"context"
	"github.com/featureform/metadata"
	pb "github.com/featureform/metadata/proto"
	"github.com/featureform/provider"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"testing"
)

func createProto(c provider.K8sConfig) *pb.KCFConfig {
	return &pb.KCFConfig{
		ExecutorType: string(c.ExecutorType),
		ExecutorConfig: &pb.ExecutorConfig{
			DockerImage: "",
		},
		StoreType: string(c.StoreType),
		StoreConfig: &pb.KCFConfig_AzureBlobStoreConfig{
			AzureBlobStoreConfig: &pb.AzureBlobStoreConfig{
				AccountName:   c.StoreConfig.AccountName,
				AccountKey:    c.StoreConfig.AccountKey,
				ContainerName: c.StoreConfig.ContainerName,
				ContainerPath: c.StoreConfig.Path,
			},
		},
	}
}

func TestK8sProvider(t *testing.T) {
	lookup, err := initLookup()
	if err != nil {
		t.Fatalf("could not connect to ETCD: %s", err.Error())
	}
	for name, conf := range k8sProviderCases {
		t.Run(name, func(t *testing.T) {
			id := metadata.ResourceID{
				Name:    name,
				Variant: "default",
				Type:    metadata.PROVIDER,
			}
			resource, err := lookup.Lookup(id)
			if err != nil {
				t.Fatalf("could not lookup ID: %v: %s", id, err.Error())
			}

			p := resource.(*dummyProvider)
			serializedConfig := p.serialized.SerializedConfig
			var config *pb.KCFConfig
			err = proto.Unmarshal(serializedConfig, config)
			if err != nil {
				t.Errorf("could not parse serialized config into proto: %s", err.Error())
			}
			expectedProto := createProto(conf)
			if !proto.Equal(config, expectedProto) {
				t.Fatalf("values do not match: got %v, expected %v", config, expectedProto)
			}
		})
	}

	// Check if we have the same number of providers we started with
	client, err := etcdClient()
	if err != nil {
		t.Fatalf("could not get etcd client: %s", err.Error())
	}
	resp, err := client.Get(context.Background(), string(metadata.PROVIDER), clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("could not get providers: %s", err.Error())
	}
	if int(resp.Count) != len(k8sProviderCases) {
		t.Fatalf("Expected %d entries, got %d entries", len(k8sProviderCases), resp.Count)
	}
	_, err = client.Delete(context.Background(), string(metadata.PROVIDER), clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("could not delete providers: %s", err.Error())
	}
}
