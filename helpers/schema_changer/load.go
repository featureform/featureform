package main

import (
	"fmt"
	help "github.com/featureform/helpers"
	"github.com/featureform/metadata"
	pb "github.com/featureform/metadata/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

func initLookup() (metadata.ResourceLookup, error) {
	etcd := metadata.EtcdConfig{
		Nodes: []metadata.EtcdNode{
			{help.GetEnv("ETCD_HOST", "localhost"), help.GetEnv("ETCD_PORT", "6379")},
		},
	}

	pr := metadata.EtcdStorageProvider{
		etcd,
	}

	lookup, err := pr.GetResourceLookup()
	if err != nil {
		return nil, err
	}
	return lookup, err
}

func etcdClient() (*clientv3.Client, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:         []string{fmt.Sprintf("%s:%s", help.GetEnv("ETCD_HOST", "localhost"), help.GetEnv("ETCD_PORT", "6379"))},
		AutoSyncInterval:  time.Second * 30,
		DialTimeout:       time.Second * 1,
		DialKeepAliveTime: time.Second * 1,
		Username:          help.GetEnv("ETCD_USERNAME", "root"),
		Password:          help.GetEnv("ETCD_PASSWORD", "secretpassword"),
	})
	if err != nil {
		return nil, err
	}
	return client, nil
}

func insertK8sProvider(lookup metadata.ResourceLookup) {
	fmt.Println("Running K8s Provider Inserts")
	for name, conf := range k8sProviderCases {
		serial, err := conf.Serialize()
		if err != nil {
			fmt.Printf("could not serialize: %v: %s", conf, err.Error())
		}
		id := metadata.ResourceID{
			Name:    name,
			Variant: "default",
			Type:    metadata.PROVIDER,
		}

		resource := &dummyProvider{
			&pb.Provider{
				Name:             "name",
				Description:      "description",
				Type:             "K8s",
				Software:         "software",
				Team:             "Some team",
				SerializedConfig: serial,
			},
		}

		err = lookup.Set(id, resource)
		if err != nil {
			fmt.Printf("could not set resource: %v, %v: %s", id, resource, err.Error())
		}
	}
}

func main() {
	lookup, err := initLookup()
	if err != nil {
		panic(err)
	}
	insertK8sProvider(lookup)

}
