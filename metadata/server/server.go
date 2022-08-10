// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"fmt"
	"os"

	"github.com/featureform/metadata"
	"github.com/featureform/metadata/search"
	"go.uber.org/zap"
)

func main() {
	etcdHost := os.Getenv("ETCD_HOST")
	etcdPort := os.Getenv("ETCD_PORT")
	logger := zap.NewExample().Sugar()
	addr := ":8080"
	storageProvider := metadata.EtcdStorageProvider{
		metadata.EtcdConfig{
			Nodes: []metadata.EtcdNode{
				{etcdHost, etcdPort},
			},
		},
	}
	fmt.Println("TS Port", os.Getenv("TYPESENSE_PORT"), "TS HOST", os.Getenv("TYPESENSE_HOST"), "TS KEY", os.Getenv("TYPESENSE_APIKEY"))
	config := &metadata.Config{
		Logger:  logger,
		Address: addr,
		TypeSenseParams: &search.TypeSenseParams{
			Port:   os.Getenv("TYPESENSE_PORT"),
			Host:   os.Getenv("TYPESENSE_HOST"),
			ApiKey: os.Getenv("TYPESENSE_APIKEY"),
		},
		StorageProvider: storageProvider,
	}
	server, err := metadata.NewMetadataServer(config)
	if err != nil {
		logger.Panicw("Failed to create metadata server", "Err", err)
	}
	if err := server.Serve(); err != nil {
		logger.Errorw("Serve failed with error", "Err", err)
	}
}
