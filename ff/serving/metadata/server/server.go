// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"github.com/featureform/serving/metadata/search"

	"github.com/featureform/serving/metadata"
	"go.uber.org/zap"
)

func main() {
	logger := zap.NewExample().Sugar()
	addr := ":8080"
	storageProvider := metadata.EtcdStorageProvider{
		metadata.EtcdConfig{
			Nodes: []metadata.EtcdNode{
				{"localhost", "2379"},
			},
		},
	}
	config := &metadata.Config{
		Logger:  logger,
		Address: addr,
		TypeSenseParams: &search.TypeSenseParams{
			Port:   "8108",
			Host:   "localhost",
			ApiKey: "xyz",
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
