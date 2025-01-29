// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package main

import (
	"context"
	"fmt"

	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/scheduling"
)

func main() {
	logger := logging.NewLogger("local-metadata")
	ctx := logger.AttachToContext(context.Background())
	addr := help.GetEnv("METADATA_PORT", "8080")

	meta, err := scheduling.NewMemoryTaskMetadataManager(ctx)
	config := &metadata.Config{
		Logger:      logger,
		Address:     fmt.Sprintf(":%s", addr),
		TaskManager: meta,
	}
	server, err := metadata.NewMetadataServer(config)
	if err != nil {
		logger.Panicw("Failed to create metadata server", "Err", err)
	}
	if err := server.Serve(); err != nil {
		logger.Errorw("Serve failed with error", "Err", err)
	}
}
