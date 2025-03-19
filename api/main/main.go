// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package main

import (
	"fmt"

	"github.com/joho/godotenv"

	"github.com/featureform/api"
	"github.com/featureform/health"
	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Printf("could not fetch .env file: %s", err.Error())
	}

	logger := logging.NewLogger("api")
	apiPort := help.GetEnv("API_PORT", "7878")
	logger.Infow("Retrieved API port from ENV", "port", apiPort)
	apiStatusPort := help.GetEnv("API_STATUS_PORT", "8443")
	logger.Infow("Retrieved API status port from ENV", "port", apiStatusPort)
	metadataHost := help.GetEnv("METADATA_HOST", "localhost")
	logger.Infow("Retrieved metadata host from ENV", "host", metadataHost)
	metadataPort := help.GetEnv("METADATA_PORT", "8080")
	logger.Infow("Retrieved metadata port from ENV", "port", metadataPort)
	servingHost := help.GetEnv("SERVING_HOST", "localhost")
	logger.Infow("Retrieved serving host from ENV", "host", servingHost)
	servingPort := help.GetEnv("SERVING_PORT", "8080")
	logger.Infow("Retrieved serving port from ENV", "port", servingPort)
	apiConn := fmt.Sprintf("0.0.0.0:%s", apiPort)
	metadataConn := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	servingConn := fmt.Sprintf("%s:%s", servingHost, servingPort)

	if err := health.StartHttpServer(logger, apiStatusPort); err != nil {
		logger.Errorw("Error starting health check HTTP server", "error", err)
		panic(fmt.Sprintf("health check HTTP server failed: %+v", err))
	}

	serv, err := api.NewApiServer(logger, apiConn, metadataConn, servingConn)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(serv.Serve())
}
