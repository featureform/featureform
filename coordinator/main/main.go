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
	"os"
	"time"

	"github.com/featureform/config"
	"github.com/featureform/config/bootstrap"
	"github.com/featureform/coordinator"
	"github.com/featureform/health"

	"github.com/google/uuid"

	"github.com/featureform/coordinator/spawner"
	ct "github.com/featureform/coordinator/types"
	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
)

func main() {
	metadataHost := help.GetEnv("METADATA_HOST", "localhost")
	metadataPort := help.GetEnv("METADATA_PORT", "8080")
	metadataUrl := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	useK8sRunner := help.GetEnv("K8S_RUNNER_ENABLE", "false")
	logger := logging.NewLogger("coordinator")
	defer logger.Sync()
	logger.Info("Parsing Featureform App Config")
	appConfig, err := config.Get(logger)
	if err != nil {
		logger.Errorw("Invalid App Config", "err", err)
		panic(err)
	}
	initTimeout := appConfig.InitTimeout
	ctx, cancelFn := context.WithTimeout(context.Background(), initTimeout)
	defer cancelFn()
	initCtx := logger.AttachToContext(ctx)
	logger.Info("Created initialization context with timeout", "timeout", initTimeout)
	logger.Debug("Creating initializer")
	init, err := bootstrap.NewInitializer(appConfig)
	if err != nil {
		logger.Errorw("Failed to bootstrap service from config", "err", err)
		panic(err)
	}
	defer logger.LogIfErr("Failed to close service-level resources", init.Close())

	logger.Infof("connecting to metadata: %s\n", metadataUrl)
	client, err := metadata.NewClient(metadataUrl, logger)
	if err != nil {
		logger.Errorw("Failed to connect to metadata: %v", err)
		panic(err)
	}

	logger.Debug("Getting job spawner")
	var spawnerInstance spawner.JobSpawner
	if useK8sRunner == "false" {
		logger.Debug("Using go-routine job spawner")
		spawnerInstance = &spawner.MemoryJobSpawner{}
	} else {
		logger.Errorw("K8s job spawner no longer supported")
		panic("K8s Job Spawner no longer supported")
	}

	logger.Debug("Getting task metadata manager")
	manager, err := init.GetOrCreateTaskMetadataManager(initCtx)
	if err != nil {
		panic(err.Error())
	}

	apiStatusPort := help.GetEnv("API_STATUS_PORT", "8443")
	logger.Infow("Retrieved API status port from ENV", "port", apiStatusPort)
	if err = health.StartHttpServer(logger, apiStatusPort); err != nil {
		logger.Errorw("Failed to start health check", "err", err)
		panic(err)
	}

	config := coordinator.SchedulerConfig{
		TaskPollInterval: func() time.Duration {
			return appConfig.SchedulerTaskPollInterval
		}(),
		TaskStatusSyncInterval: func() time.Duration {
			return appConfig.SchedulerTaskStatusSyncInterval
		}(),
		DependencyPollInterval: func() time.Duration {
			return appConfig.SchedulerDependencyPollInterval
		}(),
		TaskDistributionInterval: appConfig.SchedulerTaskDistributionInterval,
	}

	logger.Info("Dependencies created. Starting Scheduler...")
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "featureform-coordinator-" + uuid.New().String()[0:8]
		logger.Warnw("Failed to get hostname; using random hostname", "err", err, "hostname", hostname)
	}
	scheduler := coordinator.NewScheduler(initCtx, ct.SchedulerID(hostname), client, spawnerInstance, manager.Storage.Locker, config)

	err = scheduler.Start(context.Background())
	if err != nil {
		panic(err.Error())
	}
}
