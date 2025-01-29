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
	"time"

	"github.com/featureform/config"
	"github.com/featureform/config/bootstrap"
	"github.com/featureform/coordinator"
	"github.com/featureform/coordinator/spawner"
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

	config := coordinator.SchedulerConfig{
		TaskPollInterval: func() time.Duration {
			interval, err := time.ParseDuration(help.GetEnv("TASK_POLL_INTERVAL", "1s"))
			if err != nil {
				logger.Errorw("Invalid TASK_POLL_INTERVAL")
				panic(err.Error())
			}
			return interval
		}(),
		TaskStatusSyncInterval: func() time.Duration {
			interval, err := time.ParseDuration(help.GetEnv("TASK_STATUS_SYNC_INTERVAL", "1h"))
			if err != nil {
				logger.Errorw("Invalid TASK_STATUS_SYNC_INTERVAL")
				panic(err.Error())
			}
			return interval
		}(),
		DependencyPollInterval: func() time.Duration {
			interval, err := time.ParseDuration(help.GetEnv("TASK_DEPENDENCY_POLL_INTERVAL", "1s"))
			if err != nil {
				logger.Errorw("Invalid TASK_DEPENDENCY_POLL_INTERVAL")
				panic(err.Error())
			}
			return interval
		}(),
	}

	logger.Info("Dependencies created. Starting Scheduler...")
	scheduler := coordinator.NewScheduler(client, logger, spawnerInstance, manager.Storage.Locker, config)

	err = scheduler.Start()
	if err != nil {
		panic(err.Error())
	}
}
