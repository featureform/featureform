// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package main

import (
	"fmt"
	"time"

	"github.com/featureform/coordinator"

	"github.com/featureform/scheduling"

	"github.com/featureform/coordinator/spawner"
	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	etcdHost := help.GetEnv("ETCD_HOST", "localhost")
	etcdPort := help.GetEnv("ETCD_PORT", "2379")
	etcdUrl := fmt.Sprintf("%s:%s", etcdHost, etcdPort)
	metadataHost := help.GetEnv("METADATA_HOST", "localhost")
	metadataPort := help.GetEnv("METADATA_PORT", "8080")
	metadataUrl := fmt.Sprintf("%s:%s", metadataHost, metadataPort)
	useK8sRunner := help.GetEnv("K8S_RUNNER_ENABLE", "false")
	managerType := help.GetEnv("FF_STATE_PROVIDER", "ETCD")
	fmt.Printf("connecting to etcd: %s\n", etcdUrl)
	fmt.Printf("connecting to metadata: %s\n", metadataUrl)
	etcdConfig := clientv3.Config{
		Endpoints:   []string{etcdUrl},
		Username:    help.GetEnv("ETCD_USERNAME", "root"),
		Password:    help.GetEnv("ETCD_PASSWORD", "secretpassword"),
		DialTimeout: time.Second * 1,
	}

	cli, err := clientv3.New(etcdConfig)
	if err != nil {
		fmt.Println("Failed to connect to ETCD. Continuing...")
	}

	defer func(cli *clientv3.Client) {
		err := cli.Close()
		if err != nil {
			panic(fmt.Errorf("failed to close etcd client: %w", err))
		}
	}(cli)
	logger := logging.NewLogger("coordinator")
	defer logger.Sync()
	client, err := metadata.NewClient(metadataUrl, logger)
	if err != nil {
		logger.Errorw("Failed to connect: %v", err)
		panic(err)
	}
	logger.Debug("Connected to Metadata")
	var spawnerInstance spawner.JobSpawner
	if useK8sRunner == "false" {
		spawnerInstance = &spawner.MemoryJobSpawner{}
	} else {
		spawnerInstance = &spawner.KubernetesJobSpawner{EtcdConfig: etcdConfig}
	}

	manager, err := scheduling.NewTaskMetadataManagerFromEnv(scheduling.TaskMetadataManagerType(managerType))
	if err != nil {
		panic(err.Error())
	}

	config := coordinator.SchedulerConfig{
		TaskPollInterval: func() time.Duration {
			interval, err := time.ParseDuration(help.GetEnv("TASK_POLL_INTERVAL", "1s"))
			if err != nil {
				panic(err.Error())
			}
			return interval
		}(),
		TaskStatusSyncInterval: func() time.Duration {
			interval, err := time.ParseDuration(help.GetEnv("TASK_STATUS_SYNC_INTERVAL", "1h"))
			if err != nil {
				panic(err.Error())
			}
			return interval
		}(),
		DependencyPollInterval: func() time.Duration {
			interval, err := time.ParseDuration(help.GetEnv("TASK_DEPENDENCY_POLL_INTERVAL", "1s"))
			if err != nil {
				panic(err.Error())
			}
			return interval
		}(),
	}

	scheduler := coordinator.NewScheduler(client, logger, spawnerInstance, manager.Storage.Locker, config)
	err = scheduler.Start()
	if err != nil {
		panic(err.Error())
	}
}
