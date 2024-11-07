// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package coordinator

import (
	"time"

	"github.com/featureform/coordinator/spawner"
	"github.com/featureform/ffsync"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/scheduling"
)

func NewScheduler(client *metadata.Client, logger logging.Logger, spawner spawner.JobSpawner, locker ffsync.Locker, config SchedulerConfig) *Scheduler {
	return &Scheduler{
		Metadata: client,
		Logger:   logger,
		Executor: &Executor{
			metadata: client,
			logger:   logger,
			locker: &metadata.TaskLocker{
				Locker: locker,
			},
			spawner: spawner,
			config:  ExecutorConfig{DependencyPollInterval: config.DependencyPollInterval},
		},
		Config: config,
	}
}

type SchedulerConfig struct {
	TaskPollInterval       time.Duration
	TaskStatusSyncInterval time.Duration
	DependencyPollInterval time.Duration
}

type Scheduler struct {
	Metadata     *metadata.Client
	Logger       logging.Logger
	Executor     *Executor
	Config       SchedulerConfig
	stop         bool
	lastSyncTime time.Time
}

func (c *Scheduler) Start() error {
	c.Logger.Info("Watching for new jobs")
	for !c.stop {
		if c.shouldSyncTaskStatus() {
			err := c.Metadata.Tasks.SyncUnfinishedRuns()
			if err != nil {
				c.Logger.Error(err.Error())
			}
		}

		runs, err := c.Metadata.Tasks.GetUnfinishedRuns()
		if err != nil {
			c.Logger.Error(err.Error())
		}

		for _, run := range runs {
			go func(run scheduling.TaskRunMetadata) {
				err = c.Executor.RunTask(run.TaskId, run.ID)
				if err != nil {
					c.Logger.Error(err.Error())
				}
			}(run)
		}
		time.Sleep(c.Config.TaskPollInterval)
	}
	return nil
}

func (c *Scheduler) shouldSyncTaskStatus() bool {
	if time.Since(c.lastSyncTime) > c.Config.TaskStatusSyncInterval {
		c.lastSyncTime = time.Now()
		return true
	}
	return false
}

func (c *Scheduler) Stop() {
	c.stop = true
}
