// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package coordinator

import (
	"context"
	"math/rand"
	"strconv"
	"time"

	"github.com/featureform/coordinator/spawner"
	ct "github.com/featureform/coordinator/types"
	"github.com/featureform/ffsync"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/scheduling"
)

func NewScheduler(ctx context.Context, id ct.SchedulerID, client *metadata.Client, spawner spawner.JobSpawner, locker ffsync.Locker, config SchedulerConfig) *Scheduler {
	logger := logging.GetLoggerFromContext(ctx)
	return &Scheduler{
		ID:       id,
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
	TaskPollInterval         time.Duration
	TaskStatusSyncInterval   time.Duration
	DependencyPollInterval   time.Duration
	TaskDistributionInterval int
}

type Scheduler struct {
	ID           ct.SchedulerID
	Metadata     *metadata.Client
	Logger       logging.Logger
	Executor     *Executor
	Config       SchedulerConfig
	stop         bool
	lastSyncTime time.Time
}

func (c *Scheduler) Start(ctx context.Context) error {
	c.Logger.Info("Watching for new jobs")
	stochasticFilter := NewStochasticTaskRunFilter(c.Config.TaskDistributionInterval)
	runIteration := 0
	for !c.stop {
		runIteration++
		ctx = context.WithValue(ctx, ct.RunIterationContextKey("runIteration"), strconv.Itoa(runIteration))
		if c.shouldSyncTaskStatus() {
			err := c.Metadata.Tasks.SyncUnfinishedRuns()
			if err != nil {
				c.Logger.Error(err.Error())
			}
		}

		runs, err := c.Metadata.Tasks.GetUnfinishedRuns()
		c.Logger.Debugf("Fetched all unfinished runs: %v", runs)
		if err != nil {
			c.Logger.Error(err.Error())
		}

		for _, run := range stochasticFilter.filterRuns(runs) {
			go func(run scheduling.TaskRunMetadata) {
				err = c.Executor.RunTask(ctx, c.ID, run.TaskId, run.ID)
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

func NewStochasticTaskRunFilter(interval int) *stochasticTaskRunFilter {
	return &stochasticTaskRunFilter{
		Rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		interval: interval,
	}
}

type stochasticTaskRunFilter struct {
	Rand     *rand.Rand
	interval int
}

func (rf *stochasticTaskRunFilter) filterRuns(unfiltered scheduling.TaskRunList) scheduling.TaskRunList {
	filtered := make(scheduling.TaskRunList, 0, len(unfiltered)/rf.interval+1)
	for _, run := range unfiltered {
		if rf.Rand.Intn(rf.interval) == 0 {
			filtered = append(filtered, run)
		}
	}
	return filtered
}
