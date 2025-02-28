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
	"testing"
	"time"

	"github.com/featureform/coordinator/spawner"
	ct "github.com/featureform/coordinator/types"
	"github.com/featureform/ffsync"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/scheduling"
)

func newScheduler(ctx context.Context, t *testing.T, logger logging.Logger) (*Scheduler, *metadata.MetadataServer, *metadata.Client) {
	serv, addr := startServ(ctx, t)
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		panic(err)
	}
	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}

	return &Scheduler{
		ID:       ct.SchedulerID("test_scheduler"),
		Metadata: client,
		Logger:   logger,
		Executor: &Executor{
			metadata: client,
			logger:   logger,
			locker: &metadata.TaskLocker{
				Locker: &locker,
			},
			spawner: &spawner.MemoryJobSpawner{},
			config:  ExecutorConfig{DependencyPollInterval: 1 * time.Second},
		},
		Config: SchedulerConfig{
			TaskStatusSyncInterval:   1 * time.Hour,
			TaskPollInterval:         1 * time.Second,
			TaskDistributionInterval: 1,
		},
	}, serv, client
}

func newSchedulers(ctx context.Context, t *testing.T, logger logging.Logger) ([]*Scheduler, *metadata.MetadataServer, *metadata.Client) {
	serv, addr := startServ(ctx, t)
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		t.Fatalf(err.Error())
	}
	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}

	return []*Scheduler{
		{
			ID:       ct.SchedulerID("test_scheduler_a"),
			Metadata: client,
			Logger:   logger,
			Executor: &Executor{
				metadata: client,
				logger:   logger,
				locker: &metadata.TaskLocker{
					Locker: &locker,
				},
				spawner: &spawner.MemoryJobSpawner{},
				config:  ExecutorConfig{DependencyPollInterval: 1 * time.Second},
			},
			Config: SchedulerConfig{
				TaskStatusSyncInterval:   1 * time.Hour,
				TaskPollInterval:         1 * time.Second,
				TaskDistributionInterval: 2,
			},
		},
		{
			ID:       ct.SchedulerID("test_scheduler_b"),
			Metadata: client,
			Logger:   logger,
			Executor: &Executor{
				metadata: client,
				logger:   logger,
				locker: &metadata.TaskLocker{
					Locker: &locker,
				},
				spawner: &spawner.MemoryJobSpawner{},
				config:  ExecutorConfig{DependencyPollInterval: 1 * time.Second},
			},
			Config: SchedulerConfig{
				TaskStatusSyncInterval:   1 * time.Hour,
				TaskPollInterval:         1 * time.Second,
				TaskDistributionInterval: 2,
			},
		},
	}, serv, client
}

func TestCreatePrimary(t *testing.T) {
	ctx, logger := logging.NewTestContextAndLogger(t)
	scheduler, _, client := newScheduler(ctx, t, logger)
	defer scheduler.Stop()

	go func() {
		err := scheduler.Start(ctx)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
	}()
	err := client.CreateUser(ctx, metadata.UserDef{
		Name: "mockOwner",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = client.CreateProvider(ctx, metadata.ProviderDef{
		Name: "mockProvider",
		Type: pt.MemoryOffline.String(),
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = client.CreateSourceVariant(ctx, metadata.SourceDef{
		Name:    "sourceName",
		Variant: "sourceVariant",
		Definition: metadata.PrimaryDataSource{
			Location: metadata.SQLTable{
				Name: "mockPrimary",
			},
		},
		Owner:    "mockOwner",
		Provider: "mockProvider",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	ready := false
	for i := 0; i < 200; i++ {
		source, err := client.GetSourceVariant(ctx, metadata.NameVariant{Name: "sourceName", Variant: "sourceVariant"})
		if err != nil {
			t.Fatalf(err.Error())
		}
		time.Sleep(1 * time.Second)
		if source.Status() == scheduling.READY {
			ready = true
			break
		}
	}
	if !ready {
		t.Fatalf("source not ready")
	}
	scheduler.Stop()
}

func TestCreateFeature(t *testing.T) {
	ctx, logger := logging.NewTestContextAndLogger(t)
	scheduler, _, client := newScheduler(ctx, t, logger)
	defer scheduler.Stop()

	go func() {
		err := scheduler.Start(ctx)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
	}()
	err := client.CreateUser(ctx, metadata.UserDef{
		Name: "mockOwner",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = client.CreateProvider(ctx, metadata.ProviderDef{
		Name: "mockProvider",
		Type: pt.MemoryOffline.String(),
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = client.CreateSourceVariant(ctx, metadata.SourceDef{
		Name:    "sourceName",
		Variant: "sourceVariant",
		Definition: metadata.PrimaryDataSource{
			Location: metadata.SQLTable{
				Name: "mockPrimary",
			},
		},
		Owner:    "mockOwner",
		Provider: "mockProvider",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = client.CreateEntity(ctx, metadata.EntityDef{
		Name: "mockEntity",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = client.CreateFeatureVariant(ctx, metadata.FeatureDef{
		Name:    "featureName",
		Variant: "featureVariant",
		Owner:   "mockOwner",
		Source:  metadata.NameVariant{Name: "sourceName", Variant: "sourceVariant"},
		Location: metadata.ResourceVariantColumns{
			Entity: "col1",
			Value:  "col2",
			Source: "mockTable",
		},
		Entity: "mockEntity",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	ready := false
	for i := 0; i < 10; i++ {
		source, err := client.GetFeatureVariant(ctx, metadata.NameVariant{Name: "featureName", Variant: "featureVariant"})
		if err != nil {
			t.Fatalf(err.Error())
		}
		time.Sleep(1 * time.Second)
		if source.Status() == scheduling.READY {
			ready = true
			break
		}
	}
	if !ready {
		t.Fatalf("feature not ready")
	}
}

func TestTaskDistributionAcrossCoordinators(t *testing.T) {
	ctx, logger := logging.NewTestContextAndLogger(t)
	schedulers, _, client := newSchedulers(ctx, t, logger)
	schedA := schedulers[0]
	schedB := schedulers[1]
	defer schedA.Stop()
	defer schedB.Stop()

	if err := client.CreateAll(ctx, getResourceDefs()); err != nil {
		t.Fatalf("Failed to create resources: %v", err)
	}

	go func() {
		err := schedA.Start(ctx)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
	}()
	go func() {
		err := schedB.Start(ctx)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
	}()

	features, err := client.ListFeatures(ctx)
	if err != nil {
		t.Fatalf("Failed to list features: %v", err)
	}
	if len(features) != 1 {
		t.Fatalf("Expected 1 feature, got %d", len(features))
	}
	feature := features[0]
	allReady := make([]bool, len(feature.Variants()))
	for i := 0; i < 10; i++ {
		variants, err := feature.FetchVariants(client, ctx)
		if err != nil {
			t.Fatalf("Failed to fetch feature variants: %v", err)
		}
		time.Sleep(1 * time.Second)
		for i, variant := range variants {
			if variant.Status() == scheduling.READY {
				allReady[i] = true
			}
		}
	}
	for i, ready := range allReady {
		if !ready {
			t.Fatalf("Feature variant %d not ready", i)
		}
	}

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf("Failed to get task runs: %v", err)
	}
	schedulerTasksMap := map[ct.SchedulerID]int{
		schedA.ID: 0,
		schedB.ID: 0,
	}
	for _, run := range runs {
		schedulerTasksMap[ct.SchedulerID(run.SchedulerID)]++
	}
	if schedulerTasksMap[schedA.ID] == 0 || schedulerTasksMap[schedB.ID] == 0 {
		t.Fatalf("Tasks not distributed across schedulers: %v", schedulerTasksMap)
	}
}

func TestStochasticTaskRunFiltering(t *testing.T) {
	cases := []struct {
		name     string
		interval int
	}{
		{name: "Interval 1", interval: 1},
		{name: "Interval 2", interval: 2},
		{name: "Interval 3", interval: 3},
		{name: "Interval 5", interval: 5},
		{name: "Interval 10", interval: 10},
	}

	taskRuns := make(scheduling.TaskRunList, 0)
	for i := 0; i < 1000; i++ {
		taskRuns = append(taskRuns, scheduling.TaskRunMetadata{
			ID:     scheduling.NewIntTaskID(uint64(i) + 1),
			Status: scheduling.PENDING,
		})
	}

	const iterations = 100

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rf := stochasticTaskRunFilter{Rand: rand.New(rand.NewSource(42)), interval: tc.interval}
			totalFiltered := 0

			for i := 0; i < iterations; i++ {
				filtered := rf.filterRuns(taskRuns)
				totalFiltered += len(filtered)
			}

			avgFiltered := float64(totalFiltered) / float64(iterations)
			expectedFiltered := float64(len(taskRuns)) / float64(tc.interval)

			// Allowing a 10% margin of error
			lowerBound := expectedFiltered * 0.9
			upperBound := expectedFiltered * 1.1

			if avgFiltered < lowerBound || avgFiltered > upperBound {
				t.Errorf("For interval %d, expected filtered tasks in range (%.2f - %.2f), but got %.2f",
					tc.interval, lowerBound, upperBound, avgFiltered)
			} else {
				t.Logf("For interval %d, average filtered tasks: %.2f (expected %.2f)",
					tc.interval, avgFiltered, expectedFiltered)
			}
		})
	}
}

func getResourceDefs() []metadata.ResourceDef {
	return []metadata.ResourceDef{
		metadata.UserDef{Name: "mock_owner"},
		metadata.ProviderDef{Name: "mock_provider", Type: pt.MemoryOffline.String()},
		metadata.SourceDef{
			Name:    "source_name",
			Variant: "source_variant",
			Definition: metadata.PrimaryDataSource{
				Location: metadata.SQLTable{
					Name: "mock_primary",
				},
			},
			Owner:    "mock_owner",
			Provider: "mock_provider",
		},
		metadata.EntityDef{Name: "mock_entity"},
		metadata.FeatureDef{
			Name:     "feature_name",
			Variant:  "feature_variant_a",
			Owner:    "mock_owner",
			Source:   metadata.NameVariant{Name: "source_name", Variant: "source_variant"},
			Location: metadata.ResourceVariantColumns{Entity: "col1", Value: "col2", Source: "mock_table"},
			Entity:   "mock_entity",
		},
		metadata.FeatureDef{
			Name:     "feature_name",
			Variant:  "feature_variant_b",
			Owner:    "mock_owner",
			Source:   metadata.NameVariant{Name: "source_name", Variant: "source_variant"},
			Location: metadata.ResourceVariantColumns{Entity: "col1", Value: "col2", Source: "mock_table"},
			Entity:   "mock_entity",
		},
		metadata.FeatureDef{
			Name:     "feature_name",
			Variant:  "feature_variant_c",
			Owner:    "mock_owner",
			Source:   metadata.NameVariant{Name: "source_name", Variant: "source_variant"},
			Location: metadata.ResourceVariantColumns{Entity: "col1", Value: "col2", Source: "mock_table"},
			Entity:   "mock_entity",
		},
		metadata.FeatureDef{
			Name:     "feature_name",
			Variant:  "feature_variant_d",
			Owner:    "mock_owner",
			Source:   metadata.NameVariant{Name: "source_name", Variant: "source_variant"},
			Location: metadata.ResourceVariantColumns{Entity: "col1", Value: "col2", Source: "mock_table"},
			Entity:   "mock_entity",
		},
		metadata.FeatureDef{
			Name:     "feature_name",
			Variant:  "feature_variant_e",
			Owner:    "mock_owner",
			Source:   metadata.NameVariant{Name: "source_name", Variant: "source_variant"},
			Location: metadata.ResourceVariantColumns{Entity: "col1", Value: "col2", Source: "mock_table"},
			Entity:   "mock_entity",
		},
		metadata.FeatureDef{
			Name:     "feature_name",
			Variant:  "feature_variant_f",
			Owner:    "mock_owner",
			Source:   metadata.NameVariant{Name: "source_name", Variant: "source_variant"},
			Location: metadata.ResourceVariantColumns{Entity: "col1", Value: "col2", Source: "mock_table"},
			Entity:   "mock_entity",
		},
	}
}
