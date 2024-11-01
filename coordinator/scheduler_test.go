// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/featureform/coordinator/spawner"
	"github.com/featureform/ffsync"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/scheduling"
	"go.uber.org/zap/zaptest"
)

func newScheduler(t *testing.T) (*Scheduler, *metadata.MetadataServer, *metadata.Client) {
	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())
	serv, addr := startServ(t)
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		panic(err)
	}
	locker, err := ffsync.NewMemoryLocker()
	if err != nil {
		t.Fatalf(err.Error())
	}

	return &Scheduler{
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
			TaskStatusSyncInterval: 1 * time.Hour,
			TaskPollInterval:       1 * time.Second,
		},
	}, serv, client
}

func TestCreatePrimary(t *testing.T) {
	scheduler, _, client := newScheduler(t)
	defer scheduler.Stop()

	go func() {
		err := scheduler.Start()
		if err != nil {
			t.Errorf(err.Error())
			return
		}
	}()
	err := client.CreateUser(context.Background(), metadata.UserDef{
		Name: "mockOwner",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = client.CreateProvider(context.Background(), metadata.ProviderDef{
		Name: "mockProvider",
		Type: pt.MemoryOffline.String(),
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = client.CreateSourceVariant(context.Background(), metadata.SourceDef{
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
		source, err := client.GetSourceVariant(context.Background(), metadata.NameVariant{Name: "sourceName", Variant: "sourceVariant"})
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
	scheduler, _, client := newScheduler(t)
	defer scheduler.Stop()

	go func() {
		err := scheduler.Start()
		if err != nil {
			t.Errorf(err.Error())
			return
		}
	}()
	err := client.CreateUser(context.Background(), metadata.UserDef{
		Name: "mockOwner",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = client.CreateProvider(context.Background(), metadata.ProviderDef{
		Name: "mockProvider",
		Type: pt.MemoryOffline.String(),
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = client.CreateSourceVariant(context.Background(), metadata.SourceDef{
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
	err = client.CreateEntity(context.Background(), metadata.EntityDef{
		Name: "mockEntity",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = client.CreateFeatureVariant(context.Background(), metadata.FeatureDef{
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
		source, err := client.GetFeatureVariant(context.Background(), metadata.NameVariant{Name: "featureName", Variant: "featureVariant"})
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
