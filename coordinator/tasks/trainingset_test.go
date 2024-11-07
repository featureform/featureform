// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package tasks

import (
	"context"
	"testing"

	"github.com/featureform/coordinator/spawner"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	"github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/scheduling"
	"go.uber.org/zap/zaptest"
)

func TestTrainingSetTaskRun(t *testing.T) {
	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())

	serv, addr := startServ(t)
	defer serv.Stop()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		panic(err)
	}

	preReqTaskRuns := createPreqTrainingSetResources(t, client)

	for _, run := range preReqTaskRuns {
		err = client.Tasks.SetRunStatus(run.TaskId, run.ID, scheduling.RUNNING, nil)
		if err != nil {
			t.Fatalf(err.Error())
		}
		err = client.Tasks.SetRunStatus(run.TaskId, run.ID, scheduling.READY, nil)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}

	err = client.CreateTrainingSetVariant(context.Background(), metadata.TrainingSetDef{
		Name:     "trainingSetName",
		Variant:  "trainingSetVariant",
		Owner:    "mockOwner",
		Provider: "mockProvider",
		Label:    metadata.NameVariant{Name: "labelName", Variant: "labelVariant"},
		Features: metadata.NameVariants{
			{Name: "featureName", Variant: "featureVariant"},
		},
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 4 {
		t.Fatalf("Expected 4 run to be created, got: %d", len(runs))
	}

	var trainingSetTaskRun scheduling.TaskRunMetadata
	runDiff := difference(runs, preReqTaskRuns)
	if len(runDiff) != 1 {
		t.Fatalf("Expected 1 run to be different, got: %d", len(runDiff))
	}

	trainingSetTaskRun = runDiff[0]

	task := TrainingSetTask{
		BaseTask{
			metadata: client,
			taskDef:  trainingSetTaskRun,
			spawner:  &spawner.MemoryJobSpawner{},
			logger:   zaptest.NewLogger(t).Sugar(),
		},
	}
	err = task.Run()
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func createPreqTrainingSetResources(t *testing.T, client *metadata.Client) []scheduling.TaskRunMetadata {
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

	p, err := provider.Get(pt.MemoryOffline, provider_config.SerializedConfig{})
	if err != nil {
		t.Fatalf(err.Error())
	}

	store, err := p.AsOfflineStore()
	if err != nil {
		t.Fatalf(err.Error())
	}

	_, err = store.RegisterResourceFromSourceTable(provider.ResourceID{Name: "labelName", Variant: "labelVariant", Type: provider.Label}, provider.ResourceSchema{})
	if err != nil {
		return nil
	}

	_, err = store.RegisterResourceFromSourceTable(provider.ResourceID{Name: "featureName", Variant: "featureVariant", Type: provider.Feature}, provider.ResourceSchema{})
	if err != nil {
		return nil
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

	err = client.CreateLabelVariant(context.Background(), metadata.LabelDef{
		Name:     "labelName",
		Variant:  "labelVariant",
		Owner:    "mockOwner",
		Provider: "mockProvider",
		Source:   metadata.NameVariant{Name: "sourceName", Variant: "sourceVariant"},
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

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 3 {
		t.Fatalf("Expected 3 run to be created, got: %d", len(runs))
	}

	return runs
}

func difference(a, b []scheduling.TaskRunMetadata) []scheduling.TaskRunMetadata {
	var diff []scheduling.TaskRunMetadata

	for _, itemA := range a {
		found := false
		for _, itemB := range b {
			if itemA.ID.String() == itemB.ID.String() {
				found = true
				break
			}
		}
		if !found {
			diff = append(diff, itemA)
		}
	}

	return diff
}
