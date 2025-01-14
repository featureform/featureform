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
	"github.com/featureform/scheduling"
)

func TestFeatureTaskRun(t *testing.T) {
	logger := logging.NewTestLogger(t)

	serv, addr := startServ(t)
	defer serv.Stop()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		panic(err)
	}

	sourceTaskRun := createPreqResources(t, client)
	t.Log("Source Run:", sourceTaskRun)

	err = client.Tasks.SetRunStatus(sourceTaskRun.TaskId, sourceTaskRun.ID, scheduling.RUNNING, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.Tasks.SetRunStatus(sourceTaskRun.TaskId, sourceTaskRun.ID, scheduling.READY, nil)
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

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 2 {
		t.Fatalf("Expected 2 run to be created, got: %d", len(runs))
	}

	var featureTaskRun scheduling.TaskRunMetadata
	for _, run := range runs {
		if sourceTaskRun.ID.String() != run.ID.String() {
			featureTaskRun = run
		}
	}

	task := FeatureTask{
		BaseTask{
			metadata: client,
			taskDef:  featureTaskRun,
			spawner:  &spawner.MemoryJobSpawner{},
			logger:   logging.NewTestLogger(t),
		},
	}
	err = task.Run()
	if err != nil {
		t.Fatalf(err.Error())
	}
}
