// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package tasks

import (
	"context"
	"net"
	"testing"

	"github.com/featureform/coordinator/spawner"
	"github.com/featureform/logging"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
	"github.com/featureform/scheduling"
)

func startServ(t *testing.T, ctx context.Context, logger logging.Logger) (*metadata.MetadataServer, string) {
	manager, err := scheduling.NewMemoryTaskMetadataManager(ctx)
	if err != nil {
		panic(err.Error())
	}
	config := &metadata.Config{
		TaskManager: manager,
		Logger:      logger,
	}
	serv, err := metadata.NewMetadataServer(config)
	if err != nil {
		panic(err)
	}
	// listen on a random port
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	go func() {
		if err := serv.ServeOnListener(lis); err != nil {
			panic(err)
		}
	}()
	return serv, lis.Addr().String()
}

func TestLabelTaskRun(t *testing.T) {
	ctx, logger := logging.NewTestContextAndLogger(t)
	serv, addr := startServ(t, ctx, logger)
	defer serv.Stop()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		panic(err)
	}

	sourceTaskRun := createPreqResources(t, ctx, client)
	t.Log("Source Run:", sourceTaskRun)

	err = client.Tasks.SetRunStatus(sourceTaskRun.TaskId, sourceTaskRun.ID, scheduling.RUNNING, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.Tasks.SetRunStatus(sourceTaskRun.TaskId, sourceTaskRun.ID, scheduling.READY, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.CreateLabelVariant(ctx, metadata.LabelDef{
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

	if len(runs) != 2 {
		t.Fatalf("Expected 2 run to be created, got: %d", len(runs))
	}

	var labelTaskRun scheduling.TaskRunMetadata
	for _, run := range runs {
		if sourceTaskRun.ID.String() != run.ID.String() {
			labelTaskRun = run
		}
	}

	task := LabelTask{
		BaseTask{
			metadata: client,
			taskDef:  labelTaskRun,
			spawner:  &spawner.MemoryJobSpawner{},
			logger:   logger,
		},
	}
	err = task.Run()
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func createPreqResources(t *testing.T, ctx context.Context, client *metadata.Client) scheduling.TaskRunMetadata {
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

	source, err := client.GetSourceVariant(ctx, metadata.NameVariant{Name: "sourceName", Variant: "sourceVariant"})
	if err != nil {
		t.Fatalf(err.Error())
	}

	sourceProvider, err := source.FetchProvider(client, ctx)
	if err != nil {
		t.Fatalf(err.Error())
	}

	p, err := provider.Get(pt.Type(sourceProvider.Type()), sourceProvider.SerializedConfig())
	if err != nil {
		t.Fatalf(err.Error())
	}

	store, err := p.AsOfflineStore()
	if err != nil {
		t.Fatalf(err.Error())
	}

	schema := provider.TableSchema{
		Columns: []provider.TableColumn{
			{Name: "col1", ValueType: types.String},
			{Name: "col2", ValueType: types.String},
		},
		SourceTable: "mockTable",
	}

	// Added this because we dont actually run the primary table registration before this test
	tableID := provider.ResourceID{Name: "sourceName", Variant: "sourceVariant", Type: provider.Primary}
	_, err = store.CreatePrimaryTable(tableID, schema)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = client.CreateEntity(ctx, metadata.EntityDef{
		Name: "mockEntity",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 1 {
		t.Fatalf("Expected 1 run to be created, got: %d", len(runs))
	}

	return runs[0]
}
