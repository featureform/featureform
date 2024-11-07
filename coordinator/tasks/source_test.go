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
	pt "github.com/featureform/provider/provider_type"
	"go.uber.org/zap/zaptest"
)

func TestSourceTaskRun(t *testing.T) {
	logger := logging.WrapZapLogger(zaptest.NewLogger(t).Sugar())

	serv, addr := startServ(t)
	defer serv.Stop()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		panic(err)
	}

	createSourcePreqResources(t, client)

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

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 1 {
		t.Fatalf("Expected 1 run to be created, got: %d", len(runs))
	}

	task := SourceTask{
		BaseTask: BaseTask{
			metadata: client,
			taskDef:  runs[0],
			spawner:  &spawner.MemoryJobSpawner{},
			logger:   zaptest.NewLogger(t).Sugar(),
		},
	}
	err = task.Run()
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func createSourcePreqResources(t *testing.T, client *metadata.Client) {
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

	err = client.CreateEntity(context.Background(), metadata.EntityDef{
		Name: "mockEntity",
	})
	if err != nil {
		t.Fatalf(err.Error())
	}

	runs, err := client.Tasks.GetAllRuns()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if len(runs) != 0 {
		t.Fatalf("Expected 1 run to be created, got: %d", len(runs))
	}
}

func TestSqlVariableReplace(t *testing.T) {
	testCases := []struct {
		name          string
		originalQuery string
		expectedQuery string
	}{
		{
			name:          "no variables",
			originalQuery: "SELECT * FROM table",
			expectedQuery: "SELECT * FROM table",
		},
		{
			name:          "one FF_LAST_RUN_TIMESTAMP variable",
			originalQuery: "SELECT * FROM table WHERE column > FF_LAST_RUN_TIMESTAMP",
			expectedQuery: "SELECT * FROM table WHERE column > TO_TIMESTAMP(0)",
		},
		{
			name:          "multiple FF_LAST_RUN_TIMESTAMP variable",
			originalQuery: "SELECT * FROM table WHERE column > FF_LAST_RUN_TIMESTAMP AND column2 < FF_LAST_RUN_TIMESTAMP",
			expectedQuery: "SELECT * FROM table WHERE column > TO_TIMESTAMP(0) AND column2 < TO_TIMESTAMP(0)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			replacedQuery := sqlVariableReplace(tc.originalQuery, nil)
			if replacedQuery != tc.expectedQuery {
				t.Fatalf("Expected: %s, got: %s", tc.expectedQuery, replacedQuery)
			}
		})
	}
}
