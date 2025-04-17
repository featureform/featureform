// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package runner

import (
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap/zaptest"

	"github.com/featureform/filestore"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	"github.com/featureform/provider/dataset"
	pl "github.com/featureform/provider/location"
	pt "github.com/featureform/provider/provider_type"
	vt "github.com/featureform/provider/types"
	"github.com/featureform/types"
)

func TestMaterializationRunner(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// A lot of tests mess with this global state. We should stop doing that, but for now we can try to
	// force the factory to exist by calling this directly.
	ResetFactoryMap()
	registerFactories()
	dynamodb := provider.GetTestingDynamoDB(t, map[string]string{})
	dbrix := provider.GetTestingS3Databricks(t)
	t.Run("dbrix_to_dynamo", func(t *testing.T) {
		testMaterializationRunner(t, dbrix, dynamodb)
	})
}

func testMaterializationRunner(t *testing.T, offline provider.OfflineStore, online provider.OnlineStore) {
	logger := zaptest.NewLogger(t).Sugar()

	schema := provider.TableSchema{
		Columns: []provider.TableColumn{
			{Name: "entity", ValueType: vt.String},
			{Name: "value", ValueType: vt.Float32},
			{Name: "ts", ValueType: vt.Timestamp},
		},
	}
	records := make([]provider.ResourceRecord, 99)
	for i := 0; i < 99; i++ {
		entity, val := strconv.Itoa(i), float32(i)
		records[i] = provider.ResourceRecord{Entity: entity, Value: val}
	}
	id, mat := createMaterialization(t, offline, schema, records)
	defer offline.DeleteMaterialization(provider.MaterializationID(mat.ID()))
	job := MaterializeRunner{
		Online:  online,
		Offline: offline,
		ID:      id,
		VType:   vt.Float32,
		// Only testing initial materializations
		IsUpdate: false,
		// Not testing K8s
		Cloud:  LocalMaterializeRunner,
		Logger: logger,
	}
	waiter, err := job.MaterializeToOnline(mat)
	if err != nil {
		t.Fatalf("Run failed: %s", err)
	}
	if err := waiter.Wait(); err != nil {
		panic(err)
	}
	defer online.DeleteTable(id.Name, id.Variant)
	for _, rec := range records {
		entity, expVal := rec.Entity, rec.Value
		tab, err := online.GetTable(id.Name, id.Variant)
		if err != nil {
			t.Fatalf("Failed to get table %v.\n%s\n", id, err)
		}
		gotVal, err := tab.Get(entity)
		if err != nil {
			t.Fatalf("Entity not found %s.", entity)
		}
		casted, ok := gotVal.(float32)
		if !ok {
			t.Fatalf(
				"Got wrong type for %s.\nExpected: %+v\nFound: %+v\n",
				entity, expVal, gotVal,
			)
		}
		if casted != gotVal {
			t.Fatalf(
				"Got wrong value for %s.\nExpected: %+v\nFound: %+v\n",
				entity, expVal, gotVal,
			)
		}
	}
}

func createMaterialization(
	t *testing.T, store provider.OfflineStore, schema provider.TableSchema, records []provider.ResourceRecord,
) (provider.ResourceID, dataset.Materialization) {
	id := provider.ResourceID{Name: uuid.NewString(), Variant: uuid.NewString(), Type: provider.Feature}
	table, err := store.CreateResourceTable(id, schema)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if err := table.WriteBatch(records); err != nil {
		t.Fatalf("Failed to write batch: %s", err)
	}
	mat, err := store.CreateMaterialization(id, provider.MaterializationOptions{ShouldIncludeHeaders: true, MaxJobDuration: time.Duration(10) * time.Minute})
	if err != nil {
		t.Fatalf("Failed to create materialization: %s", err)
	}
	return id, mat
}

type mockChunkRunner struct{}

func (m mockChunkRunner) Run() (types.CompletionWatcher, error) {
	return mockCompletionWatcher{}, nil
}

func (m mockChunkRunner) Resource() metadata.ResourceID {
	return metadata.ResourceID{}
}

func (m mockChunkRunner) IsUpdateJob() bool {
	return false
}

type mockCompletionWatcher struct{}

func (m mockCompletionWatcher) Wait() error {
	return nil
}

func (m mockCompletionWatcher) Err() error {
	return nil
}

func (m mockCompletionWatcher) String() string {
	return ""
}

func (m mockCompletionWatcher) Complete() bool {
	return true
}

func mockChunkRunnerFactory(config Config) (types.Runner, error) {
	return &mockChunkRunner{}, nil
}

func TestMockMaterializeRunner(t *testing.T) {
	materializeRunner := MaterializeRunner{
		Online:  MockOnlineStore{},
		Offline: MockOfflineStore{},
		ID: provider.ResourceID{
			Name:    "test",
			Variant: "test",
			Type:    provider.Feature,
		},
		VType:  vt.String,
		Cloud:  LocalMaterializeRunner,
		Logger: zaptest.NewLogger(t).Sugar(),
	}
	delete(factoryMap, COPY_TO_ONLINE)
	if err := RegisterFactory(COPY_TO_ONLINE, mockChunkRunnerFactory); err != nil {
		t.Fatalf("Failed to register factory: %v", err)
	}

	watcher, err := materializeRunner.Run()
	if err != nil {
		t.Fatalf("Failed to create materialize runner: %v", err)
	}
	if err := watcher.Wait(); err != nil {
		t.Fatalf("Failed to run materialize runner: %v", err)
	}
	if err := watcher.Err(); err != nil {
		t.Fatalf("Failed to run materialize runner: %v", err)
	}
	if complete := watcher.Complete(); !complete {
		t.Fatalf("Runner failed to complete")
	}
	if result := watcher.String(); len(result) == 0 {
		t.Fatalf("Failed to return string on completion status")
	}
	delete(factoryMap, COPY_TO_ONLINE)

}

func TestWatcherMultiplex(t *testing.T) {
	watcherList := make([]types.CompletionWatcher, 1)
	watcherList[0] = &mockCompletionWatcher{}
	multiplex := WatcherMultiplex{watcherList}
	if err := multiplex.Wait(); err != nil {
		t.Fatalf("Multiplex failed: %v", err)
	}
	if err := multiplex.Err(); err != nil {
		t.Fatalf("Multiplex failed: %v", err)
	}
	if complete := multiplex.Complete(); !complete {
		t.Fatalf("Multiplex failed to complete")
	}
	if result := multiplex.String(); len(result) == 0 {
		t.Fatalf("Failed to return multiplexer string")
	}
}

// TODO: (Erik) Improve on this test
func TestMaterializedRunnerConfigSerde(t *testing.T) {
	tests := []struct {
		name      string
		config    MaterializedRunnerConfig
		expectErr bool
	}{
		{
			name: "Valid config",
			config: MaterializedRunnerConfig{
				OnlineType:    pt.RedisOnline,
				OfflineType:   pt.SnowflakeOffline,
				OnlineConfig:  []byte("{}"),
				OfflineConfig: []byte("{}"),
				ResourceID:    provider.ResourceID{Name: "name", Variant: "variant", Type: provider.Feature},
				VType:         vt.ValueTypeJSONWrapper{ValueType: vt.UInt64},
				Cloud:         LocalMaterializeRunner,
				Options: provider.MaterializationOptions{
					Output:               filestore.Parquet,
					ShouldIncludeHeaders: true,
					MaxJobDuration:       time.Duration(10) * time.Minute,
					JobName:              "job",
					ResourceSnowflakeConfig: &metadata.ResourceSnowflakeConfig{
						DynamicTableConfig: &metadata.SnowflakeDynamicTableConfig{
							ExternalVolume: "volume",
							BaseLocation:   "location",
							TargetLag:      "1 hours",
							RefreshMode:    metadata.AutoRefresh,
							Initialize:     metadata.InitializeOnCreate,
						},
					},
					Schema: provider.ResourceSchema{
						Entity:      "entity",
						Value:       "value",
						TS:          "ts",
						SourceTable: pl.NewSQLLocation("table"),
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := test.config.Serialize()
			if err != nil {
				t.Fatalf("Failed to serialize config: %v", err)
			}
			config := MaterializedRunnerConfig{}
			if err := config.Deserialize(data); (err != nil) != test.expectErr {
				t.Fatalf("Failed to deserialize config: %v", err)
			}
		})
	}
}
