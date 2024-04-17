// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"testing"

	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	"github.com/featureform/types"
	"github.com/google/uuid"
	"go.uber.org/zap/zaptest"
)

func TestMaterializationRunner(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dynamodb := provider.GetTestingDynamoDB(t)
	dbrix := provider.GetTestingS3Databricks(t)
	t.Run("dbrix_to_dynamo", func(t *testing.T) {
		testMaterializationRunner(t, dbrix, dynamodb)
	})
}

func testMaterializationRunner(t *testing.T, offline provider.OfflineStore, online provider.OnlineStore) {
	logger := zaptest.NewLogger(t).Sugar()

	schema := provider.TableSchema{
		Columns: []provider.TableColumn{
			{Name: "entity", ValueType: provider.String},
			{Name: "value", ValueType: provider.Float32},
			{Name: "ts", ValueType: provider.Timestamp},
		},
	}
	records := []provider.ResourceRecord{
		{Entity: "a", Value: float32(1)},
		{Entity: "b", Value: float32(2)},
		{Entity: "c", Value: float32(3)},
	}
	id, mat := createMaterialization(t, offline, schema, records)
	defer offline.DeleteMaterialization(mat.ID())
	job := MaterializeRunner{
		Online:  online,
		Offline: offline,
		ID:      id,
		VType:   provider.Float32,
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
) (provider.ResourceID, provider.Materialization) {
	id := provider.ResourceID{Name: uuid.NewString(), Variant: uuid.NewString(), Type: provider.Feature}
	table, err := store.CreateResourceTable(id, schema)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if err := table.WriteBatch(records); err != nil {
		t.Fatalf("Failed to write batch: %s", err)
	}
	mat, err := store.CreateMaterialization(id)
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
		VType:  provider.String,
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
