// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"fmt"
	"testing"

	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pc "github.com/featureform/provider/provider_config"
	"github.com/featureform/types"
	"go.uber.org/zap/zaptest"
)

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
	delete(factoryMap, string(COPY_TO_ONLINE))
	if err := RegisterFactory(string(COPY_TO_ONLINE), mockChunkRunnerFactory); err != nil {
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
	delete(factoryMap, string(COPY_TO_ONLINE))

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

func TestMaterializeRunnerConfigUnmarshalVectorType(t *testing.T) {
	config := []byte(`{
		"OnlineType": "REDIS_ONLINE",
		"OfflineType": "SPARK_OFFLINE",
		"OnlineConfig": "eyJBZGRyIjogInJlZGlzZWFyY2g6NjM3OSIsICJQYXNzd29yZCI6ICIiLCAiREIiOiAwfQ==",
		"OfflineConfig": "eyJFeGVjdXRvclR5cGUiOiAiREFUQUJSSUNLUyIsICJTdG9yZVR5cGUiOiAiQVpVUkUiLCAiRXhlY3V0b3JDb25maWciOiB7IlVzZXJuYW1lIjogIiIsICJQYXNzd29yZCI6ICIiLCAiSG9zdCI6ICJodHRwczovL2FkYi00MTc0OTc2Mzk1NzA5MDc4LjE4LmF6dXJlZGF0YWJyaWNrcy5uZXQiLCAiVG9rZW4iOiAiZGFwaTczODljYzU3NTgwOTk3YmM1YWJiYTc1N2YwNzJlMGFmLTMiLCAiQ2x1c3RlciI6ICIxMTAyLTIxNDYxNy1jbng1OHltaiJ9LCAiU3RvcmVDb25maWciOiB7IkFjY291bnROYW1lIjogImZmc2FudGFuZGVyZGVtbyIsICJBY2NvdW50S2V5IjogIkFQZ3RJZjJpT2lqdFd0blJEbUo0VzNPNStnRDdNaDNRVEd1cXBzU0tlL09zZ2V0b2xKOUxjVnlWVTNNNVZkRk4wSmNydFpUVXRhWVUrQVN0aFdSazh3PT0iLCAiQ29udGFpbmVyTmFtZSI6ICJxdW90ZXMiLCAiUGF0aCI6ICJxdW90ZXMifX0=",
		"ResourceID": {
			"Name": "pb_quote_embeddings",
			"Variant": "vector32poc_65",
			"Type": 2
		},
		"VType": {
			"ValueType": {
				"ScalarType": "float32",
				"Dimension": 384
			}
		},
		"Cloud": "LOCAL",
		"IsUpdate": false,
		"IsEmbedding": true
	}`)
	runnerConfig := &MaterializedRunnerConfig{}
	if err := runnerConfig.Deserialize(config); err != nil {
		t.Fatalf("failed to deserialize materialize runner config: %v", err)
	}
	vectorType, isVectorType := runnerConfig.VType.ValueType.(provider.VectorType)
	if !isVectorType {
		t.Fatalf("expected VectorType, got %v", runnerConfig.VType.ValueType)
	}
	if vectorType.ScalarType != "float32" {
		t.Fatalf("expected float32, got %v", vectorType.ScalarType)
	}
	if vectorType.Dimension != 384 {
		t.Fatalf("expected 384, got %v", vectorType.Dimension)
	}
}

func TestMaterializeRunnerConfigUnmarshalScalarType(t *testing.T) {
	config := []byte(`{
		"OnlineType": "REDIS_ONLINE",
		"OfflineType": "SPARK_OFFLINE",
		"OnlineConfig": "eyJBZGRyIjogInJlZGlzZWFyY2g6NjM3OSIsICJQYXNzd29yZCI6ICIiLCAiREIiOiAwfQ==",
		"OfflineConfig": "eyJFeGVjdXRvclR5cGUiOiAiREFUQUJSSUNLUyIsICJTdG9yZVR5cGUiOiAiQVpVUkUiLCAiRXhlY3V0b3JDb25maWciOiB7IlVzZXJuYW1lIjogIiIsICJQYXNzd29yZCI6ICIiLCAiSG9zdCI6ICJodHRwczovL2FkYi00MTc0OTc2Mzk1NzA5MDc4LjE4LmF6dXJlZGF0YWJyaWNrcy5uZXQiLCAiVG9rZW4iOiAiZGFwaTczODljYzU3NTgwOTk3YmM1YWJiYTc1N2YwNzJlMGFmLTMiLCAiQ2x1c3RlciI6ICIxMTAyLTIxNDYxNy1jbng1OHltaiJ9LCAiU3RvcmVDb25maWciOiB7IkFjY291bnROYW1lIjogImZmc2FudGFuZGVyZGVtbyIsICJBY2NvdW50S2V5IjogIkFQZ3RJZjJpT2lqdFd0blJEbUo0VzNPNStnRDdNaDNRVEd1cXBzU0tlL09zZ2V0b2xKOUxjVnlWVTNNNVZkRk4wSmNydFpUVXRhWVUrQVN0aFdSazh3PT0iLCAiQ29udGFpbmVyTmFtZSI6ICJxdW90ZXMiLCAiUGF0aCI6ICJxdW90ZXMifX0=",
		"ResourceID": {
			"Name": "pb_quote_embeddings",
			"Variant": "vector32poc_65",
			"Type": 2
		},
		"VType": {
			"ValueType": "float32"
		},
		"Cloud": "LOCAL",
		"IsUpdate": false,
		"IsEmbedding": true
	}`)
	runnerConfig := &MaterializedRunnerConfig{}
	if err := runnerConfig.Deserialize(config); err != nil {
		t.Fatalf("failed to deserialize materialize runner config: %v", err)
	}
	if _, isScalarType := runnerConfig.VType.ValueType.(provider.ScalarType); !isScalarType {
		t.Fatalf("expected ScalarType, got %v", runnerConfig.VType.ValueType)
	}
	scalarType, isScalarType := runnerConfig.VType.ValueType.(provider.ScalarType)
	if !isScalarType {
		t.Fatalf("expected ScalarType, got %v", scalarType)
	}
	if scalarType != provider.Float32 {
		t.Fatalf("expected Float32, got %v", scalarType)
	}
}

// TODO: determine how best to mock connection to ensure this test runs locally,
// as well as in CI/CD
func TestMaterializeRunnerVectorStoreTypeAssertion(t *testing.T) {
	// c := &pc.RedisConfig{
	// 	Addr:   "localhost:6379",
	// 	Prefix: "Featureform_table__",
	// }
	// fmt.Println(base64.StdEncoding.EncodeToString(c.Serialized()))
	config := []byte(`{
		"OnlineType": "REDIS_ONLINE",
		"OfflineType": "SPARK_OFFLINE",
		"OnlineConfig": "eyJQcmVmaXgiOiJGZWF0dXJlZm9ybV90YWJsZV9fIiwiQWRkciI6ImxvY2FsaG9zdDo2Mzc5IiwiUGFzc3dvcmQiOiIiLCJEQiI6MH0=",
		"OfflineConfig": "eyJFeGVjdXRvclR5cGUiOiAiREFUQUJSSUNLUyIsICJTdG9yZVR5cGUiOiAiQVpVUkUiLCAiRXhlY3V0b3JDb25maWciOiB7IlVzZXJuYW1lIjogIiIsICJQYXNzd29yZCI6ICIiLCAiSG9zdCI6ICJodHRwczovL2FkYi00MTc0OTc2Mzk1NzA5MDc4LjE4LmF6dXJlZGF0YWJyaWNrcy5uZXQiLCAiVG9rZW4iOiAiZGFwaTczODljYzU3NTgwOTk3YmM1YWJiYTc1N2YwNzJlMGFmLTMiLCAiQ2x1c3RlciI6ICIxMTAyLTIxNDYxNy1jbng1OHltaiJ9LCAiU3RvcmVDb25maWciOiB7IkFjY291bnROYW1lIjogImZmc2FudGFuZGVyZGVtbyIsICJBY2NvdW50S2V5IjogIkFQZ3RJZjJpT2lqdFd0blJEbUo0VzNPNStnRDdNaDNRVEd1cXBzU0tlL09zZ2V0b2xKOUxjVnlWVTNNNVZkRk4wSmNydFpUVXRhWVUrQVN0aFdSazh3PT0iLCAiQ29udGFpbmVyTmFtZSI6ICJxdW90ZXMiLCAiUGF0aCI6ICJxdW90ZXMifX0=",
		"ResourceID": {
			"Name": "pb_quote_embeddings",
			"Variant": "vector32poc_54",
			"Type": 2
		},
		"VType": {
			"ValueType": {
				"ScalarType": "float32",
				"Dimension": 384
			}
		},
		"Cloud": "LOCAL",
		"IsUpdate": false,
		"IsEmbedding": true
	}`)

	runner, err := MaterializeRunnerFactory(config)
	if err != nil {
		t.Fatalf("failed to create materialize runner: %v", err)
	}
	materializeRunner, ok := runner.(*MaterializeRunner)
	if !ok {
		t.Fatalf("expected materialize runner, got %v", runner)
	}

	if materializeRunner.IsEmbedding {
		vectorType, ok := materializeRunner.VType.(provider.VectorType)
		fmt.Println(vectorType)
		if !ok {
			t.Fatalf("expected vector type, got %v", materializeRunner.VType)
		}
		if vectorType.Dimension != 384 {
			t.Fatalf("expected vector type dimension 384, got %v", vectorType.Dimension)
		}
	}
}

// TODO: use miniredis to mock redis; this currently only work by running
// > kubectl port-forward redisearch-<ID> 6379:6379
func TestOnlineStoreCastToVectorStore(t *testing.T) {
	redisOnlineStore, err := provider.NewRedisOnlineStore(&pc.RedisConfig{
		Addr:   "localhost:6379",
		Prefix: "Featureform_table__",
	})
	if err != nil {
		t.Fatalf("failed to create redis online store: %v", err)
	}

	runner := &MaterializeRunner{
		Online:      redisOnlineStore,
		IsEmbedding: true,
	}

	if runner.IsEmbedding {
		vectorStore, ok := runner.Online.(provider.VectorStore)
		if !ok {
			t.Fatalf("expected vector store, got %v", runner.Online)
		}
		if vectorStore == nil {
			t.Fatalf("expected vector store, got nil")
		}
	}
}
