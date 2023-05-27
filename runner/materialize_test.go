// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"testing"

	"github.com/featureform/metadata"
	"github.com/featureform/provider"
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

// write a unit test to cover serializing and deserializing the config
func TestMaterializeRunnerConfigVectorTypeUnMarshall(t *testing.T) {
	// config := map[string]interface{}{"OnlineType": "REDIS_ONLINE", "OfflineType": "SPARK_OFFLINE", "OnlineConfig": "eyJBZGRyIjogInJlZGlzZWFyY2g6NjM4MCIsICJQYXNzd29yZCI6ICIiLCAiREIiOiAwfQ==", "OfflineConfig": "eyJFeGVjdXRvclR5cGUiOiAiREFUQUJSSUNLUyIsICJTdG9yZVR5cGUiOiAiQVpVUkUiLCAiRXhlY3V0b3JDb25maWciOiB7IlVzZXJuYW1lIjogIiIsICJQYXNzd29yZCI6ICIiLCAiSG9zdCI6ICJodHRwczovL2FkYi00MTc0OTc2Mzk1NzA5MDc4LjE4LmF6dXJlZGF0YWJyaWNrcy5uZXQiLCAiVG9rZW4iOiAiZGFwaTczODljYzU3NTgwOTk3YmM1YWJiYTc1N2YwNzJlMGFmLTMiLCAiQ2x1c3RlciI6ICIxMTAyLTIxNDYxNy1jbng1OHltaiJ9LCAiU3RvcmVDb25maWciOiB7IkFjY291bnROYW1lIjogImZmc2FudGFuZGVyZGVtbyIsICJBY2NvdW50S2V5IjogIkFQZ3RJZjJpT2lqdFd0blJEbUo0VzNPNStnRDdNaDNRVEd1cXBzU0tlL09zZ2V0b2xKOUxjVnlWVTNNNVZkRk4wSmNydFpUVXRhWVUrQVN0aFdSazh3PT0iLCAiQ29udGFpbmVyTmFtZSI6ICJxdW90ZXMiLCAiUGF0aCI6ICJxdW90ZXMifX0=", "ResourceID": map[string]interface{}{"Name": "pb_quote_embeddings", "Variant": "vector32poc_54", "Type": 2}, "VType": map[string]interface{}{"ScalarType": "float32", "Dimension": 384}, "Cloud": "LOCAL", "IsUpdate": false, "IsEmbedding": true}
	// config := []byte(`{"OnlineType":"REDIS_ONLINE","OfflineType":"SPARK_OFFLINE","OnlineConfig":"eyJBZGRyIjogInJlZGlzZWFyY2g6NjM4MCIsICJQYXNzd29yZCI6ICIiLCAiREIiOiAwfQ==","OfflineConfig":"eyJFeGVjdXRvclR5cGUiOiAiREFUQUJSSUNLUyIsICJTdG9yZVR5cGUiOiAiQVpVUkUiLCAiRXhlY3V0b3JDb25maWciOiB7IlVzZXJuYW1lIjogIiIsICJQYXNzd29yZCI6ICIiLCAiSG9zdCI6ICJodHRwczovL2FkYi00MTc0OTc2Mzk1NzA5MDc4LjE4LmF6dXJlZGF0YWJyaWNrcy5uZXQiLCAiVG9rZW4iOiAiZGFwaTczODljYzU3NTgwOTk3YmM1YWJiYTc1N2YwNzJlMGFmLTMiLCAiQ2x1c3RlciI6ICIxMTAyLTIxNDYxNy1jbng1OHltaiJ9LCAiU3RvcmVDb25maWciOiB7IkFjY291bnROYW1lIjogImZmc2FudGFuZGVyZGVtbyIsICJBY2NvdW50S2V5IjogIkFQZ3RJZjJpT2lqdFd0blJEbUo0VzNPNStnRDdNaDNRVEd1cXBzU0tlL09zZ2V0b2xKOUxjVnlWVTNNNVZkRk4wSmNydFpUVXRhWVUrQVN0aFdSazh3PT0iLCAiQ29udGFpbmVyTmFtZSI6ICJxdW90ZXMiLCAiUGF0aCI6ICJxdW90ZXMifX0=","ResourceID":{"Name":"pb_quote_embeddings","Variant":"vector32poc_54","Type":2},"VType":{"ScalarType":"float32","Dimension":384},"Cloud":"LOCAL","IsUpdate":false,"IsEmbedding":true}`)
	config := []byte(`{
		"OnlineType": "REDIS_ONLINE",
		"OfflineType": "SPARK_OFFLINE",
		"OnlineConfig": "eyJBZGRyIjogInJlZGlzZWFyY2g6NjM4MCIsICJQYXNzd29yZCI6ICIiLCAiREIiOiAwfQ==",
		"OfflineConfig": "eyJFeGVjdXRvclR5cGUiOiAiREFUQUJSSUNLUyIsICJTdG9yZVR5cGUiOiAiQVpVUkUiLCAiRXhlY3V0b3JDb25maWciOiB7IlVzZXJuYW1lIjogIiIsICJQYXNzd29yZCI6ICIiLCAiSG9zdCI6ICJodHRwczovL2FkYi00MTc0OTc2Mzk1NzA5MDc4LjE4LmF6dXJlZGF0YWJyaWNrcy5uZXQiLCAiVG9rZW4iOiAiZGFwaTczODljYzU3NTgwOTk3YmM1YWJiYTc1N2YwNzJlMGFmLTMiLCAiQ2x1c3RlciI6ICIxMTAyLTIxNDYxNy1jbng1OHltaiJ9LCAiU3RvcmVDb25maWciOiB7IkFjY291bnROYW1lIjogImZmc2FudGFuZGVyZGVtbyIsICJBY2NvdW50S2V5IjogIkFQZ3RJZjJpT2lqdFd0blJEbUo0VzNPNStnRDdNaDNRVEd1cXBzU0tlL09zZ2V0b2xKOUxjVnlWVTNNNVZkRk4wSmNydFpUVXRhWVUrQVN0aFdSazh3PT0iLCAiQ29udGFpbmVyTmFtZSI6ICJxdW90ZXMiLCAiUGF0aCI6ICJxdW90ZXMifX0=",
		"ResourceID": {
			"Name": "pb_quote_embeddings",
			"Variant": "vector32poc_54",
			"Type": 2
		},
		"VType": {
			"ScalarType": "float32",
			"Dimension": 384
		},
		"Cloud": "LOCAL",
		"IsUpdate": false,
		"IsEmbedding": true
	}`)
	runnerConfig := &MaterializedRunnerConfig{}
	if err := runnerConfig.Deserialize(config); err != nil {
		t.Fatalf("failed to deserialize materialize runner config: %v", err)
	}
	if _, isVectorType := runnerConfig.VType.ValueType.(provider.VectorType); !isVectorType {
		t.Fatalf("expected VectorType, got %v", runnerConfig.VType.ValueType)
	}
}

func TestMaterializeRunnerConfigScalarTypeUnMarshall(t *testing.T) {
	// config := map[string]interface{}{"OnlineType": "REDIS_ONLINE", "OfflineType": "SPARK_OFFLINE", "OnlineConfig": "eyJBZGRyIjogInJlZGlzZWFyY2g6NjM4MCIsICJQYXNzd29yZCI6ICIiLCAiREIiOiAwfQ==", "OfflineConfig": "eyJFeGVjdXRvclR5cGUiOiAiREFUQUJSSUNLUyIsICJTdG9yZVR5cGUiOiAiQVpVUkUiLCAiRXhlY3V0b3JDb25maWciOiB7IlVzZXJuYW1lIjogIiIsICJQYXNzd29yZCI6ICIiLCAiSG9zdCI6ICJodHRwczovL2FkYi00MTc0OTc2Mzk1NzA5MDc4LjE4LmF6dXJlZGF0YWJyaWNrcy5uZXQiLCAiVG9rZW4iOiAiZGFwaTczODljYzU3NTgwOTk3YmM1YWJiYTc1N2YwNzJlMGFmLTMiLCAiQ2x1c3RlciI6ICIxMTAyLTIxNDYxNy1jbng1OHltaiJ9LCAiU3RvcmVDb25maWciOiB7IkFjY291bnROYW1lIjogImZmc2FudGFuZGVyZGVtbyIsICJBY2NvdW50S2V5IjogIkFQZ3RJZjJpT2lqdFd0blJEbUo0VzNPNStnRDdNaDNRVEd1cXBzU0tlL09zZ2V0b2xKOUxjVnlWVTNNNVZkRk4wSmNydFpUVXRhWVUrQVN0aFdSazh3PT0iLCAiQ29udGFpbmVyTmFtZSI6ICJxdW90ZXMiLCAiUGF0aCI6ICJxdW90ZXMifX0=", "ResourceID": map[string]interface{}{"Name": "pb_quote_embeddings", "Variant": "vector32poc_54", "Type": 2}, "VType": map[string]interface{}{"ScalarType": "float32", "Dimension": 384}, "Cloud": "LOCAL", "IsUpdate": false, "IsEmbedding": true}
	// config := []byte(`{"OnlineType":"REDIS_ONLINE","OfflineType":"SPARK_OFFLINE","OnlineConfig":"eyJBZGRyIjogInJlZGlzZWFyY2g6NjM4MCIsICJQYXNzd29yZCI6ICIiLCAiREIiOiAwfQ==","OfflineConfig":"eyJFeGVjdXRvclR5cGUiOiAiREFUQUJSSUNLUyIsICJTdG9yZVR5cGUiOiAiQVpVUkUiLCAiRXhlY3V0b3JDb25maWciOiB7IlVzZXJuYW1lIjogIiIsICJQYXNzd29yZCI6ICIiLCAiSG9zdCI6ICJodHRwczovL2FkYi00MTc0OTc2Mzk1NzA5MDc4LjE4LmF6dXJlZGF0YWJyaWNrcy5uZXQiLCAiVG9rZW4iOiAiZGFwaTczODljYzU3NTgwOTk3YmM1YWJiYTc1N2YwNzJlMGFmLTMiLCAiQ2x1c3RlciI6ICIxMTAyLTIxNDYxNy1jbng1OHltaiJ9LCAiU3RvcmVDb25maWciOiB7IkFjY291bnROYW1lIjogImZmc2FudGFuZGVyZGVtbyIsICJBY2NvdW50S2V5IjogIkFQZ3RJZjJpT2lqdFd0blJEbUo0VzNPNStnRDdNaDNRVEd1cXBzU0tlL09zZ2V0b2xKOUxjVnlWVTNNNVZkRk4wSmNydFpUVXRhWVUrQVN0aFdSazh3PT0iLCAiQ29udGFpbmVyTmFtZSI6ICJxdW90ZXMiLCAiUGF0aCI6ICJxdW90ZXMifX0=","ResourceID":{"Name":"pb_quote_embeddings","Variant":"vector32poc_54","Type":2},"VType":{"ScalarType":"float32","Dimension":384},"Cloud":"LOCAL","IsUpdate":false,"IsEmbedding":true}`)
	config := []byte(`{
		"OnlineType": "REDIS_ONLINE",
		"OfflineType": "SPARK_OFFLINE",
		"OnlineConfig": "eyJBZGRyIjogInJlZGlzZWFyY2g6NjM4MCIsICJQYXNzd29yZCI6ICIiLCAiREIiOiAwfQ==",
		"OfflineConfig": "eyJFeGVjdXRvclR5cGUiOiAiREFUQUJSSUNLUyIsICJTdG9yZVR5cGUiOiAiQVpVUkUiLCAiRXhlY3V0b3JDb25maWciOiB7IlVzZXJuYW1lIjogIiIsICJQYXNzd29yZCI6ICIiLCAiSG9zdCI6ICJodHRwczovL2FkYi00MTc0OTc2Mzk1NzA5MDc4LjE4LmF6dXJlZGF0YWJyaWNrcy5uZXQiLCAiVG9rZW4iOiAiZGFwaTczODljYzU3NTgwOTk3YmM1YWJiYTc1N2YwNzJlMGFmLTMiLCAiQ2x1c3RlciI6ICIxMTAyLTIxNDYxNy1jbng1OHltaiJ9LCAiU3RvcmVDb25maWciOiB7IkFjY291bnROYW1lIjogImZmc2FudGFuZGVyZGVtbyIsICJBY2NvdW50S2V5IjogIkFQZ3RJZjJpT2lqdFd0blJEbUo0VzNPNStnRDdNaDNRVEd1cXBzU0tlL09zZ2V0b2xKOUxjVnlWVTNNNVZkRk4wSmNydFpUVXRhWVUrQVN0aFdSazh3PT0iLCAiQ29udGFpbmVyTmFtZSI6ICJxdW90ZXMiLCAiUGF0aCI6ICJxdW90ZXMifX0=",
		"ResourceID": {
			"Name": "pb_quote_embeddings",
			"Variant": "vector32poc_54",
			"Type": 2
		},
		"VType": "int32",
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
}
