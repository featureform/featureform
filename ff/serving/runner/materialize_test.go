package runner

import (
	provider "github.com/featureform/serving/provider"
	"testing"
)

type mockChunkRunner struct{}

func (m mockChunkRunner) Run() (CompletionWatcher, error) {
	return mockCompletionWatcher{}, nil
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

func mockChunkRunnerFactory(config Config) (Runner, error) {
	return &mockChunkRunner{}, nil
}

func MockMaterialize(t *testing.T) {
	materializeRunner := MaterializeRunner{
		Online:  MockOnlineStore{},
		Offline: MockOfflineStore{},
		ID: provider.ResourceID{
			Name:    "test",
			Variant: "test",
			Type:    provider.Feature,
		},
		Cloud: Local,
	}
	if err := RegisterFactory("COPY", mockChunkRunnerFactory); err != nil {
		t.Fatalf("Failed to register factory: %v", err)
	}

	watcher, err := materializeRunner.Run()
	if err != nil {
		t.Fatalf("Failed to create materialize runner: %v", err)
	}
	if err := watcher.Wait(); err != nil {
		t.Fatalf("Failed to run materialize runner: %v", err)
	}
}
