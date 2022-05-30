// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	metadata "github.com/featureform/serving/metadata"
	provider "github.com/featureform/serving/provider"
	"testing"
)

type mockChunkRunner struct{}

func (m mockChunkRunner) Run() (CompletionWatcher, error) {
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

func mockChunkRunnerFactory(config Config) (Runner, error) {
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
		VType: provider.String,
		Cloud: LocalMaterializeRunner,
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
}

func TestWatcherMultiplex(t *testing.T) {
	watcherList := make([]CompletionWatcher, 1)
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
