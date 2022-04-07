package runner

import (
	"fmt"
	provider "github.com/featureform/serving/provider"
	"testing"
)

type MockOfflineCreateTrainingSetFail struct {
	provider.BaseProvider
}

func (m MockOfflineCreateTrainingSetFail) CreateResourceTable(id provider.ResourceID) (provider.OfflineTable, error) {
	return nil, nil
}
func (m MockOfflineCreateTrainingSetFail) GetResourceTable(id provider.ResourceID) (provider.OfflineTable, error) {
	return nil, nil
}
func (m MockOfflineCreateTrainingSetFail) CreateMaterialization(id provider.ResourceID) (provider.Materialization, error) {
	return nil, nil
}
func (m MockOfflineCreateTrainingSetFail) GetMaterialization(id provider.MaterializationID) (provider.Materialization, error) {
	return nil, nil
}
func (m MockOfflineCreateTrainingSetFail) DeleteMaterialization(id provider.MaterializationID) error {
	return nil
}
func (m MockOfflineCreateTrainingSetFail) CreateTrainingSet(provider.TrainingSetDef) error {
	return fmt.Errorf("could not create training set")
}
func (m MockOfflineCreateTrainingSetFail) GetTrainingSet(id provider.ResourceID) (provider.TrainingSetIterator, error) {
	return nil, nil
}

func TestRun(t *testing.T) {
	runner := TrainingSetRunner{
		MockOfflineStore{},
		provider.TrainingSetDef{},
	}
	watcher, err := runner.Run()
	if err != nil {
		t.Fatalf("failed to create create training set runner: %v", err)
	}
	if err := watcher.Wait(); err != nil {
		t.Fatalf("training set runer failed: %v", err)
	}
}

func TestFail(t *testing.T) {
	runner := TrainingSetRunner{
		MockOfflineCreateTrainingSetFail{},
		provider.TrainingSetDef{},
	}
	watcher, err := runner.Run()
	if err != nil {
		t.Fatalf("failed to create create training set runner: %v", err)
	}
	if err := watcher.Wait(); err == nil {
		t.Fatalf("failed to report error creating training set")
	}
}
