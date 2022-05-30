// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"fmt"
	provider "github.com/featureform/serving/provider"
	"testing"
)

type MockOfflineCreateTransformationFail struct {
	provider.BaseProvider
}

func (m MockOfflineCreateTransformationFail) CreateResourceTable(provider.ResourceID, provider.TableSchema) (provider.OfflineTable, error) {
	return nil, nil
}
func (m MockOfflineCreateTransformationFail) GetResourceTable(id provider.ResourceID) (provider.OfflineTable, error) {
	return nil, nil
}
func (m MockOfflineCreateTransformationFail) CreateMaterialization(id provider.ResourceID) (provider.Materialization, error) {
	return nil, nil
}
func (m MockOfflineCreateTransformationFail) GetMaterialization(id provider.MaterializationID) (provider.Materialization, error) {
	return nil, nil
}
func (m MockOfflineCreateTransformationFail) DeleteMaterialization(id provider.MaterializationID) error {
	return nil
}
func (m MockOfflineCreateTransformationFail) CreateTrainingSet(provider.TrainingSetDef) error {
	return fmt.Errorf("could not create training set")
}
func (m MockOfflineCreateTransformationFail) GetTrainingSet(id provider.ResourceID) (provider.TrainingSetIterator, error) {
	return nil, nil
}

func (m MockOfflineCreateTransformationFail) CreatePrimaryTable(id provider.ResourceID, schema provider.TableSchema) (provider.PrimaryTable, error) {
	return nil, nil
}
func (m MockOfflineCreateTransformationFail) GetPrimaryTable(id provider.ResourceID) (provider.PrimaryTable, error) {
	return nil, nil
}

func (m MockOfflineCreateTransformationFail) RegisterResourceFromSourceTable(id provider.ResourceID, schema provider.ResourceSchema) (provider.OfflineTable, error) {
	return nil, nil
}

func (m MockOfflineCreateTransformationFail) RegisterPrimaryFromSourceTable(id provider.ResourceID, sourceName string) (provider.PrimaryTable, error) {
	return nil, nil
}

func (m MockOfflineCreateTransformationFail) CreateTransformation(config provider.TransformationConfig) error {
	return nil
}

func (m MockOfflineCreateTransformationFail) GetTransformationTable(id provider.ResourceID) (provider.TransformationTable, error) {
	return nil, nil
}

func TestRun(t *testing.T) {
	runner := CreateTransformationRunner{
		MockOfflineStore{},
		provider.TransformationConfig{},
		false,
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
	runner := CreateTransformationRunnerr{
		MockOfflineCreateTransformationFail{},
		provider.TransformationConfig{},
		false,
	}
	watcher, err := runner.Run()
	if err != nil {
		t.Fatalf("failed to create create training set runner: %v", err)
	}
	if err := watcher.Wait(); err == nil {
		t.Fatalf("failed to report error creating training set")
	}
}

func testTransformationErrorConfigsFactory(config Config) error {
	_, err := Create(CREATE_TRANSFORMATION, config)
	return err
}

type ErrorTransformationFactoryConfigs struct {
	Name        string
	ErrorConfig Config
}

func TestCreateTransformationRunnerFactoryErrorCoverage(t *testing.T) {
	transformationSerialize := func(ts CreateTransformationRunnerConfig) Config {
		config, err := ts.Serialize()
		if err != nil {
			t.Fatalf("error serializing transformation runner config: %v", err)
		}
		return config
	}
	errorConfigs := []ErrorTransformationFactoryConfigs{
		{
			Name:        "cannot deserialize config",
			ErrorConfig: []byte{},
		},
		{
			Name: "cannot configure offline provider",
			ErrorConfig: transformationSerialize(CreateTransformationRunnerConfig{
				OfflineType: "Invalid_Offline_type",
			}),
		},
		{
			Name: "cannot convert offline provider to offline store",
			ErrorConfig: transformationSerialize(CreateTransformationRunnerConfig{
				OfflineType:   provider.LocalOnline,
				OfflineConfig: []byte{},
			}),
		},
	}
	err := RegisterFactory(CREATE_TRANSFORMATION, CreateTransformationRunnerFactory)
	if err != nil {
		t.Fatalf("Could not register transformation factory: %v", err)
	}
	for _, config := range errorConfigs {
		if err := testTransformationErrorConfigsFactory(config.ErrorConfig); err == nil {
			t.Fatalf("Test Job Failed to catch error: %s", config.Name)
		}
	}
	delete(factoryMap, CREATE_TRAINING_SET)
}

func TestTransformationFactory(t *testing.T) {
	transformationSerialize := func(ts CreateTransformationRunnerConfig) Config {
		config, err := ts.Serialize()
		if err != nil {
			t.Fatalf("error serializing transformation runner config: %v", err)
		}
		return config
	}
	serializedConfig := createTransformationSerialize(TransformationRunnerConfig{
		OfflineType:   "MOCK_OFFLINE",
		OfflineConfig: []byte{},
		Def: provider.TransformationConfig{
			TargetTableId: provider.ResourceID{},
			Query: "",
			ColumnMapping: []ColumnMapping{},
		}
	})
	err := RegisterFactory(CREATE_TRANSFORMATION, CreateTransformationRunnerFactory)
	if err != nil {
		t.Fatalf("Could not register transformation factory: %v", err)
	}
	_, err = Create(CREATE_TRANSFORMATION, serializedConfig)
	if err != nil {
		t.Fatalf("Could not create create transformation runner")
	}
}
