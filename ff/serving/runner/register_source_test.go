// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"fmt"
	provider "github.com/featureform/serving/provider"
	"testing"
)

type MockOfflineRegisterSourceFail struct {
	provider.BaseProvider
}

func (m MockOfflineRegisterSourceFail) CreateResourceTable(provider.ResourceID, provider.TableSchema) (provider.OfflineTable, error) {
	return nil, nil
}
func (m MockOfflineRegisterSourceFail) GetResourceTable(id provider.ResourceID) (provider.OfflineTable, error) {
	return nil, nil
}
func (m MockOfflineRegisterSourceFail) CreateMaterialization(id provider.ResourceID) (provider.Materialization, error) {
	return nil, nil
}
func (m MockOfflineRegisterSourceFail) GetMaterialization(id provider.MaterializationID) (provider.Materialization, error) {
	return nil, nil
}
func (m MockOfflineRegisterSourceFail) DeleteMaterialization(id provider.MaterializationID) error {
	return nil
}
func (m MockOfflineRegisterSourceFail) CreateTrainingSet(provider.TrainingSetDef) error {
	return fmt.Errorf("could not create training set")
}
func (m MockOfflineRegisterSourceFail) GetTrainingSet(id provider.ResourceID) (provider.TrainingSetIterator, error) {
	return nil, nil
}

func (m MockOfflineRegisterSourceFail) CreatePrimaryTable(id provider.ResourceID, schema provider.TableSchema) (provider.PrimaryTable, error) {
	return nil, nil
}
func (m MockOfflineRegisterSourceFail) GetPrimaryTable(id provider.ResourceID) (provider.PrimaryTable, error) {
	return nil, nil
}

func (m MockOfflineRegisterSourceFail) RegisterResourceFromSourceTable(id provider.ResourceID, schema provider.ResourceSchema) (provider.OfflineTable, error) {
	return nil, nil
}

func (m MockOfflineRegisterSourceFail) RegisterPrimaryFromSourceTable(id provider.ResourceID, sourceName string) (provider.PrimaryTable, error) {
	return nil, nil
}

func (m MockOfflineRegisterSourceFail) CreateTransformation(config provider.TransformationConfig) error {
	return nil
}

func (m MockOfflineRegisterSourceFail) GetTransformationTable(id provider.ResourceID) (provider.TransformationTable, error) {
	return nil, nil
}

func TestRunRegisterResource(t *testing.T) {
	runner := RegisterSourceRunner{
		MockOfflineStore{},
		provider.ResourceID{},
		"",
	}
	watcher, err := runner.Run()
	if err != nil {
		t.Fatalf("failed to create create register source runner: %v", err)
	}
	if err := watcher.Wait(); err != nil {
		t.Fatalf("register source runner failed: %v", err)
	}
}

func TestFailRegisterResource(t *testing.T) {
	runner := RegisterSourceRunner{
		MockOfflineStore{},
		provider.ResourceID{},
		"",
	}
	watcher, err := runner.Run()
	if err != nil {
		t.Fatalf("failed to create register source runner: %v", err)
	}
	if err := watcher.Wait(); err == nil {
		t.Fatalf("failed to report error creating registered source")
	}
}

func testRegisterSourceErrorConfigsFactory(config Config) error {
	_, err := Create(REGISTER_SOURCE, config)
	return err
}

type ErrorRegisterSourceFactoryConfigs struct {
	Name        string
	ErrorConfig Config
}

func TestRegisterSourceRunnerFactoryErrorCoverage(t *testing.T) {
	registerSourceSerialize := func(rs RegisterSourceConfig) Config {
		config, err := rs.Serialize()
		if err != nil {
			t.Fatalf("error serializing register source runner config: %v", err)
		}
		return config
	}
	errorConfigs := []ErrorRegisterSourceFactoryConfigs{
		{
			Name:        "cannot deserialize config",
			ErrorConfig: []byte{},
		},
		{
			Name: "cannot configure offline provider",
			ErrorConfig: registerSourceSerialize(RegisterSourceConfig{
				OfflineType: "Invalid_Offline_type",
			}),
		},
		{
			Name: "cannot convert offline provider to offline store",
			ErrorConfig: registerSourceSerialize(RegisterSourceConfig{
				OfflineType:   provider.LocalOnline,
				OfflineConfig: []byte{},
			}),
		},
	}
	err := RegisterFactory(REGISTER_SOURCE, RegisterSourceRunnerFactory)
	if err != nil {
		t.Fatalf("Could not register register source factory: %v", err)
	}
	for _, config := range errorConfigs {
		if err := testRegisterSourceErrorConfigsFactory(config.ErrorConfig); err == nil {
			t.Fatalf("Test Job Failed to catch error: %s", config.Name)
		}
	}
	delete(factoryMap, CREATE_TRAINING_SET)
}

func TestRegisterSourceFactory(t *testing.T) {
	registerSourceSerialize := func(ts RegisterSourceConfig) Config {
		config, err := ts.Serialize()
		if err != nil {
			t.Fatalf("error serializing register source runner config: %v", err)
		}
		return config
	}
	serializedConfig := registerSourceSerialize(RegisterSourceConfig{
		OfflineType:     "MOCK_OFFLINE",
		OfflineConfig:   []byte{},
		ResourceID:      provider.ResourceID{},
		SourceTableName: "",
	},
	)
	err := RegisterFactory(REGISTER_SOURCE, RegisterSourceRunnerFactory)
	if err != nil {
		t.Fatalf("Could not register register source factory: %v", err)
	}
	_, err = Create(REGISTER_SOURCE, serializedConfig)
	if err != nil {
		t.Fatalf("Could not create create register source runner")
	}
}
