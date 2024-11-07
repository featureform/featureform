// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package runner

import (
	"fmt"
	"testing"

	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pl "github.com/featureform/provider/location"
	pt "github.com/featureform/provider/provider_type"
)

type MockOfflineCreateTrainingSetFail struct {
	provider.BaseProvider
}

func (m MockOfflineCreateTrainingSetFail) CreateResourceTable(provider.ResourceID, provider.TableSchema) (provider.OfflineTable, error) {
	return nil, nil
}

func (m MockOfflineCreateTrainingSetFail) GetResourceTable(id provider.ResourceID) (provider.OfflineTable, error) {
	return nil, nil
}

func (m MockOfflineCreateTrainingSetFail) CreateMaterialization(id provider.ResourceID, opts provider.MaterializationOptions) (provider.Materialization, error) {
	return nil, nil
}

func (m MockOfflineCreateTrainingSetFail) SupportsMaterializationOption(opt provider.MaterializationOptionType) (bool, error) {
	return false, nil
}

func (m MockOfflineCreateTrainingSetFail) UpdateMaterialization(id provider.ResourceID, opts provider.MaterializationOptions) (provider.Materialization, error) {
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

func (m MockOfflineCreateTrainingSetFail) CreateTrainTestSplit(provider.TrainTestSplitDef) (func() error, error) {
	return nil, nil
}

func (m MockOfflineCreateTrainingSetFail) GetTrainTestSplit(def provider.TrainTestSplitDef) (provider.TrainingSetIterator, provider.TrainingSetIterator, error) {
	return nil, nil, nil
}

func (m MockOfflineCreateTrainingSetFail) GetBatchFeatures(ids []provider.ResourceID) (provider.BatchFeatureIterator, error) {
	return nil, nil
}
func (m MockOfflineCreateTrainingSetFail) UpdateTrainingSet(provider.TrainingSetDef) error {
	return nil
}
func (m MockOfflineCreateTrainingSetFail) CreatePrimaryTable(id provider.ResourceID, schema provider.TableSchema) (provider.PrimaryTable, error) {
	return nil, nil
}
func (m MockOfflineCreateTrainingSetFail) GetPrimaryTable(id provider.ResourceID, source metadata.SourceVariant) (provider.PrimaryTable, error) {
	return nil, nil
}

func (m MockOfflineCreateTrainingSetFail) RegisterResourceFromSourceTable(id provider.ResourceID, schema provider.ResourceSchema, opts ...provider.ResourceOption) (provider.OfflineTable, error) {
	return nil, nil
}

func (m MockOfflineCreateTrainingSetFail) RegisterPrimaryFromSourceTable(id provider.ResourceID, tableLocation pl.Location) (provider.PrimaryTable, error) {
	return nil, nil
}

func (m MockOfflineCreateTrainingSetFail) SupportsTransformationOption(opt provider.TransformationOptionType) (bool, error) {
	return false, nil
}

func (m MockOfflineCreateTrainingSetFail) CreateTransformation(config provider.TransformationConfig, opt ...provider.TransformationOption) error {
	return nil
}
func (m MockOfflineCreateTrainingSetFail) UpdateTransformation(config provider.TransformationConfig, opts ...provider.TransformationOption) error {
	return nil
}
func (m MockOfflineCreateTrainingSetFail) GetTransformationTable(id provider.ResourceID) (provider.TransformationTable, error) {
	return nil, nil
}

func (m MockOfflineCreateTrainingSetFail) Close() error {
	return nil
}

func (m MockOfflineCreateTrainingSetFail) CheckHealth() (bool, error) {
	return false, fmt.Errorf("provider health check not implemented")
}

func (m MockOfflineCreateTrainingSetFail) ResourceLocation(id provider.ResourceID, resource any) (pl.Location, error) {
	return nil, nil
}

func TestRunTrainingSet(t *testing.T) {
	runner := TrainingSetRunner{
		MockOfflineStore{},
		provider.TrainingSetDef{},
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

func TestFailTrainingSet(t *testing.T) {
	runner := TrainingSetRunner{
		MockOfflineCreateTrainingSetFail{},
		provider.TrainingSetDef{},
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

func testTrainingSetErrorConfigsFactory(config Config) error {
	_, err := Create("TEST_CREATE_TRAINING_SET", config)
	return err
}

type ErrorTrainingSetFactoryConfigs struct {
	Name        string
	ErrorConfig Config
}

func TestTrainingSetRunnerFactoryErrorCoverage(t *testing.T) {
	trainingSetSerialize := func(ts TrainingSetRunnerConfig) Config {
		config, err := ts.Serialize()
		if err != nil {
			t.Fatalf("error serializing training set runner config: %v", err)
		}
		return config
	}
	errorConfigs := []ErrorTrainingSetFactoryConfigs{
		{
			Name:        "cannot deserialize config",
			ErrorConfig: []byte{},
		},
		{
			Name: "cannot configure offline provider",
			ErrorConfig: trainingSetSerialize(TrainingSetRunnerConfig{
				OfflineType: "Invalid_Offline_type",
			}),
		},
		{
			Name: "cannot convert offline provider to offline store",
			ErrorConfig: trainingSetSerialize(TrainingSetRunnerConfig{
				OfflineType:   pt.LocalOnline,
				OfflineConfig: []byte{},
			}),
		},
	}
	err := RegisterFactory("TEST_CREATE_TRAINING_SET", TrainingSetRunnerFactory)
	if err != nil {
		t.Fatalf("Could not register training set factory: %v", err)
	}
	for _, config := range errorConfigs {
		if err := testTrainingSetErrorConfigsFactory(config.ErrorConfig); err == nil {
			t.Fatalf("Test Job Failed to catch error: %s", config.Name)
		}
	}
	delete(factoryMap, "TEST_CREATE_TRAINING_SET")
}

func TestTrainingSetFactory(t *testing.T) {
	trainingSetSerialize := func(ts TrainingSetRunnerConfig) Config {
		config, err := ts.Serialize()
		if err != nil {
			t.Fatalf("error serializing training set runner config: %v", err)
		}
		return config
	}
	serializedConfig := trainingSetSerialize(TrainingSetRunnerConfig{
		OfflineType:   "MOCK_OFFLINE",
		OfflineConfig: []byte{},
		Def: provider.TrainingSetDef{
			ID:       provider.ResourceID{},
			Label:    provider.ResourceID{},
			Features: []provider.ResourceID{},
		},
		IsUpdate: false,
	})
	err := RegisterFactory("TEST_CREATE_TRAINING_SET", TrainingSetRunnerFactory)
	if err != nil {
		t.Fatalf("Could not register training set factory: %v", err)
	}
	_, err = Create("TEST_CREATE_TRAINING_SET", serializedConfig)
	if err != nil {
		t.Fatalf("Could not create create training set runner")
	}
}

// TODO: (Erik) improve and expand on this test
func TestTrainingSetRunnerConfigSerde(t *testing.T) {
	tests := []struct {
		name string
		cfg  TrainingSetRunnerConfig
	}{
		{
			name: "Valid Config",
			cfg: TrainingSetRunnerConfig{
				OfflineType:   pt.SnowflakeOffline,
				OfflineConfig: []byte(`{"account":"account","password":"password","role":"role","warehouse":"warehouse","database":"database","schema":"schema"}`),
				Def: provider.TrainingSetDef{
					ID:    provider.ResourceID{Name: "ts", Variant: "ts-variant", Type: provider.TrainingSet},
					Label: provider.ResourceID{Name: "lbl", Variant: "lbl-variant", Type: provider.Label},
					LabelSourceMapping: provider.SourceMapping{
						Template:            "SELECT * FROM table",
						Source:              "table",
						ProviderType:        pt.SnowflakeOffline,
						ProviderConfig:      []byte(`{"account":"account","password":"password","role":"role","warehouse":"warehouse","database":"database","schema":"schema"}`),
						TimestampColumnName: "ts",
						Location:            pl.NewSQLLocation("table"),
					},
					Features: []provider.ResourceID{
						{Name: "f1", Variant: "f1-variant", Type: provider.Feature},
					},
					FeatureSourceMappings: []provider.SourceMapping{
						{
							Template:            "SELECT * FROM table",
							Source:              "table",
							ProviderType:        pt.SnowflakeOffline,
							ProviderConfig:      []byte(`{"account":"account","password":"password","role":"role","warehouse":"warehouse","database":"database","schema":"schema"}`),
							TimestampColumnName: "ts",
							Location:            pl.NewSQLLocation("table"),
						},
					},
				},
				IsUpdate: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serialized, err := tt.cfg.Serialize()
			if err != nil {
				t.Fatalf("failed to serialize config: %v", err)
			}

			deserialized := TrainingSetRunnerConfig{}
			err = deserialized.Deserialize(serialized)
			if err != nil {
				t.Fatalf("failed to deserialize config: %v", err)
			}

		})
	}
}
