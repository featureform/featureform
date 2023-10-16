// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pt "github.com/featureform/provider/provider_type"
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
	return nil
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
	return fmt.Errorf("could not create training set")
}

func (m MockOfflineCreateTransformationFail) GetTransformationTable(id provider.ResourceID) (provider.TransformationTable, error) {
	return nil, nil
}

func (m MockOfflineCreateTransformationFail) UpdateMaterialization(id provider.ResourceID) (provider.Materialization, error) {
	return nil, nil
}

func (m MockOfflineCreateTransformationFail) UpdateTransformation(config provider.TransformationConfig) error {
	return nil
}

func (m MockOfflineCreateTransformationFail) UpdateTrainingSet(provider.TrainingSetDef) error {
	return nil
}

func (m MockOfflineCreateTransformationFail) Close() error {
	return nil
}

func (m MockOfflineCreateTransformationFail) Check() (bool, error) {
	return false, fmt.Errorf("provider health check not implemented")
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
	runner := CreateTransformationRunner{
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
	ResetFactoryMap()
	transformationSerialize := func(ts CreateTransformationConfig) Config {
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
			ErrorConfig: transformationSerialize(CreateTransformationConfig{
				OfflineType: "Invalid_Offline_type",
			}),
		},
		{
			Name: "cannot convert offline provider to offline store",
			ErrorConfig: transformationSerialize(CreateTransformationConfig{
				OfflineType:   pt.LocalOnline,
				OfflineConfig: []byte{},
			}),
		},
	}
	err := RegisterFactory("TEST_CREATE_TRANSFORMATION", CreateTransformationRunnerFactory)
	if err != nil {
		t.Fatalf("Could not register transformation factory: %v", err)
	}
	for _, config := range errorConfigs {
		if err := testTransformationErrorConfigsFactory(config.ErrorConfig); err == nil {
			t.Fatalf("Test Job Failed to catch error: %s", config.Name)
		}
	}
	delete(factoryMap, "TEST_CREATE_TRANSFORMATION")
}

func TestTransformationFactory(t *testing.T) {
	ResetFactoryMap()
	transformationSerialize := func(ts CreateTransformationConfig) Config {
		config, err := ts.Serialize()
		if err != nil {
			t.Fatalf("error serializing transformation runner config: %v", err)
		}
		return config
	}
	serializedConfig := transformationSerialize(CreateTransformationConfig{
		OfflineType:   "MOCK_OFFLINE",
		OfflineConfig: []byte{},
		TransformationConfig: provider.TransformationConfig{
			Type:          provider.SQLTransformation,
			TargetTableID: provider.ResourceID{},
			Query:         "",
			SourceMapping: []provider.SourceMapping{},
		},
		IsUpdate: false,
	})
	err := RegisterFactory("TEST_CREATE_TRANSFORMATION", CreateTransformationRunnerFactory)
	if err != nil {
		t.Fatalf("Could not register transformation factory: %v", err)
	}
	_, err = Create("TEST_CREATE_TRANSFORMATION", serializedConfig)
	if err != nil {
		t.Fatalf("Could not create create transformation runner")
	}
	delete(factoryMap, "TEST_CREATE_TRANSFORMATION")
}

func TestCreateTransformationConfigDeserializeInterface(t *testing.T) {
	config := CreateTransformationConfig{
		OfflineType:   pt.K8sOffline,
		OfflineConfig: []byte("My config"),
		TransformationConfig: provider.TransformationConfig{
			Type: provider.DFTransformation,
			TargetTableID: provider.ResourceID{
				Type:    provider.Transformation,
				Name:    "Name",
				Variant: "variant",
			},
			Code: []byte("My code"),
			SourceMapping: []provider.SourceMapping{
				{Template: "template", Source: "source"},
			},
			Args: metadata.KubernetesArgs{
				DockerImage: "my_image",
			},
		},
	}
	serialized, err := config.Serialize()
	if err != nil {
		t.Fatalf(err.Error())
	}
	configEmpty := CreateTransformationConfig{
		OfflineType:   pt.K8sOffline,
		OfflineConfig: []byte{},
		TransformationConfig: provider.TransformationConfig{
			Type: 1,
			TargetTableID: provider.ResourceID{
				Type: provider.Transformation,
			},
			Code:          []byte{},
			SourceMapping: []provider.SourceMapping{},
		},
	}
	serializedEmpty, err := configEmpty.Serialize()
	if err != nil {
		t.Fatalf(err.Error())
	}
	type args struct {
		config Config
	}
	tests := []struct {
		name          string
		args          args
		expected      CreateTransformationConfig
		transformType metadata.TransformationArgType
		wantErr       bool
	}{
		{"Deserialize With Kubernetes Args", args{serialized}, config, metadata.K8sArgs, false},
		{"Deserialize Empty", args{serializedEmpty}, configEmpty, metadata.NoArgs, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CreateTransformationConfig{}
			if err := c.Deserialize(tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("Deserialize() error = %v, wantErr %v", err, tt.wantErr)
			}
			c.TransformationConfig.ArgType = tt.transformType
			if !reflect.DeepEqual(tt.expected, c) {
				t.Errorf("Deserialize() \nexpected = %#v \ngot = %#v", tt.expected, c)
			}
		})
	}
}
