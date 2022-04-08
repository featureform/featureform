package runner

import (
	"encoding/json"
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

func trainingSetSerialize(config CreateTrainingSetRunnerConfig) Config {
	serializedConfig, _ := json.Marshal(config)
	return serializedConfig
}

func testTrainingSetErrorConfigsFactory(config Config) error {
	_, err := Create("CREATE_TRAINING_SET", config)
	return err
}

type ErrorTrainingSetFactoryConfigs struct {
	ErrorType   string
	ErrorConfig Config
}

func TestTrainingSetRunnerFactoryErrorCoverage(t *testing.T) {
	errorConfigs := []ErrorTrainingSetFactoryConfigs{
		ErrorTrainingSetFactoryConfigs{
			ErrorType:   "cannot deserialize config",
			ErrorConfig: []byte{},
		},
		ErrorTrainingSetFactoryConfigs{
			ErrorType: "cannot configure offline provider",
			ErrorConfig: trainingSetSerialize(CreateTrainingSetRunnerConfig{
				OfflineType: "Invalid_Offline_type",
			}),
		},
		ErrorTrainingSetFactoryConfigs{
			ErrorType: "cannot convert offline provider to offline store",
			ErrorConfig: trainingSetSerialize(CreateTrainingSetRunnerConfig{
				OfflineType:   provider.LocalOnline,
				OfflineConfig: []byte{},
			}),
		},
	}
	err := RegisterFactory("CREATE_TRAINING_SET", CreateTrainingSetRunnerFactory)
	if err != nil {
		t.Fatalf("Could not register training set factory: %v", err)
	}
	for _, config := range errorConfigs {
		if err := testTrainingSetErrorConfigsFactory(config.ErrorConfig); err == nil {
			t.Fatalf("Test Job Failed to catch error: %s", config.ErrorType)
		}
	}
	delete(factoryMap, "CREATE_TRAINING_SET")
}

func TestCreateTrainingSetFactory(t *testing.T) {
	serializedConfig := trainingSetSerialize(CreateTrainingSetRunnerConfig{
		OfflineType:   "MOCK_OFFLINE",
		OfflineConfig: []byte{},
		Def: provider.TrainingSetDef{
			ID:       provider.ResourceID{},
			Label:    provider.ResourceID{},
			Features: []provider.ResourceID{},
		},
	})
	err := RegisterFactory("CREATE_TRAINING_SET", CreateTrainingSetRunnerFactory)
	if err != nil {
		t.Fatalf("Could not register training set factory: %v", err)
	}
	_, err = Create("CREATE_TRAINING_SET", serializedConfig)
	if err != nil {
		t.Fatalf("Could not create create training set runner")
	}
}
