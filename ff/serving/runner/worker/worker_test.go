// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package worker

import (
	"errors"
	runner "github.com/featureform/serving/runner"
	"testing"
)

type MockRunner struct {
}

type MockIndexRunner struct {
	index int
}

type MockUpdateRunner struct {
	Resource metadata.ResourceID
}

type MockCompletionWatcher struct{}

func (m *MockRunner) Run() (runner.CompletionWatcher, error) {
	return &MockCompletionWatcher{}, nil
}

func (m *MockRunner) Resource() metadata.ResourceID {
	return metadata.ResourceID{}
}

func (m *MockRunner) IsUpdateJob() bool {
	return false
}

func (m *MockIndexRunner) Run() (runner.CompletionWatcher, error) {
	return &MockCompletionWatcher{}, nil
}

func (m *MockIndexRunner) Resource() metadata.ResourceID {
	return metadata.ResourceID{}
}

func (m *MockIndexRunner) IsUpdateJob() bool {
	return false
}

func (m *MockIndexRunner) SetIndex(index int) error {
	m.index = index
	return nil
}

func (m *MockUpdateRunner) Run() (runner.CompletionWatcher, error) {
	return &MockCompletionWatcher{}, nil
}

func (m *MockUpdateRunner) Resource() metadata.ResourceID {
	return m.Resource
}

func (m *MockUpdateRunner) IsUpdateJob() bool {
	return true
}

func (m *MockCompletionWatcher) Complete() bool {
	return false
}

func (m *MockCompletionWatcher) String() string {
	return ""
}

func (m *MockCompletionWatcher) Wait() error {
	return nil
}

func (m *MockCompletionWatcher) Err() error {
	return nil
}

type RunnerWithFailingWatcher struct{}

func (r *RunnerWithFailingWatcher) Run() (runner.CompletionWatcher, error) {
	return &FailingWatcher{}, nil
}

type FailingWatcher struct{}

func (f *FailingWatcher) Complete() bool {
	return false
}
func (f *FailingWatcher) String() string {
	return ""
}
func (f *FailingWatcher) Wait() error {
	return errors.New("Run failed")
}
func (f *FailingWatcher) Err() error {
	return errors.New("Run failed")
}

type FailingRunner struct{}

func (f *FailingRunner) Run() (runner.CompletionWatcher, error) {
	return nil, errors.New("Failed to run runner")
}

type FailingIndexRunner struct{}

func (f *FailingIndexRunner) Run() (runner.CompletionWatcher, error) {
	return &MockCompletionWatcher{}, nil
}

func (f *FailingIndexRunner) SetIndex(index int) error {
	return errors.New("failed to set index")
}

func registerMockRunnerFactoryFailingWatcher() error {
	mockRunnerFailingWatcher := &RunnerWithFailingWatcher{}
	mockFactory := func(config runner.Config) (runner.Runner, error) {
		return mockRunnerFailingWatcher, nil
	}
	if err := runner.RegisterFactory("test", mockFactory); err != nil {
		return err
	}
	return nil
}

func registerMockFailRunnerFactory() error {
	failRunner := &FailingRunner{}
	failRunnerFactory := func(config runner.Config) (runner.Runner, error) {
		return failRunner, nil
	}
	if err := runner.RegisterFactory("test", failRunnerFactory); err != nil {
		return err
	}
	return nil
}

func registerMockRunnerFactory() error {
	mockRunner := &MockRunner{}
	mockFactory := func(config runner.Config) (runner.Runner, error) {
		return mockRunner, nil
	}
	if err := runner.RegisterFactory("test", mockFactory); err != nil {
		return err
	}
	return nil
}

func registerMockIndexRunnerFactory() error {
	mockRunner := &MockIndexRunner{}
	mockFactory := func(config runner.Config) (runner.Runner, error) {
		return mockRunner, nil
	}
	if err := runner.RegisterFactory("test", mockFactory); err != nil {
		return err
	}
	return nil
}

func registerMockFailIndexRunnerFactory() error {
	mockRunner := &FailingIndexRunner{}
	mockFactory := func(config runner.Config) (runner.Runner, error) {
		return mockRunner, nil
	}
	if err := runner.RegisterFactory("test", mockFactory); err != nil {
		return err
	}
	return nil
}

func TestBasicRunner(t *testing.T) {
	runner.ResetFactoryMap()
	if err := registerMockRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", "test")
	if err := CreateAndRun(); err != nil {
		t.Fatalf("Error running mock runner: %v", err)
	}
}
func TestBasicRunnerIndex(t *testing.T) {
	runner.ResetFactoryMap()
	if err := registerMockIndexRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", "test")
	t.Setenv("JOB_COMPLETION_INDEX", "0")
	if err := CreateAndRun(); err != nil {
		t.Fatalf("Error running mock runner: %v", err)
	}
}

func TestBasicRunnerNoSetIndex(t *testing.T) {
	runner.ResetFactoryMap()
	if err := registerMockIndexRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", "test")
	if err := CreateAndRun(); err == nil {
		t.Fatalf("failed to capture error no set index")
	}
}

func TestInvalidIndex(t *testing.T) {
	runner.ResetFactoryMap()
	if err := registerMockIndexRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", "test")
	t.Setenv("JOB_COMPLETION_INDEX", "a")
	if err := CreateAndRun(); err == nil {
		t.Fatalf("failed to catch invalid index")
	}
}

func TestUnneededIndex(t *testing.T) {
	runner.ResetFactoryMap()
	if err := registerMockRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", "test")
	t.Setenv("JOB_COMPLETION_INDEX", "0")
	if err := CreateAndRun(); err == nil {
		t.Fatalf("failed to catch unneeded index error")
	}
}

func TestIndexSetFail(t *testing.T) {
	runner.ResetFactoryMap()
	if err := registerMockFailIndexRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", "test")
	t.Setenv("JOB_COMPLETION_INDEX", "0")
	if err := CreateAndRun(); err == nil {
		t.Fatalf("failed to catch unneeded index error")
	}
}

func TestRunnerNoConfig(t *testing.T) {
	runner.ResetFactoryMap()
	if err := registerMockRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	t.Setenv("NAME", "test")
	if err := CreateAndRun(); err == nil {
		t.Fatalf("Failed to call error on missing config envvar")
	}
}

func TestRunnerNoName(t *testing.T) {
	runner.ResetFactoryMap()
	if err := registerMockRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	if err := CreateAndRun(); err == nil {
		t.Fatalf("Failed to call error on missing name envvar")
	}
}

func TestRunnerNoFactory(t *testing.T) {
	runner.ResetFactoryMap()
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", "test")
	if err := CreateAndRun(); err == nil {
		t.Fatalf("Failed to call error on missing runner factory")
	}
}

func TestRunnerCreateFail(t *testing.T) {
	runner.ResetFactoryMap()
	if err := registerMockFailRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", "test")
	if err := CreateAndRun(); err == nil {
		t.Fatalf("Broken runner doesn't fail when run")
	}
}

func TestRunnerRunFail(t *testing.T) {
	runner.ResetFactoryMap()
	if err := registerMockRunnerFactoryFailingWatcher(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", "test")
	if err := CreateAndRun(); err == nil {
		t.Fatalf("Broken watcher does not return error")
	}
}

func registerUpdateMockRunnerFactory(resID metadata.ResourceID) error {
	mockRunner := &MockRunner{Resource: resID}
	mockFactory := func(config runner.Config) (runner.Runner, error) {
		return mockRunner, nil
	}
	if err := runner.RegisterFactory("test", mockFactory); err != nil {
		return err
	}
	return nil
}

func TestBasicUpdateRunner(t *testing.T) {

	if testing.Short() {
		return
	}
	runner.ResetFactoryMap()
	resourceName := uuid.New().String()
	resourceVariant := ""
	resourceType := metadata.FEATURE_VARIANT
	resourceID := metadata.ResourceID{resourceName, resourceVariant, resourceType}
	if err := registerUpdateMockRunnerFactory(resourceID); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	config := runner.Config{}
	etcdConfig := &coordinator.ETCDConfig{Endpoints: []string{"localhost:2379"}}
	serializedETCD, err := etcdConfig.Serialize()
	if err != nil {
		return nil, err
	}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", "test")
	t.Setenv("ETCD_CONFIG", string(serializedETCD))
	if err := CreateAndRun(); err != nil {
		t.Fatalf("Error running mock runner: %v", err)
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: time.Second * 1,
	})
	if err != nil {
		log.Fatalf("Could not connect to etcd client: %v", err)
	}
	resp, err := client.GetWithPrefix("UPDATE_EVENT_%s__%s__%s", jobResource.Name, jobResource.Variant, jobResource.Type.String())
	if err != nil {
		t.Fatalf("Worker did not set update event on success of scheduled job")
	}
	for _, ev := range resp.Kvs {
		fmt.Printf("%s : %s\n", ev.Key, ev.Value)
	}
}
