package runner

import (
	"errors"
	"testing"
)

type RunnerWithFailingWatcher struct{}

func (r *RunnerWithFailingWatcher) Run() (CompletionWatcher, error) {
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

func (f *FailingRunner) Run() (CompletionWatcher, error) {
	return nil, errors.New("Failed to run runner")
}

func registerMockRunnerFactoryFailingWatcher() error {
	mockRunnerFailingWatcher := &RunnerWithFailingWatcher{}
	mockFactory := func(config Config) (Runner, error) {
		return mockRunnerFailingWatcher, nil
	}
	if err := RegisterFactory("test", mockFactory); err != nil {
		return err
	}
	return nil
}

func registerMockFailRunnerFactory() error {
	failRunner := &FailingRunner{}
	failRunnerFactory := func(config Config) (Runner, error) {
		return failRunner, nil
	}
	if err := RegisterFactory("test", failRunnerFactory); err != nil {
		return err
	}
	return nil
}

func registerMockRunnerFactory() error {
	mockRunner := &MockRunner{}
	mockFactory := func(config Config) (Runner, error) {
		return mockRunner, nil
	}
	if err := RegisterFactory("test", mockFactory); err != nil {
		return err
	}
	return nil
}

func TestBasicRunner(t *testing.T) {
	factoryMap = make(map[string]RunnerFactory)
	if err := registerMockRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	config := Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", "test")
	if err := createAndRun(); err != nil {
		t.Fatalf("Error running mock runner: %v", err)
	}
}

func TestRunnerNoConfig(t *testing.T) {
	factoryMap = make(map[string]RunnerFactory)
	if err := registerMockRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	t.Setenv("NAME", "test")
	if err := createAndRun(); err == nil {
		t.Fatalf("Failed to call error on missing config envvar")
	}
}

func TestRunnerNoName(t *testing.T) {
	factoryMap = make(map[string]RunnerFactory)
	if err := registerMockRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	config := Config{}
	t.Setenv("CONFIG", string(config))
	if err := createAndRun(); err == nil {
		t.Fatalf("Failed to call error on missing name envvar")
	}
}

func TestRunnerNoFactory(t *testing.T) {
	factoryMap = make(map[string]RunnerFactory)
	config := Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", "test")
	if err := createAndRun(); err == nil {
		t.Fatalf("Failed to call error on missing runner factory")
	}
}

func TestRunnerCreateFail(t *testing.T) {
	factoryMap = make(map[string]RunnerFactory)
	if err := registerMockFailRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	config := Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", "test")
	if err := createAndRun(); err == nil {
		t.Fatalf("Broken runner doesn't fail when run")
	}
}

func TestRunnerRunFail(t *testing.T) {
	factoryMap = make(map[string]RunnerFactory)
	if err := registerMockRunnerFactoryFailingWatcher(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	config := Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", "test")
	if err := createAndRun(); err == nil {
		t.Fatalf("Broken watcher does not return error")
	}
}
