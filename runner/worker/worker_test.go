// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package worker

import (
	"errors"
	"testing"

	"github.com/featureform/metadata"
	"github.com/featureform/runner"
	"github.com/featureform/types"
)

const mockFactoryName = "test"

type MockRunner struct {
}

type MockIndexRunner struct {
	index int
}

type MockUpdateRunner struct {
	ResourceID metadata.ResourceID
}

type MockCompletionWatcher struct{}

func (m *MockRunner) Run() (types.CompletionWatcher, error) {
	return &MockCompletionWatcher{}, nil
}

func (m *MockRunner) Resource() metadata.ResourceID {
	return metadata.ResourceID{}
}

func (m *MockRunner) IsUpdateJob() bool {
	return false
}

func (m *MockIndexRunner) Run() (types.CompletionWatcher, error) {
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

func (m *MockUpdateRunner) Run() (types.CompletionWatcher, error) {
	return &MockCompletionWatcher{}, nil
}

func (m *MockUpdateRunner) Resource() metadata.ResourceID {
	return m.ResourceID
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

func (r *RunnerWithFailingWatcher) Run() (types.CompletionWatcher, error) {
	return &FailingWatcher{}, nil
}

func (r *RunnerWithFailingWatcher) Resource() metadata.ResourceID {
	return metadata.ResourceID{}
}

func (r *RunnerWithFailingWatcher) IsUpdateJob() bool {
	return false
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

func (f *FailingRunner) Run() (types.CompletionWatcher, error) {
	return nil, errors.New("Failed to run runner")
}

func (f *FailingRunner) Resource() metadata.ResourceID {
	return metadata.ResourceID{}
}

func (f *FailingRunner) IsUpdateJob() bool {
	return false
}

type FailingIndexRunner struct{}

func (f *FailingIndexRunner) Run() (types.CompletionWatcher, error) {
	return &MockCompletionWatcher{}, nil
}

func (f *FailingIndexRunner) Resource() metadata.ResourceID {
	return metadata.ResourceID{}
}

func (f *FailingIndexRunner) IsUpdateJob() bool {
	return false
}

func (f *FailingIndexRunner) SetIndex(index int) error {
	return errors.New("failed to set index")
}

func registerMockRunnerFactoryFailingWatcher() error {
	mockRunnerFailingWatcher := &RunnerWithFailingWatcher{}
	mockFactory := func(config runner.Config) (types.Runner, error) {
		return mockRunnerFailingWatcher, nil
	}
	if err := runner.RegisterFactory(mockFactoryName, mockFactory); err != nil {
		return err
	}
	return nil
}

func registerMockFailRunnerFactory() error {
	failRunner := &FailingRunner{}
	failRunnerFactory := func(config runner.Config) (types.Runner, error) {
		return failRunner, nil
	}
	if err := runner.RegisterFactory(mockFactoryName, failRunnerFactory); err != nil {
		return err
	}
	return nil
}

func registerMockRunnerFactory() error {
	mockRunner := &MockRunner{}
	mockFactory := func(config runner.Config) (types.Runner, error) {
		return mockRunner, nil
	}
	if err := runner.RegisterFactory(mockFactoryName, mockFactory); err != nil {
		return err
	}
	return nil
}

func registerMockIndexRunnerFactory() error {
	mockRunner := &MockIndexRunner{}
	mockFactory := func(config runner.Config) (types.Runner, error) {
		return mockRunner, nil
	}
	if err := runner.RegisterFactory(mockFactoryName, mockFactory); err != nil {
		return err
	}
	return nil
}

func registerMockFailIndexRunnerFactory() error {
	mockRunner := &FailingIndexRunner{}
	mockFactory := func(config runner.Config) (types.Runner, error) {
		return mockRunner, nil
	}
	if err := runner.RegisterFactory(mockFactoryName, mockFactory); err != nil {
		return err
	}
	return nil
}

func TestBasicRunner(t *testing.T) {
	if err := registerMockRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	defer runner.UnregisterFactory(mockFactoryName)
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", mockFactoryName)
	if err := CreateAndRun(); err != nil {
		t.Fatalf("Error running mock runner: %v", err)
	}
}
func TestBasicRunnerIndex(t *testing.T) {
	if err := registerMockIndexRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	defer runner.UnregisterFactory(mockFactoryName)
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", mockFactoryName)
	t.Setenv("JOB_COMPLETION_INDEX", "0")
	if err := CreateAndRun(); err != nil {
		t.Fatalf("Error running mock runner: %v", err)
	}
}

func TestBasicRunnerNoSetIndex(t *testing.T) {
	if err := registerMockIndexRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	defer runner.UnregisterFactory(mockFactoryName)
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", mockFactoryName)
	if err := CreateAndRun(); err == nil {
		t.Fatalf("failed to capture error no set index")
	}
}

func TestInvalidIndex(t *testing.T) {
	if err := registerMockIndexRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	defer runner.UnregisterFactory(mockFactoryName)
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", mockFactoryName)
	t.Setenv("JOB_COMPLETION_INDEX", "a")
	if err := CreateAndRun(); err == nil {
		t.Fatalf("failed to catch invalid index")
	}
}

func TestUnneededIndex(t *testing.T) {
	if err := registerMockRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	defer runner.UnregisterFactory(mockFactoryName)
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", mockFactoryName)
	t.Setenv("JOB_COMPLETION_INDEX", "0")
	if err := CreateAndRun(); err == nil {
		t.Fatalf("failed to catch unneeded index error")
	}
}

func TestIndexSetFail(t *testing.T) {
	if err := registerMockFailIndexRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	defer runner.UnregisterFactory(mockFactoryName)
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", mockFactoryName)
	t.Setenv("JOB_COMPLETION_INDEX", "0")
	if err := CreateAndRun(); err == nil {
		t.Fatalf("failed to catch unneeded index error")
	}
}

func TestRunnerNoConfig(t *testing.T) {
	if err := registerMockRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	defer runner.UnregisterFactory(mockFactoryName)
	t.Setenv("NAME", mockFactoryName)
	if err := CreateAndRun(); err == nil {
		t.Fatalf("Failed to call error on missing config envvar")
	}
}

func TestRunnerNoName(t *testing.T) {
	if err := registerMockRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	defer runner.UnregisterFactory(mockFactoryName)
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	if err := CreateAndRun(); err == nil {
		t.Fatalf("Failed to call error on missing name envvar")
	}
}

func TestRunnerNoFactory(t *testing.T) {
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", mockFactoryName)
	if err := CreateAndRun(); err == nil {
		t.Fatalf("Failed to call error on missing runner factory")
	}
}

func TestRunnerCreateFail(t *testing.T) {
	if err := registerMockFailRunnerFactory(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	defer runner.UnregisterFactory(mockFactoryName)
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", mockFactoryName)
	if err := CreateAndRun(); err == nil {
		t.Fatalf("Broken runner doesn't fail when run")
	}
}

func TestRunnerRunFail(t *testing.T) {
	if err := registerMockRunnerFactoryFailingWatcher(); err != nil {
		t.Fatalf("Error registering mock runner factory: %v", err)
	}
	defer runner.UnregisterFactory(mockFactoryName)
	config := runner.Config{}
	t.Setenv("CONFIG", string(config))
	t.Setenv("NAME", mockFactoryName)
	if err := CreateAndRun(); err == nil {
		t.Fatalf("Broken watcher does not return error")
	}
}

func registerUpdateMockRunnerFactory(resID metadata.ResourceID) error {
	mockRunner := &MockUpdateRunner{ResourceID: resID}
	mockFactory := func(config runner.Config) (types.Runner, error) {
		return mockRunner, nil
	}
	if err := runner.RegisterFactory(mockFactoryName, mockFactory); err != nil {
		return err
	}
	return nil
}
