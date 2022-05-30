// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"errors"
	metadata "github.com/featureform/serving/metadata"
	"testing"
)

type MockRunner struct{}

type MockCompletionWatcher struct{}

func (m *MockRunner) Run() (CompletionWatcher, error) {
	return &MockCompletionWatcher{}, nil
}

func (m *MockRunner) Resource() metadata.ResourceID {
	return metadata.ResourceID{}
}

func (m *MockRunner) IsUpdateJob() bool {
	return false
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

func TestRegisterAndCreate(t *testing.T) {
	mockRunner := &MockRunner{}
	mockConfig := []byte{}
	mockFactory := func(config Config) (Runner, error) {
		return mockRunner, nil
	}
	if err := RegisterFactory("mock", mockFactory); err != nil {
		t.Fatalf("Error registering factory: %v", err)
	}
	if _, err := Create("mock", mockConfig); err != nil {
		t.Fatalf("Error creating runner: %v", err)
	}
	if err := RegisterFactory("mock", mockFactory); err == nil {
		t.Fatalf("Register factory allowed duplicate registration")
	}
	if _, err := Create("doesNotExist", mockConfig); err == nil {
		t.Fatalf("Created unregistered runner")
	}
}

func TestCreateRunnerError(t *testing.T) {
	errorFactory := func(config Config) (Runner, error) {
		return nil, errors.New("creating runner triggered error")
	}
	mockConfig := []byte{}
	if err := RegisterFactory("error", errorFactory); err != nil {
		t.Fatalf("Error registering factory: %v", err)
	}
	if _, err := Create("error", mockConfig); err == nil {
		t.Fatalf("Failed to record error creating runner")
	}
}
