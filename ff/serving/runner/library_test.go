package runner

import (
	"errors"
	"testing"
)

type MockRunner struct{}

type MockCompletionStatus struct{}

func (m *MockRunner) Run() (CompletionStatus, error) {
	return &MockCompletionStatus{}, nil
}

func (m *MockCompletionStatus) Complete() bool {
	return false
}

func (m *MockCompletionStatus) String() string {
	return ""
}

func (m *MockCompletionStatus) Wait() error {
	return nil
}

func (m *MockCompletionStatus) Err() error {
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
