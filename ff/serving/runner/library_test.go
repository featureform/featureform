package runner

import (
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

	err := RegisterFactory("mock", mockFactory)
	if err != nil {
		t.Fatalf("Error registering factory: %v", err)
	}

	_, err = Create("mock", mockConfig)
	if err != nil {
		t.Fatalf("Error creating runner: %v", err)
	}

	err = RegisterFactory("mock", mockFactory)
	if err == nil {
		t.Fatalf("Register factory allowed duplicate registration")
	}

	_, err = Create("doesNotExist", mockConfig)
	if err == nil {
		t.Fatalf("Created unregistered runner")
	}
}
