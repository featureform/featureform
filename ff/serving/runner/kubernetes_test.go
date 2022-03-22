package runner

// import (
// 	"testing"
// )
// func TestKubernetesRunnerCreate(t *testing.T) {
// 	mockRunner := &MockRunner{}
// 	mockFactory := func(config Config) (Runner, error) {
// 		return mockRunner, nil
// 	}
// 	if err := RegisterFactory("kubernetes_runner_factory", mockFactory); err != nil {
// 		t.Fatalf("Error registering factory: %v", err)
// 	}

// 	envVars := make(map[string]string)
// 	image := "test_image"
// 	numTasks := 1
// 	mockKubernetesConfig := KubernetesRunnerConfig{
// 		EnvVars: envVars,
// 		Image: image,
// 		NumTasks: numTasks,
// 	}

// 	runner, err := NewKubernetesRunner(mockKubernetesConfig)
// 	if err != nil {
// 		t.Fatalf("Error creating kubernetes runner: %v", err)
// 	}

// 	completionStatus, err := runner.Run()
// 	if err != nil {
// 		t.Fatalf("Kubernetes runner failed to run: %v", err)
// 	}

// 	completionStatus.Wait()
// 	if !completionStatus.Complete() {
// 		t.Fatalf("Runner failed to complete")
// 	}

// }
