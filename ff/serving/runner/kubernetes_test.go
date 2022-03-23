package runner

import (
	"github.com/google/uuid"
	batchv1 "k8s.io/api/batch/v1"
	watch "k8s.io/apimachinery/pkg/watch"
	"testing"
)

func NewMockKubernetesRunner(config KubernetesRunnerConfig) (Runner, error) {
	jobSpec := newJobSpec(config)
	jobName := uuid.New().String()
	namespace := "default"
	jobClient := MockJobClient{
		JobName:   jobName,
		Namespace: namespace,
	}
	return KubernetesRunner{
		jobClient: jobClient,
		jobSpec:   &jobSpec,
	}, nil
}

type MockJobClient struct {
	JobName   string
	Namespace string
}

func (m MockJobClient) Get() (*batchv1.Job, error) {
	return &batchv1.Job{}, nil
}

func (m MockJobClient) Watch() (watch.Interface, error) {
	return watch.NewEmptyWatch(), nil
}

func (m MockJobClient) Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error) {
	return &batchv1.Job{}, nil
}
func TestKubernetesRunnerCreate(t *testing.T) {

	runner, err := NewMockKubernetesRunner(KubernetesRunnerConfig{envVars: map[string]string{"test": "envVar"}, image: "test", numTasks: 1})
	if err != nil {
		t.Fatalf("Failed to create Kubernetes runner")
	}

	completionWatcher, err := runner.Run()
	if err != nil {
		t.Fatalf("Failed to initialize run of Kubernetes runner")
	}

	if err := completionWatcher.Wait(); err != nil {
		t.Fatalf("Kubernetes runner failed while running")
	}

	if completionWatcher.Err() != nil {
		t.Fatalf("Wait failed to report error")
	}

	if !completionWatcher.Complete() {
		t.Fatalf("Kubernetes runner failed to set complete")
	}
	completionWatcher.String()

}
