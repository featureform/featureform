package runner

import (
	"errors"
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

type MockJobClientBroken struct{}

func (m MockJobClientBroken) Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error) {
	return nil, errors.New("cannot create job")
}

func (m MockJobClientBroken) Get() (*batchv1.Job, error) {
	return nil, errors.New("cannot get job")
}

func (m MockJobClientBroken) Watch() (watch.Interface, error) {
	return nil, errors.New("cannot get watcher")
}

func TestJobClientCreateFail(t *testing.T) {
	runner := KubernetesRunner{
		jobClient: MockJobClientBroken{},
		jobSpec:   &batchv1.JobSpec{},
	}
	if _, err := runner.Run(); err == nil {
		t.Fatalf("Failed to trigger error on failure to create job")
	}
}

type MockJobClientRunBroken struct{}

func (m MockJobClientRunBroken) Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error) {
	return &batchv1.Job{}, nil
}

func (m MockJobClientRunBroken) Get() (*batchv1.Job, error) {
	return nil, errors.New("cannot get job")
}

func (m MockJobClientRunBroken) Watch() (watch.Interface, error) {
	return nil, errors.New("cannot get watcher")
}

func TestJobClientRunFail(t *testing.T) {
	runner := KubernetesRunner{
		jobClient: MockJobClientRunBroken{},
		jobSpec:   &batchv1.JobSpec{},
	}
	completionWatcher, err := runner.Run()
	if err != nil {
		t.Fatalf("Failed to create job")
	}
	if completionWatcher.Wait() == nil {
		t.Fatalf("Failed to trigger error Get()")
	}
	if completionWatcher.Complete() {
		t.Fatalf("Failed to trigger error on Get()")
	}
	if completionWatcher.Err() == nil {
		t.Fatalf("Failed to trigger error on Get()")
	}
	completionWatcher.String()
}

type MockWatch struct{}

var failJob *batchv1.Job = &batchv1.Job{Status: batchv1.JobStatus{Failed: 1}}

func (m MockWatch) Stop() {}
func (m MockWatch) ResultChan() <-chan watch.Event {
	resultChan := make(chan watch.Event, 1)
	resultChan <- watch.Event{Object: failJob}
	return resultChan
}

type MockJobClientFailChannel struct{}

func (m MockJobClientFailChannel) Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error) {
	return &batchv1.Job{}, nil
}

func (m MockJobClientFailChannel) Get() (*batchv1.Job, error) {
	return failJob, nil
}

func (m MockJobClientFailChannel) Watch() (watch.Interface, error) {
	return MockWatch{}, nil
}

func TestPodFailure(t *testing.T) {
	runner := KubernetesRunner{
		jobClient: MockJobClientFailChannel{},
		jobSpec:   &batchv1.JobSpec{},
	}
	completionWatcher, err := runner.Run()
	if err != nil {
		t.Fatalf("Failed to create job")
	}
	if completionWatcher.Wait() == nil {
		t.Fatalf("Failed to read failure job on Wait()")
	}
	if completionWatcher.Complete() {
		t.Fatalf("Failed to read failure job on Complete()")
	}
	if completionWatcher.Err() == nil {
		t.Fatalf("Failed to read failure job on Err()")
	}
	completionWatcher.String()
}
