// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package kubernetes

import (
	"errors"
	"github.com/featureform/metadata"
	"github.com/google/uuid"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	watch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"reflect"
	"testing"
)

func NewMockKubernetesRunner(config KubernetesRunnerConfig) (CronRunner, error) {
	rsrcReqs, err := validateJobLimits(config.Specs)
	if err != nil {
		return nil, err
	}
	jobSpec := newJobSpec(config, rsrcReqs)
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

func (m MockJobClient) GetJobName() string {
	return m.JobName
}

func (m MockJobClient) Get() (*batchv1.Job, error) {
	return &batchv1.Job{}, nil
}

func (m MockJobClient) GetCronJob() (*batchv1.CronJob, error) {
	return &batchv1.CronJob{}, nil
}

func (m MockJobClient) UpdateCronJob(cronJob *batchv1.CronJob) (*batchv1.CronJob, error) {
	return &batchv1.CronJob{}, nil
}

func (m MockJobClient) Watch() (watch.Interface, error) {
	return watch.NewEmptyWatch(), nil
}

func (m MockJobClient) Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error) {
	return &batchv1.Job{}, nil
}

func (m MockJobClient) SetJobSchedule(schedule CronSchedule, jobSpec *batchv1.JobSpec) error {
	return nil
}

func (m MockJobClient) GetJobSchedule(jobName string) (CronSchedule, error) {
	return CronSchedule("* * * * *"), nil
}

func TestKubernetesRunnerCreate(t *testing.T) {
	runner, err := NewMockKubernetesRunner(KubernetesRunnerConfig{EnvVars: map[string]string{"test": "envVar"}, JobPrefix: "", Image: "test", NumTasks: 1})
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

func (m MockJobClientBroken) GetJobName() string {
	return ""
}

func (m MockJobClientBroken) Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error) {
	return nil, errors.New("cannot create job")
}

func (m MockJobClientBroken) Get() (*batchv1.Job, error) {
	return nil, errors.New("cannot get job")
}

func (m MockJobClientBroken) GetCronJob() (*batchv1.CronJob, error) {
	return &batchv1.CronJob{}, nil
}

func (m MockJobClientBroken) UpdateCronJob(cronJob *batchv1.CronJob) (*batchv1.CronJob, error) {
	return &batchv1.CronJob{}, nil
}

func (m MockJobClientBroken) Watch() (watch.Interface, error) {
	return nil, errors.New("cannot get watcher")
}

func (m MockJobClientBroken) SetJobSchedule(schedule CronSchedule, jobSpec *batchv1.JobSpec) error {
	return errors.New("cannot schedule job")
}

func (m MockJobClientBroken) GetJobSchedule(jobName string) (CronSchedule, error) {
	return CronSchedule(""), errors.New("cannot get job schedule")
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

func (m MockJobClientRunBroken) GetJobName() string {
	return ""
}

func (m MockJobClientRunBroken) Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error) {
	return &batchv1.Job{}, nil
}

func (m MockJobClientRunBroken) Get() (*batchv1.Job, error) {
	return nil, errors.New("cannot get job")
}

func (m MockJobClientRunBroken) GetCronJob() (*batchv1.CronJob, error) {
	return &batchv1.CronJob{}, nil
}

func (m MockJobClientRunBroken) UpdateCronJob(cronJob *batchv1.CronJob) (*batchv1.CronJob, error) {
	return &batchv1.CronJob{}, nil
}

func (m MockJobClientRunBroken) Watch() (watch.Interface, error) {
	return nil, errors.New("cannot get watcher")
}

func (m MockJobClientRunBroken) SetJobSchedule(schedule CronSchedule, jobSpec *batchv1.JobSpec) error {
	return errors.New("cannot set job schedule")
}

func (m MockJobClientRunBroken) GetJobSchedule(jobName string) (CronSchedule, error) {
	return CronSchedule(""), errors.New("cannot get job schedule")
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

func (m MockJobClientFailChannel) GetJobName() string {
	return ""
}

func (m MockJobClientFailChannel) Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error) {
	return &batchv1.Job{}, nil
}

func (m MockJobClientFailChannel) Get() (*batchv1.Job, error) {
	return failJob, nil
}

func (m MockJobClientFailChannel) GetCronJob() (*batchv1.CronJob, error) {
	return &batchv1.CronJob{}, nil
}

func (m MockJobClientFailChannel) UpdateCronJob(cronJob *batchv1.CronJob) (*batchv1.CronJob, error) {
	return &batchv1.CronJob{}, nil
}

func (m MockJobClientFailChannel) Watch() (watch.Interface, error) {
	return MockWatch{}, nil
}

func (m MockJobClientFailChannel) SetJobSchedule(schedule CronSchedule, jobSpec *batchv1.JobSpec) error {
	return nil
}

func (m MockJobClientFailChannel) GetJobSchedule(jobName string) (CronSchedule, error) {
	return CronSchedule(""), nil
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

func TestKubernetesRunnerSchedule(t *testing.T) {
	runner, err := NewMockKubernetesRunner(KubernetesRunnerConfig{EnvVars: map[string]string{"test": "envVar"}, JobPrefix: "", Image: "test", NumTasks: 1})
	if err != nil {
		t.Fatalf("Failed to create Kubernetes runner")
	}
	schedule := CronSchedule("* * * * *")
	if err = runner.ScheduleJob(schedule); err != nil {
		t.Fatalf("Failed to schedule kubernetes job")
	}
}

func TestKubernetesRunnerScheduleFail(t *testing.T) {
	runner := KubernetesRunner{
		jobClient: MockJobClientBroken{},
		jobSpec:   &batchv1.JobSpec{},
	}
	schedule := CronSchedule("* * * * *")
	if err := runner.ScheduleJob(schedule); err == nil {
		t.Fatalf("Failed to report error scheduling Kubernetes job")
	}
}

func Test_validateJobLimits(t *testing.T) {
	type args struct {
		specs metadata.KubernetesResourceSpecs
	}
	tests := []struct {
		name    string
		args    args
		want    v1.ResourceRequirements
		wantErr bool
	}{
		{
			name: "CPU Request",
			args: args{
				metadata.KubernetesResourceSpecs{
					CPURequest: "1",
				},
			},
			want: func() v1.ResourceRequirements {
				rsrcReq := v1.ResourceRequirements{
					Requests: make(v1.ResourceList),
					Limits:   make(v1.ResourceList),
				}
				rsrcReq.Requests[v1.ResourceCPU], _ = resource.ParseQuantity("1")
				return rsrcReq
			}(),
			wantErr: false,
		},
		{
			name: "CPU Limit",
			args: args{
				metadata.KubernetesResourceSpecs{
					CPULimit: "1",
				},
			},
			want: func() v1.ResourceRequirements {
				rsrcReq := v1.ResourceRequirements{
					Requests: make(v1.ResourceList),
					Limits:   make(v1.ResourceList),
				}
				rsrcReq.Limits[v1.ResourceCPU], _ = resource.ParseQuantity("1")
				return rsrcReq
			}(),
			wantErr: false,
		},
		{
			name: "Memory Request",
			args: args{
				metadata.KubernetesResourceSpecs{
					MemoryRequest: "1",
				},
			},
			want: func() v1.ResourceRequirements {
				rsrcReq := v1.ResourceRequirements{
					Requests: make(v1.ResourceList),
					Limits:   make(v1.ResourceList),
				}
				rsrcReq.Requests[v1.ResourceMemory], _ = resource.ParseQuantity("1")
				return rsrcReq
			}(),
			wantErr: false,
		},
		{
			name: "Memory Limit",
			args: args{
				metadata.KubernetesResourceSpecs{
					MemoryLimit: "1",
				},
			},
			want: func() v1.ResourceRequirements {
				rsrcReq := v1.ResourceRequirements{
					Requests: make(v1.ResourceList),
					Limits:   make(v1.ResourceList),
				}
				rsrcReq.Limits[v1.ResourceMemory], _ = resource.ParseQuantity("1")
				return rsrcReq
			}(),
			wantErr: false,
		},
		{
			name: "Memory Limit Fail",
			args: args{
				metadata.KubernetesResourceSpecs{
					MemoryLimit: "abc",
				},
			},
			want: func() v1.ResourceRequirements {
				rsrcReq := v1.ResourceRequirements{
					Requests: make(v1.ResourceList),
					Limits:   make(v1.ResourceList),
				}
				rsrcReq.Limits[v1.ResourceMemory], _ = resource.ParseQuantity("1")
				return rsrcReq
			}(),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validateJobLimits(tt.args.specs)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateJobLimits() error = %v, wantErr %v", err, tt.wantErr)
				return
			} else {
				if err != nil {
					return
				}
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("validateJobLimits() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKubernetesJobClient_GetJobName(t *testing.T) {
	type fields struct {
		Clientset *kubernetes.Clientset
		JobName   string
		Namespace string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"Correct name", fields{&kubernetes.Clientset{}, "test", "test"}, "test"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := KubernetesJobClient{
				Clientset: tt.fields.Clientset,
				JobName:   tt.fields.JobName,
				Namespace: tt.fields.Namespace,
			}
			if got := k.GetJobName(); got != tt.want {
				t.Errorf("GetJobName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKubernetesJobClient_getCronJobName(t *testing.T) {
	type fields struct {
		Clientset *kubernetes.Clientset
		JobName   string
		Namespace string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"Correct name", fields{&kubernetes.Clientset{}, "test", "test"}, "cron-test"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := KubernetesJobClient{
				Clientset: tt.fields.Clientset,
				JobName:   tt.fields.JobName,
				Namespace: tt.fields.Namespace,
			}
			if got := k.getCronJobName(); got != tt.want {
				t.Errorf("getCronJobName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewKubernetesJobClient(t *testing.T) {
	type args struct {
		name      string
		namespace string
	}
	tests := []struct {
		name    string
		args    args
		want    *KubernetesJobClient
		wantErr bool
	}{
		{"Expect Fail Not In Cluster", args{"test", "test"}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewKubernetesJobClient(tt.args.name, tt.args.namespace)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewKubernetesJobClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewKubernetesJobClient() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getConcurrencyPolicy(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  batchv1.ConcurrencyPolicy
	}{
		{"Allow", "Allow", batchv1.AllowConcurrent},
		{"Forbid", "Forbid", batchv1.ForbidConcurrent},
		{"Replace", "Replace", batchv1.ReplaceConcurrent},
		{"Invalid", "Invalid", batchv1.AllowConcurrent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getConcurrencyPolicy(tt.value); got != tt.want {
				t.Errorf("getConcurrencyPolicy() = %v, want %v", got, tt.want)
			}
		})
	}
}
