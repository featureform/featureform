// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"errors"
	"github.com/google/uuid"
	batchv1 "k8s.io/api/batch/v1"
	watch "k8s.io/apimachinery/pkg/watch"
	"testing"
)

func NewMockKubernetesRunner(config KubernetesRunnerConfig) (CronRunner, error) {
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

func (m MockJobClient) SetJobSchedule(schedule CronSchedule, jobSpec *batchv1.JobSpec) error {
	return nil
}

func (m MockJobClient) GetJobSchedule(jobName string) (CronSchedule, error) {
	return CronSchedule("* * * * *"), nil
}

func TestKubernetesRunnerCreate(t *testing.T) {
	runner, err := NewMockKubernetesRunner(KubernetesRunnerConfig{EnvVars: map[string]string{"test": "envVar"}, Image: "test", NumTasks: 1})
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

func (m MockJobClientRunBroken) Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error) {
	return &batchv1.Job{}, nil
}

func (m MockJobClientRunBroken) Get() (*batchv1.Job, error) {
	return nil, errors.New("cannot get job")
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

func (m MockJobClientFailChannel) Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error) {
	return &batchv1.Job{}, nil
}

func (m MockJobClientFailChannel) Get() (*batchv1.Job, error) {
	return failJob, nil
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
	runner, err := NewMockKubernetesRunner(KubernetesRunnerConfig{EnvVars: map[string]string{"test": "envVar"}, Image: "test", NumTasks: 1})
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

func TestMonthlySchedule(t *testing.T) {
	schedule, err := MonthlySchedule(1, 2, 3)
	if err != nil {
		t.Fatalf("triggered error on proper cron job parameters")
	}
	if *schedule != CronSchedule("1 2 3 * *") {
		t.Fatalf("Failed to create proper monthly cron schedule")
	}
	if _, err := MonthlySchedule(99, 99, 99); err == nil {
		t.Fatalf("Failed to trigger error on invalid monthly schedule")
	}
}

func TestWeeklySchedule(t *testing.T) {
	schedule, err := WeeklySchedule(1, 2, 3)
	if err != nil {
		t.Fatalf("triggered error on proper cron job parameters")
	}
	if *schedule != CronSchedule("1 2 * * 3") {
		t.Fatalf("Failed to create proper weekly cron schedule")
	}
	if _, err := WeeklySchedule(99, 99, 99); err == nil {
		t.Fatalf("Failed to trigger error on invalid weekly schedule")
	}
}

func TestDailySchedule(t *testing.T) {
	schedule, err := DailySchedule(1, 2)
	if err != nil {
		t.Fatalf("triggered error on proper cron job parameters")
	}
	if *schedule != CronSchedule("1 2 * * *") {
		t.Fatalf("Failed to create proper daily cron schedule")
	}
	if _, err := DailySchedule(99, 99); err == nil {
		t.Fatalf("Failed to trigger error on invalid daily schedule")
	}
}

func TestHourlySchedule(t *testing.T) {
	schedule, err := HourlySchedule(1)
	if err != nil {
		t.Fatalf("triggered error on proper cron job parameters")
	}
	if *schedule != CronSchedule("1 * * * *") {
		t.Fatalf("Failed to create proper hourly cron schedule")
	}
	if _, err := HourlySchedule(99); err == nil {
		t.Fatalf("Failed to trigger error on invalid hourly schedule")
	}
}

func TestEveryNMinutes(t *testing.T) {
	schedule, err := EveryNMinutes(1)
	if err != nil {
		t.Fatalf("triggered error on proper cron job parameters")
	}
	if *schedule != CronSchedule("*/1 * * * *") {
		t.Fatalf("Failed to create proper every n minutes cron schedule")
	}
	if _, err := EveryNMinutes(99); err == nil {
		t.Fatalf("Failed to trigger error on invalid every n minutes schedule")
	}
}

func TestEveryNHours(t *testing.T) {
	schedule, err := EveryNHours(1)
	if err != nil {
		t.Fatalf("triggered error on proper cron job parameters")
	}
	if *schedule != CronSchedule("* */1 * * *") {
		t.Fatalf("Failed to create proper every n minutes cron schedule")
	}
	if _, err := EveryNHours(99); err == nil {
		t.Fatalf("Failed to trigger error on invalid every n hours schedule")
	}
}

func TestEveryNDays(t *testing.T) {
	schedule, err := EveryNDays(1)
	if err != nil {
		t.Fatalf("triggered error on proper cron job parameters")
	}
	if *schedule != CronSchedule("* * */1 * *") {
		t.Fatalf("Failed to create proper every n minutes cron schedule")
	}
	if _, err := EveryNDays(99); err == nil {
		t.Fatalf("Failed to trigger error on invalid every n days schedule")
	}
}
