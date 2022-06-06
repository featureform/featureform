// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"context"
	"fmt"
	"github.com/featureform/metadata"
	"github.com/google/uuid"
	"github.com/gorhill/cronexpr"
	batchv1 "k8s.io/api/batch/v1"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	watch "k8s.io/apimachinery/pkg/watch"
	kubernetes "k8s.io/client-go/kubernetes"
	rest "k8s.io/client-go/rest"
)

var Namespace string = "default"

type CronSchedule string

func GetJobName(id metadata.ResourceID) string {
	return strings.ReplaceAll(fmt.Sprintf("%s-%s-%d", strings.ToLower(id.Name), strings.ToLower(id.Variant), id.Type), "_", ".")

}

func GetCronJobName(id metadata.ResourceID) string {
	return strings.ReplaceAll(fmt.Sprintf("%s-%s-%d", strings.ToLower(id.Name), strings.ToLower(id.Variant), id.Type), "_", ".")
}

func makeCronSchedule(schedule string) (*CronSchedule, error) {
	if _, err := cronexpr.Parse(schedule); err != nil {
		return nil, fmt.Errorf("invalid cron expression: %v", err)
	}
	cronSchedule := CronSchedule(schedule)
	return &cronSchedule, nil
}

func MonthlySchedule(day, minute, hour int) (*CronSchedule, error) {
	return makeCronSchedule(fmt.Sprintf("%d %d %d * *", day, minute, hour))
}

func WeeklySchedule(weekday, hour, minute int) (*CronSchedule, error) {
	return makeCronSchedule(fmt.Sprintf("%d %d * * %d", weekday, hour, minute))
}

func DailySchedule(hour, minute int) (*CronSchedule, error) {
	return makeCronSchedule(fmt.Sprintf("%d %d * * *", hour, minute))
}

func HourlySchedule(minute int) (*CronSchedule, error) {
	return makeCronSchedule(fmt.Sprintf("%d * * * *", minute))
}

func EveryNMinutes(minutes int) (*CronSchedule, error) {
	return makeCronSchedule(fmt.Sprintf("*/%d * * * *", minutes))
}

func EveryNHours(hours int) (*CronSchedule, error) {
	return makeCronSchedule(fmt.Sprintf("* */%d * * *", hours))
}

func EveryNDays(days int) (*CronSchedule, error) {
	return makeCronSchedule(fmt.Sprintf("* * */%d * *", days))

}

func EveryNMonths(months int) (*CronSchedule, error) {
	return makeCronSchedule(fmt.Sprintf("* * * */%d *", months))
}

type CronRunner interface {
	Runner
	ScheduleJob(schedule CronSchedule) error
}

func generateKubernetesEnvVars(envVars map[string]string) []v1.EnvVar {
	kubeEnvVars := make([]v1.EnvVar, len(envVars))
	i := 0
	for key, element := range envVars {
		kubeEnvVars[i] = v1.EnvVar{Name: key, Value: element}
		i++
	}
	return kubeEnvVars
}

func newJobSpec(config KubernetesRunnerConfig) batchv1.JobSpec {
	containerID := uuid.New().String()
	envVars := generateKubernetesEnvVars(config.EnvVars)
	//only indexed completion if copyRunner
	var completionMode batchv1.CompletionMode
	if config.EnvVars["Name"] == "Copy to online" {
		completionMode = batchv1.IndexedCompletion
	} else {
		completionMode = batchv1.NonIndexedCompletion
	}
	return batchv1.JobSpec{
		Completions:    &config.NumTasks,
		Parallelism:    &config.NumTasks,
		CompletionMode: &completionMode,
		Template: v1.PodTemplateSpec{
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  containerID,
						Image: config.Image,
						Env:   envVars,
					},
				},
				RestartPolicy: v1.RestartPolicyNever,
			},
		},
	}

}

type KubernetesRunnerConfig struct {
	EnvVars  map[string]string
	Resource metadata.ResourceID
	Image    string
	NumTasks int32
}

type JobClient interface {
	Get() (*batchv1.Job, error)
	GetCronJob() (*batchv1.CronJob, error)
	UpdateCronJob(cronJob *batchv1.CronJob) (*batchv1.CronJob, error)
	Watch() (watch.Interface, error)
	Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error)
	SetJobSchedule(schedule CronSchedule, jobSpec *batchv1.JobSpec) error
	GetJobSchedule(jobName string) (CronSchedule, error)
}

type KubernetesRunner struct {
	jobClient JobClient
	jobSpec   *batchv1.JobSpec
}

type KubernetesCompletionWatcher struct {
	jobClient JobClient
}

func (k KubernetesCompletionWatcher) Complete() bool {
	job, err := k.jobClient.Get()
	if err != nil {
		return false
	}
	if job.Status.Active == 0 && job.Status.Failed == 0 {
		return true
	}
	return false
}

func (k KubernetesCompletionWatcher) String() string {
	job, err := k.jobClient.Get()
	if err != nil {
		return "Could not fetch job."
	}
	return fmt.Sprintf("%d jobs succeeded. %d jobs active. %d jobs failed", job.Status.Succeeded, job.Status.Active, job.Status.Failed)
}

func (k KubernetesCompletionWatcher) Wait() error {
	watcher, err := k.jobClient.Watch()
	if err != nil {
		return err
	}
	watchChannel := watcher.ResultChan()
	for jobEvent := range watchChannel {
		if failed := jobEvent.Object.(*batchv1.Job).Status.Failed; failed > 0 {
			return fmt.Errorf("job failed while running")
		}

	}
	return nil
}

func (k KubernetesCompletionWatcher) Err() error {
	job, err := k.jobClient.Get()
	if err != nil {
		return err
	}
	if job.Status.Failed > 0 {
		return fmt.Errorf("job failed while running")
	}
	return nil
}

func (k KubernetesRunner) Resource() metadata.ResourceID {
	return metadata.ResourceID{}
}

func (k KubernetesRunner) IsUpdateJob() bool {
	return false
}

func (k KubernetesRunner) Run() (CompletionWatcher, error) {
	if _, err := k.jobClient.Create(k.jobSpec); err != nil {
		return nil, err
	}
	return KubernetesCompletionWatcher{jobClient: k.jobClient}, nil
}

func (k KubernetesRunner) ScheduleJob(schedule CronSchedule) error {
	if err := k.jobClient.SetJobSchedule(schedule, k.jobSpec); err != nil {
		return err
	}
	return nil
}

func NewKubernetesRunner(config KubernetesRunnerConfig) (CronRunner, error) {
	jobSpec := newJobSpec(config)
	jobName := GetJobName(config.Resource)
	jobClient, err := NewKubernetesJobClient(jobName, Namespace)
	if err != nil {
		return nil, err
	}
	return KubernetesRunner{
		jobClient: jobClient,
		jobSpec:   &jobSpec,
	}, nil
}

type KubernetesJobClient struct {
	Clientset *kubernetes.Clientset
	JobName   string
	Namespace string
}

func (k KubernetesJobClient) Get() (*batchv1.Job, error) {
	return k.Clientset.BatchV1().Jobs(k.Namespace).Get(context.TODO(), k.JobName, metav1.GetOptions{})
}

func (k KubernetesJobClient) GetCronJob() (*batchv1.CronJob, error) {
	return k.Clientset.BatchV1().CronJobs(k.Namespace).Get(context.TODO(), k.JobName, metav1.GetOptions{})
}

func (k KubernetesJobClient) UpdateCronJob(cronJob *batchv1.CronJob) (*batchv1.CronJob, error) {
	return k.Clientset.BatchV1().CronJobs(k.Namespace).Update(context.TODO(), cronJob, metav1.UpdateOptions{})
}

func (k KubernetesJobClient) Watch() (watch.Interface, error) {
	return k.Clientset.BatchV1().Jobs(k.Namespace).Watch(context.TODO(), metav1.ListOptions{FieldSelector: fmt.Sprintf("metadata.name=%s", k.JobName)})
}

func (k KubernetesJobClient) Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: k.JobName, Namespace: k.Namespace}, Spec: *jobSpec}
	return k.Clientset.BatchV1().Jobs(k.Namespace).Create(context.TODO(), job, metav1.CreateOptions{})
}

func (k KubernetesJobClient) SetJobSchedule(schedule CronSchedule, jobSpec *batchv1.JobSpec) error {
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.JobName,
			Namespace: k.Namespace},
		Spec: batchv1.CronJobSpec{
			Schedule: string(schedule),
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: *jobSpec,
			},
		},
	}
	if _, err := k.Clientset.BatchV1().CronJobs(k.Namespace).Create(context.TODO(), cronJob, metav1.CreateOptions{}); err != nil {
		return err
	}
	return nil
}

func (k KubernetesJobClient) UpdateJobSchedule(schedule CronSchedule, jobSpec *batchv1.JobSpec) error {
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.JobName,
			Namespace: k.Namespace},
		Spec: batchv1.CronJobSpec{
			Schedule: string(schedule),
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: *jobSpec,
			},
		},
	}
	if _, err := k.Clientset.BatchV1().CronJobs(k.Namespace).Update(context.TODO(), cronJob, metav1.UpdateOptions{}); err != nil {
		return err
	}
	return nil
}

func (k KubernetesJobClient) GetJobSchedule(jobName string) (CronSchedule, error) {

	job, err := k.Clientset.BatchV1().CronJobs(k.Namespace).Get(context.TODO(), jobName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	return CronSchedule(job.Spec.Schedule), nil
}

func NewKubernetesJobClient(name string, namespace string) (*KubernetesJobClient, error) {
	fmt.Println("kubernetes client is got", name, namespace)
	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return nil, err
	}
	return &KubernetesJobClient{Clientset: clientset, JobName: name, Namespace: namespace}, nil
}
