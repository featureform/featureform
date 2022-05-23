package runner

import (
	"context"
	"fmt"
	"strings"
	"github.com/google/uuid"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	watch "k8s.io/apimachinery/pkg/watch"
	kubernetes "k8s.io/client-go/kubernetes"
	rest "k8s.io/client-go/rest"
)

var namespace string = "default"

type Schedule struct {
	Minute string //0-59 or * for every one
	Hour string //0-23 or *
	Day string //1-31 or *
	Month string //1-12 or *
	Weekday string //0-7 or *
}

func (s Schedule) GetString() string {
	return fmt.Sprintf("%s %s %s %s %s", s.Minute, s.Hour, s.Day, s.Month, s.Weekday)
}

func (s *Schedule) ParseString(schedule string) error {
	parts := strings.Split(schedule, " ")
	if len(parts) != 5 {
		return fmt.Errorf("invalid schedule")
	}
	s.Minute = parts[0]
	s.Hour = parts[1]
	s.Day = parts[2]
	s.Month = parts[3]
	s.Weekday = parts[4]
	return nil
}

type CronRunner interface {
	Runner
	ScheduleJob(schedule Schedule) error
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
	envVars := generateKubernetesEnvVars(config.envVars)
	completionMode := batchv1.IndexedCompletion
	return batchv1.JobSpec{
		Completions:    &config.numTasks,
		Parallelism:    &config.numTasks,
		CompletionMode: &completionMode,
		Template: v1.PodTemplateSpec{
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  containerID,
						Image: config.image,
						Env:   envVars,
					},
				},
				RestartPolicy: v1.RestartPolicyNever,
			},
		},
	}

}

type KubernetesRunnerConfig struct {
	envVars  map[string]string
	image    string
	numTasks int32
}

type JobClient interface {
	Get() (*batchv1.Job, error)
	Watch() (watch.Interface, error)
	Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error)
	SetJobSchedule(schedule Schedule, jobSpec *batchv1.JobSpec) error
	GetJobSchedule(jobName string) (Schedule, error)
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

func (k KubernetesRunner) Run() (CompletionWatcher, error) {
	if _, err := k.jobClient.Create(k.jobSpec); err != nil {
		return nil, err
	}
	return KubernetesCompletionWatcher{jobClient: k.jobClient}, nil
}

func (k KubernetesRunner) ScheduleJob(schedule Schedule) error {
	if err := k.jobClient.SetJobSchedule(schedule, k.jobSpec); err != nil {
		return err
	}
	return nil
}

func NewKubernetesRunner(config KubernetesRunnerConfig) (CronRunner, error) {
	jobSpec := newJobSpec(config)
	jobName := uuid.New().String()
	jobClient, err := NewKubernetesJobClient(jobName, namespace)
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

func (k KubernetesJobClient) Watch() (watch.Interface, error) {
	return k.Clientset.BatchV1().Jobs(k.Namespace).Watch(context.TODO(), metav1.ListOptions{FieldSelector: fmt.Sprintf("metadata.name=%s", k.JobName)})
}

func (k KubernetesJobClient) Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: k.JobName, Namespace: k.Namespace}, Spec: *jobSpec}
	return k.Clientset.BatchV1().Jobs(k.Namespace).Create(context.TODO(), job, metav1.CreateOptions{})
}

func (k KubernetesJobClient) SetJobSchedule(schedule Schedule, jobSpec *batchv1.JobSpec) error {
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.JobName,
			Namespace: k.Namespace},
		Spec: batchv1.CronJobSpec{
			Schedule: schedule.GetString(),
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

func (k KubernetesJobClient) GetJobSchedule(jobName string) (Schedule, error) {

	job, err := k.Clientset.BatchV1().CronJobs(k.Namespace).Get(context.TODO(), jobName, metav1.GetOptions{})
	if err != nil {
		return Schedule{}, err
	}
	scheduleString := job.Spec.Schedule
	formattedSchedule := Schedule{}
	if err := formattedSchedule.ParseString(scheduleString); err != nil {
		return Schedule{}, err
	}
	return formattedSchedule, nil
}

func NewKubernetesJobClient(name string, namespace string) (*KubernetesJobClient, error) {
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
