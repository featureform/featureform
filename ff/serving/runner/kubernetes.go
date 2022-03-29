package runner

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	watch "k8s.io/apimachinery/pkg/watch"
	kubernetes "k8s.io/client-go/kubernetes"
	rest "k8s.io/client-go/rest"
)

var namespace string = "default"

type CronRunner interface {
	Runner
	Schedule(schedule string) error
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
	Get(name string) (*batchv1.Job, error)
	Watch(name string) (watch.Interface, error)
	Create(name string, jobSpec *batchv1.JobSpec) error
	Schedule(name string, schedule string, jobSpec *batchv1.JobSpec) error
}

type KubernetesRunner struct {
	jobClient JobClient
	name string
	jobSpec   *batchv1.JobSpec
}

type KubernetesCompletionWatcher struct {
	jobClient JobClient
	name string
}

func (k KubernetesCompletionWatcher) Complete() bool {
	job, err := k.jobClient.Get(k.name)
	if err != nil {
		return false
	}
	if job.Status.Active == 0 && job.Status.Failed == 0 {
		return true
	}
	return false
}

func (k KubernetesCompletionWatcher) String() string {
	job, err := k.jobClient.Get(k.name)
	if err != nil {
		return "Could not fetch job."
	}
	return fmt.Sprintf("%d jobs succeeded. %d jobs active. %d jobs failed", job.Status.Succeeded, job.Status.Active, job.Status.Failed)
}

func (k KubernetesCompletionWatcher) Wait() error {
	watcher, err := k.jobClient.Watch(k.name)
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
	job, err := k.jobClient.Get(k.name)
	if err != nil {
		return err
	}
	if job.Status.Failed > 0 {
		return fmt.Errorf("job failed while running")
	}
	return nil
}

func (k KubernetesRunner) Run() (CompletionWatcher, error) {
	if err := k.jobClient.Create(k.name, k.jobSpec); err != nil {
		return nil, err
	}
	return KubernetesCompletionWatcher{jobClient: k.jobClient, name: k.name}, nil
}

func (k KubernetesRunner) Schedule(schedule string) error {
	if err := k.jobClient.Schedule(k.name, schedule, k.jobSpec); err != nil {
		return err
	}
	return nil
}

func NewKubernetesRunner(config KubernetesRunnerConfig) (CronRunner, error) {
	jobSpec := newJobSpec(config)
	jobName := uuid.New().String()
	jobClient, err := NewKubernetesJobClient(namespace)
	if err != nil {
		return nil, err
	}
	return KubernetesRunner{
		jobClient: jobClient,
		name: jobName,
		jobSpec:   &jobSpec,
	}, nil
}

type KubernetesJobClient struct {
	Clientset *kubernetes.Clientset
	Namespace string
}

func (k KubernetesJobClient) Get(name string) (*batchv1.Job, error) {
	return k.Clientset.BatchV1().Jobs(k.Namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

func (k KubernetesJobClient) Watch(name string) (watch.Interface, error) {
	return k.Clientset.BatchV1().Jobs(k.Namespace).Watch(context.TODO(), metav1.ListOptions{FieldSelector: fmt.Sprintf("metadata.name=%s", name)})
}

func (k KubernetesJobClient) Create(name string, jobSpec *batchv1.JobSpec) error {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: k.Namespace}, Spec: *jobSpec}
	if _, err := k.Clientset.BatchV1().Jobs(k.Namespace).Create(context.TODO(), job, metav1.CreateOptions{}); err != nil {
		return err
	}
	return nil
}

func (k KubernetesJobClient) Schedule(name string, schedule string, jobSpec *batchv1.JobSpec) error {
	cronJobSpec := batchv1.CronJobSpec{
		Schedule: schedule,
		JobTemplate: batchv1.JobTemplateSpec{
			Spec: *jobSpec,
		},
	}
	cronJob := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: k.Namespace}, Spec: cronJobSpec}
	_, err := k.Clientset.BatchV1().CronJobs(k.Namespace).Create(context.TODO(), cronJob, metav1.CreateOptions{})
	return err
}

func NewKubernetesJobClient(namespace string) (*KubernetesJobClient, error) {
	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return nil, err
	}
	return &KubernetesJobClient{Clientset: clientset, Namespace: namespace}, nil
}
