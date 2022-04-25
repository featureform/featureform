// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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
	completionMode := batchv1.IndexedCompletion
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
	Image    string
	NumTasks int32
}

type JobClient interface {
	Get() (*batchv1.Job, error)
	Watch() (watch.Interface, error)
	Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error)
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

func NewKubernetesRunner(config KubernetesRunnerConfig) (Runner, error) {
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
