package runner

import (
	"k8s.io/client-go/kubernetes"
)

type JobClient interface {
	GetJob(name string) (Job, error)
	WatchJob(name string) (watch.Interface, error)
	CreateJob(name string, config JobConfig) error
}

type CronJobClient interface {
	ScheduleJob(name string, schedule string, config JobConfig) error
}

type KubernetesClient struct {
	Clientset kubernetes.Interface
	Namespace string
}

func (k KubernetesClient) GetJob(name string) (Job, error) {
	
}

func (k KubernetesClient) WatchJob(name string) (watch.Interface, error) {
	
}

func (k KubernetesClient) CreateJob(name string, config JobConfig) (Job, error) {
	

}

func (k KubernetesClient) ScheduleJob(name string) (Job, error) {
	
}

type JobConfig interface {
	Image() string
	EnvVars() map[string]string
	NumTasks() int
}

type KubernetesJobConfig struct {
	image string
	envVars map[string]string
	numTasks int
}

func (k KubernetesJobConfig) Image() string {
	return k.
}

type Job interface {
	Name() string
	Config() JobConfig
	Status() bool
}

type KubernetesJob struct {
	*batchv1.Job
}

func (k KubernetesJob) Name() string {
	return k.ObjectMeta.Name
}
func (k KubernetesJob) Config() JobConfig {
	return KubernetesJobConfig{k.JobSpec}
}
func (k KubernetesJob) Status() bool {
	return k.JobStatus.Succeeded == int(*k.Spec.Completions)
}


func (k *KubernetesJob) AsJob() (Job, error) {
	return k, nil
}

func (k *KubernetesJobConfig) AsJobConfig() (JobConfig)