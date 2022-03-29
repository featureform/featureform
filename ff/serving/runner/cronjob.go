package runner

import (
	batchv1 "k8s.io/api/batch/v1"
	kubernetes "k8s.io/client-go/kubernetes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	rest "k8s.io/client-go/rest"
	"context"
	"fmt"
)

type CronJobClient interface {
	JobClient
	GetScheduledJob(name string) (CronJob, error)
	ScheduleJob(schedule string, ) error
	UnscheduleJob(name string) error
	ListSchedules() ([]CronJob, error)
}

func (k KubernetesJobClient) GetScheduledJob(name string) (CronJob, error) {
	job, err := k.Clientset.BatchV1().CronJobs(k.Namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return KubernetesCronJob{job}, nil
}

func (k KubernetesJobClient) ScheduleJob(name string, schedule string, jobSpec *batchv1.JobSpec) error {
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

func (k KubernetesJobClient) UnscheduleJob(id string) error {
	err := k.Clientset.BatchV1().CronJobs(k.Namespace).Delete(context.TODO(), string(id), metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (k KubernetesJobClient) ListSchedules() ([]CronJob, error) {
	kubeCronJobs, err := k.Clientset.BatchV1().CronJobs(k.Namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	cronJobs := make([]CronJob, 0, len(kubeCronJobs.Items))
	for _, job := range kubeCronJobs.Items {
		var c CronJob = KubernetesCronJob{&job}
		cronJobs = append(cronJobs, c)
	}
	return cronJobs, nil
}

type CronJob interface {
	Config() CronJobConfig
	String() string
}

type KubernetesCronJobConfig struct {
	schedule string
	KubernetesJob *batchv1.JobSpec //can be generated from newJobSpec in kubernetes.go
}

type KubernetesCronJob struct {
	*batchv1.CronJob
}

func (k KubernetesCronJob) ID() string {
	return k.ObjectMeta.Name
}

func (k KubernetesCronJob) Config() CronJobConfig {
	return &KubernetesCronJobConfig{name: k.ID(), schedule: k.Spec.Schedule, KubernetesJob: &k.Spec.JobTemplate.Spec}
}

func (k KubernetesCronJob) String() string {
	return fmt.Sprintf("Job: %s, Schedule: %s, Last Run: %v", k.ID(), k.Spec.Schedule, k.Status.LastSuccessfulTime)
}


func newKubernetesCronJobClient(namespace string) (*KubernetesCronJobClient, error) {
	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return nil, err
	}
	return &KubernetesCronJobClient{Clientset: clientset, Namespace: namespace}, nil
}


func newKubernetesCronJobSpec(config KubernetesCronJobConfig) batchv1.CronJobSpec {
	return batchv1.CronJobSpec{
		Schedule: config.schedule,
		JobTemplate: batchv1.JobTemplateSpec{
			Spec: *config.KubernetesJob,
		},
	}
}
