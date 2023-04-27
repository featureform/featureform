// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package kubernetes

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"strings"

	"github.com/featureform/metadata"
	"github.com/featureform/types"
	"github.com/google/uuid"
	"github.com/gorhill/cronexpr"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	watch "k8s.io/apimachinery/pkg/watch"
	kubernetes "k8s.io/client-go/kubernetes"
	rest "k8s.io/client-go/rest"
)

type CronSchedule string

const MaxNameLength = 53

func GetJobName(id metadata.ResourceID, image string) string {
	resourceName := fmt.Sprintf("%s-%s-%s", id.Type, id.Name, id.Variant)
	if len(resourceName) > MaxNameLength {
		resourceName = resourceName[:MaxNameLength]
	}
	jobName := strings.ReplaceAll(resourceName, "_", ".")
	removedSlashes := strings.ReplaceAll(jobName, "/", "")
	removedColons := strings.ReplaceAll(removedSlashes, ":", "")
	MaxJobSize := 63
	lowerCase := strings.ToLower(removedColons)
	jobNameSize := int(math.Min(float64(len(lowerCase)), float64(MaxJobSize)))
	lowerName := lowerCase[0:jobNameSize]
	return lowerName
}

func GetCronJobName(id metadata.ResourceID) string {
	return strings.ReplaceAll(fmt.Sprintf("featureform-%s-%s-%s-%d", strings.ToLower(string(id.Type)), strings.ToLower(id.Name), strings.ToLower(id.Variant), id.Type), "_", ".")
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
	types.Runner
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

func validateJobLimits(specs metadata.KubernetesResourceSpecs) (v1.ResourceRequirements, error) {
	rsrcReq := v1.ResourceRequirements{
		Requests: make(v1.ResourceList),
		Limits:   make(v1.ResourceList),
	}
	var parseErr error
	if specs.CPURequest != "" {
		qty, err := resource.ParseQuantity(specs.CPURequest)
		rsrcReq.Requests[v1.ResourceCPU] = qty
		parseErr = err
	}
	if specs.CPULimit != "" {
		qty, err := resource.ParseQuantity(specs.CPULimit)
		rsrcReq.Limits[v1.ResourceCPU] = qty
		parseErr = err
	}
	if specs.MemoryRequest != "" {
		qty, err := resource.ParseQuantity(specs.MemoryRequest)
		rsrcReq.Requests[v1.ResourceMemory] = qty
		parseErr = err
	}
	if specs.MemoryLimit != "" {
		qty, err := resource.ParseQuantity(specs.MemoryLimit)
		rsrcReq.Limits[v1.ResourceMemory] = qty
		parseErr = err
	}
	if parseErr != nil {
		return rsrcReq, parseErr
	}
	return rsrcReq, nil
}

func newJobSpec(config KubernetesRunnerConfig, rsrcReqs v1.ResourceRequirements) batchv1.JobSpec {
	containerID := uuid.New().String()
	envVars := generateKubernetesEnvVars(config.EnvVars)
	//only indexed completion if copyRunner
	var completionMode batchv1.CompletionMode
	if config.EnvVars["Name"] == "Copy to online" {
		completionMode = batchv1.IndexedCompletion
	} else {
		completionMode = batchv1.NonIndexedCompletion
	}

	backoffLimit := int32(0)
	ttlLimit := int32(3600)
	return batchv1.JobSpec{
		Completions:             &config.NumTasks,
		Parallelism:             &config.NumTasks,
		CompletionMode:          &completionMode,
		BackoffLimit:            &backoffLimit,
		TTLSecondsAfterFinished: &ttlLimit,
		Template: v1.PodTemplateSpec{
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:            containerID,
						Image:           config.Image,
						Env:             envVars,
						ImagePullPolicy: v1.PullIfNotPresent,
						Resources:       rsrcReqs,
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
	Specs    metadata.KubernetesResourceSpecs
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

func getPodLogs(namespace string, name string) string {
	podLogOpts := corev1.PodLogOptions{}
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Sprintf("error in getting config, %s", err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Sprintf("error in getting access to K8S: %s", err.Error())
	}
	podList, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Sprintf("could not get pod list: %s", err.Error())
	}
	podName := ""
	for _, pod := range podList.Items {
		currentPod := pod.GetName()
		if strings.Contains(currentPod, name) {
			podName = currentPod
		}
	}
	if podName == "" {
		return fmt.Sprintf("pod not found: %s", name)
	}
	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, &podLogOpts)
	podLogs, err := req.Stream(context.Background())
	if err != nil {
		return fmt.Sprintf("error in opening stream: %s", err.Error())
	}
	defer podLogs.Close()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, podLogs)
	if err != nil {
		return fmt.Sprintf("error in copy information from podLogs to buf: %s", err.Error())
	}
	str := buf.String()

	return str
}

func (k KubernetesCompletionWatcher) Wait() error {
	watcher, err := k.jobClient.Watch()
	if err != nil {
		return err
	}
	watchChannel := watcher.ResultChan()
	for jobEvent := range watchChannel {

		job := jobEvent.Object.(*batchv1.Job)
		if active := job.Status.Active; active == 0 {
			if succeeded := job.Status.Succeeded; succeeded > 0 {
				return nil
			}
			if failed := job.Status.Failed; failed > 0 {
				return fmt.Errorf("job failed while running: container: %s: error: %s",
					job.Name, getPodLogs(job.Namespace, job.GetName()))
			}
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
		return fmt.Errorf("job failed while running: container: %s: %w", job.Name, err)
	}
	return nil
}

func (k KubernetesRunner) Resource() metadata.ResourceID {
	return metadata.ResourceID{}
}

func (k KubernetesRunner) IsUpdateJob() bool {
	return false
}

func (k KubernetesRunner) Run() (types.CompletionWatcher, error) {
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

func GetCurrentNamespace() (string, error) {
	contents, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "", err
	}
	return string(contents), nil
}

func generateCleanRandomJobName() string {
	cleanUUID := strings.ReplaceAll(uuid.New().String(), "-", "")
	jobName := fmt.Sprintf("job__%s", cleanUUID)
	return jobName[0:int(math.Min(float64(len(jobName)), 63))]
}

func NewKubernetesRunner(config KubernetesRunnerConfig) (CronRunner, error) {
	rsrcReqs, err := validateJobLimits(config.Specs)
	if err != nil {
		return nil, err
	}
	jobSpec := newJobSpec(config, rsrcReqs)
	var jobName string
	if config.Resource.Name != "" {
		jobName = GetJobName(config.Resource, config.Image)
	} else {
		jobName = generateCleanRandomJobName()
	}
	namespace, err := GetCurrentNamespace()
	if err != nil {
		return nil, err
	}
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
			Namespace: k.Namespace,
		},
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
