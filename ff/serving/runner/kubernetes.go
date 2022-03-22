package runner

// import (

// 	"errors"
// 	"context"
// 	"fmt"
// 	"github.com/google/uuid"
// 	"encoding/json"
// 	batchv1 "k8s.io/api/batch/v1"
// 	v1 "k8s.io/api/core/v1"
// 	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
// 	kubernetes "k8s.io/client-go/kubernetes"
// 	rest "k8s.io/client-go/rest"
// )

// // type CompletionStatus interface {
// // 	Complete() bool
// // 	String() string
// // 	Wait() error
// // 	Err() error
// // }

// type KubernetesCompletionStatus struct {
// 	jobManager KubernetesJobManager
// }

// type KubernetesJobConfig struct {
// 	jobName string
// 	nameSpace string
// 	envVars map[string]string
// 	image string
// 	numPods int
// 	numParalell int
// }

// type KubernetesJobManager interface {
// 	Run() error
// 	Get() metav1.Job, error
// 	Completed() bool
// 	Watch() error
// }

// type MockKubernetesJobManager struct {

// }

// func newJob(config KubernetesJobConfig) (KubernetesJobManager, error) {
// 	jobs := k.Clientset.BatchV1().Jobs("default")
// 	containerLabel := "test"
// 	compMode := batchv1.IndexedCompletion
// 	jobID := uuid.New().String()
// 	podNum := int32(k.KubeConfig.NumTasks)

// 	envVars := make([]v1.EnvVar, len(k.KubeConfig.EnvVars)+1)
// 	envVars[0] = v1.EnvVar{Name: "JOB_ID", Value: jobID}
// 	i := 1
// 	for key, element := range k.KubeConfig.EnvVars {
// 		envVars[i] = v1.EnvVar{Name: key, Value: element}
// 		i++
// 	}

// 	jobSpec := &batchv1.Job{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      jobName,
// 			Namespace: "default",
// 		},
// 		Spec: batchv1.JobSpec{
// 			Completions:    &podNum,
// 			Parallelism:    &podNum,
// 			CompletionMode: &compMode,
// 			Template: v1.PodTemplateSpec{
// 				Spec: v1.PodSpec{
// 					Containers: []v1.Container{
// 						{
// 							Name:  containerLabel,
// 							Image: k.KubeConfig.Container,
// 							Env:   envVars,
// 						},
// 					},
// 					RestartPolicy: v1.RestartPolicyNever,
// 				},
// 			},
// 		},
// 	}

// 	_, err := jobs.Create(context.TODO(), jobSpec, metav1.CreateOptions{})
// }

// func (m *MockKubernetesJobManager) Run(config KubernetesJobConfig) error {

// }

// func (m *MockKubernetesJobManager) Get(config KubernetesJobConfig) metav1.Job, error {

// }

// func (m *MockKubernetesJobManager) Completed(config KubernetesJobConfig) bool {

// }

// type KubernetesCompletionWatcher struct {
// 	kubeJobManager KubernetesJobManager
// }

// func (k *KubernetesCompletionWatcher) Complete() bool {

// }

// func (k *KubernetesCompletionWatcher) String() string {

// }

// func (k *KubernetesCompletionWatcher) Wait() error {

// }

// func (k *KubernetesCompletionWatcher) Err() error {

// }

// func (k *KubernetesCompletionWatcher) PercentComplete() float32 {

// }

// type KubernetesRunnerConfig struct {
// 	envVars map[string]string
// 	image string
// 	numTasks int
// }

// type KubernetesRunner struct {
// 	Clientset  *kubernetes.Clientset
// 	KubeConfig KubernetesRunnerConfig
// 	JobName    *string
// }

// func newJobConfig(config KubernetesRunnerConfig) KubernetesJobConfig{
// 	return &KubernetesJobConfig{
// 		jobName: uuid.New().String(),
// 		nameSpace: "default",
// 		envVars: config.envVars,
// 		image: config.image,
// 		numPods: config.numTasks,
// 		numParalell: config.numTasks,
// 	}
// }

// func (k *KubernetesRunner) Run() (CompletionStatus, error) {
	

// 	jobConfig := newJobConfig(k.KubeConfig)

// 	job := newJob(jobConfig)

// 	err := job.Run(); err != nil {
// 		return err
// 	}

// 	return KubernetesCompletionStatus{
// 		jobManager: job
// 	}
// }



// 	jobName := fmt.Sprintf("%d-%s-pods", k.KubeConfig.NumTasks, jobID)

	
// 	if err != nil {
// 		fmt.Println("Failed to create K8s job.", err)
// 		return err

// 	}
// 	k.JobName = &jobName
// 	fmt.Printf("Created K8s job %s successfully\n", jobName)

// 	if k.JobName == nil {
// 		return nil, errors.New("no job created")
// 	}
// 	getOptions := metav1.GetOptions{}
// 	job, err := k.Clientset.BatchV1().Jobs("default").Get(context.TODO(), *k.JobName, getOptions)
// 	if err != nil {
// 		return nil, errors.New("Could not get job")
// 	}
// 	totalTasks := int(job.Status.Succeeded + job.Status.Active + job.Status.Failed)
// 	tasksFailed := int(job.Status.Failed)
// 	completedTasks := int(job.Status.Succeeded)
// 	return &KubernetesCompletionStatus{
// 		TotalTasks:    totalTasks,
// 		TasksComplete: completedTasks,
// 		TasksFailed:   tasksFailed,
// 	}, nil
// 	return nil
// }

// func (k *KubernetesRunner) Config() KubernetesRunnerConfig {
// 	return k.KubeConfig
// }

// func (k *KubernetesRunnerConfig) Serialize() (Config, error) {
// 	serialized, err := json.Marshal(k)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return serialized, nil
// }

// func (k *KubernetesRunnerConfig) Deserialize(config Config) error {
// 	err := json.Unmarshal([]byte(config), k)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// func ConnectToKubernetes() error {

// }

// func NewKubernetesRunner(config KubernetesRunnerConfig) (Runner, error) {

// }
