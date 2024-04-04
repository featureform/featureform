package spawner

import (
	"github.com/featureform/metadata"
	"github.com/featureform/runner"
	"github.com/featureform/types"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type KubernetesJobSpawner struct {
	EtcdConfig clientv3.Config
}

func (k *KubernetesJobSpawner) GetJobRunner(jobName runner.RunnerName, config runner.Config, resourceId metadata.ResourceID) (types.Runner, error) {
	//etcdConfig := &ETCDConfig{Endpoints: k.EtcdConfig.Endpoints, Username: k.EtcdConfig.Username, Password: k.EtcdConfig.Password}
	//serializedETCD, err := etcdConfig.Serialize()
	//if err != nil {
	//	return nil, err
	//}
	//pandasImage := cfg.GetPandasRunnerImage()
	//workerImage := cfg.GetWorkerImage()
	//kubeConfig := kubernetes.KubernetesRunnerConfig{
	//	EnvVars: map[string]string{
	//		"NAME":             jobName.String(),
	//		"CONFIG":           string(config),
	//		"ETCD_CONFIG":      string(serializedETCD),
	//		"K8S_RUNNER_IMAGE": pandasImage,
	//	},
	//	JobPrefix: "runner",
	//	Image:     workerImage,
	//	NumTasks:  1,
	//	Resource:  resourceId,
	//}
	//jobRunner, err := kubernetes.NewKubernetesRunner(kubeConfig)
	//if err != nil {
	//	return nil, err
	//}
	//return jobRunner, nil
	return nil, nil
}
