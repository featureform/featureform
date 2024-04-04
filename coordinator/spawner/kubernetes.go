package spawner

import (
	"encoding/json"
	cfg "github.com/featureform/config"
	"github.com/featureform/fferr"
	"github.com/featureform/kubernetes"
	"github.com/featureform/metadata"
	"github.com/featureform/runner"
	"github.com/featureform/types"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type KubernetesJobSpawner struct {
	EtcdConfig clientv3.Config
}

type ETCDConfig struct {
	Endpoints []string
	Username  string
	Password  string
}

type Config []byte

func (c *ETCDConfig) Serialize() (Config, error) {
	config, err := json.Marshal(c)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return config, nil
}

func (c *ETCDConfig) Deserialize(config Config) error {
	err := json.Unmarshal(config, c)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

// TODO: Replace ETCD Reference with new storage interface
func (k *KubernetesJobSpawner) GetJobRunner(jobName runner.RunnerName, config runner.Config, resourceId metadata.ResourceID) (types.Runner, error) {
	etcdConfig := &ETCDConfig{Endpoints: k.EtcdConfig.Endpoints, Username: k.EtcdConfig.Username, Password: k.EtcdConfig.Password}
	serializedETCD, err := etcdConfig.Serialize()
	if err != nil {
		return nil, err
	}
	pandasImage := cfg.GetPandasRunnerImage()
	workerImage := cfg.GetWorkerImage()
	kubeConfig := kubernetes.KubernetesRunnerConfig{
		EnvVars: map[string]string{
			"NAME":             jobName.String(),
			"CONFIG":           string(config),
			"ETCD_CONFIG":      string(serializedETCD),
			"K8S_RUNNER_IMAGE": pandasImage,
		},
		JobPrefix: "runner",
		Image:     workerImage,
		NumTasks:  1,
		Resource:  resourceId,
	}
	jobRunner, err := kubernetes.NewKubernetesRunner(kubeConfig)
	if err != nil {
		return nil, err
	}
	return jobRunner, nil
}
