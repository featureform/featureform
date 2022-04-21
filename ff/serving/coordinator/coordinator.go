package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/featureform/serving/metadata"
	provider "github.com/featureform/serving/provider"
	runner "github.com/featureform/serving/runner"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Coordinator struct {
	Metadata    *metadata.Client
	Logger      *zap.SugaredLogger
	ETCDClient *clientv3.Client
	Spawner     JobSpawner
}

func NewCoordinator(meta *metadata.Client, logger *zap.SugaredLogger, cli *clientv3.Client) (*Coordinator, error) {
	logger.Debug("Creating new coordinator")
	return &FeatureServer{
		Metadata:   meta,
		Logger:     logger,
		ETCDClient: cli,
	}, nil
}

const MAX_ATTEMPTS = 10

const (
	Created ResourceStatus = "CREATED"
	Pending                = "PENDING"
	Failed                 = "FAILED"
	Ready                  = "READY"
)

const (
	Kubernetes JobSpawner = "Kubernetes"
	Memory                = "Memory"
)

type CoordinatorJob struct {
	Attempts int
	Resource metadata.ResourceID
}

func (c *CoordinatorJob) Serialize() ([]byte, error) {
	serialized, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

func (c *CoordinatorJob) Deserialize(serialized []byte) error {
	err := json.Unmarshal(serialized, c)
	if err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) StartTimedJobWatcher() error {
	s, err := concurrency.NewSession(c.cli, concurrency.WithTTL(10))
	if err != nil {
		return err
	}
	defer s.Close()
	for {
		getResp, err := keyValueClient.Get(context.Background(), "JOB", clientv3.WithPrefix())
		if err != nil {
			return err
		}
		for _, kv := range getResp.Kvs {
			go syncHandleJob(string(kv.Key), s)
		}
		time.Sleep(10 * time.Second)
	}
}

func (c *Coordinator) StartJobWatcher() error {
	s, err := concurrency.NewSession(c.cli, concurrency.WithTTL(10))
	if err != nil {
		return err
	}
	defer s.Close()
	for {
		rch := cli.Watch(context.Background(), "JOB", clientv3.WithPrefix())
		for wresp := range rch {
			for _, ev := range wresp.Events {
				if ev.Type == 0 {
					go syncHandleJob(string(ev.Kv.Key), s)
				}

			}
		}
	}
}

func (c *Coordinator) SetResourceStatus(resType metadata.ResourceType, name string, variant string, status string) error {
	ctx := context.Background()
	if _, err := c.Metadata.SetStatus(ctx, metadata.NameVariant{name, variant}, status); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) GetJobRunner(jobName string, config runner.Config) (runner.Runner, error) {
	switch c.Spawner {
	case Kubernetes:
		kubeConfig := runner.KubernetesRunnerConfig{
			EnvVars:  map[string]string{"NAME": "CREATE_TRAINING_SET", "CONFIG": string(getResp)},
			Image:    "featureform/worker",
			NumTasks: 1,
		}
		jobRunner, err := runner.NewKubernetesRunner(kubeConfig)
		if err != nil {
			return nil, err
		}
		return jobRunner, nil
	case Memory:
		jobRunner, err := runner.Create(jobName, config)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("Invalid job spawner set")
	}
}

func (c *Coordinator) runTrainingSetJob(name string, variant string) error {
	ts, err := c.Metadata.GetTrainingSetVariant(context.Background(), metadata.NameVariant{name, variant})
	if err != nil {
		return nil, err
	}
	status := ts.GetStatus()
	if status == ResourceStatus.Ready || status == ResourceStatus.Failed {
		return fmt.Errorf("Training Set already set to %s", status)
	}
	if err := c.SetResourceStatus("TRAINING_SET_VARIANT", name, variant, ResourceStatus.Pending); err != nil {
		return err
	}
	providerEntry, err := ts.FetchProvider(c.Metadata, context.Background())
	if err != nil {
		return err
	}
	p, err := provider.Get(provider.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return err
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		return err
	}
	if _, err := store.GetTrainingSet(provider.ResourceID{Name: name, Variant: variant}); err == nil {
		return fmt.Errorf("Training set already exists")
	}
	features := ts.Features()
	featureList := make([]provider.ResourceID, len(features))
	for i, feature := range features {
		featureList[i] = provider.ResourceID{Name: feature.Name, Variant: feature.Variant}
	}
	label, err := ts.FetchLabel()
	if err != nil {
		return err
	}
	trainingSetDef := provider.TrainingSetDef{
		ID:       provider.ResourceID{Name: name, Variant: variant},
		Label:    provider.ResourceID{Name: label.GetName(), Variant: label.GetVariant()},
		Features: featureList,
	}
	tsRunnerConfig := runner.TrainingSetRunnerConfig{
		OfflineType:   providerEntry.GetType(),
		OfflineConfig: providerEntry.GetSerializedConfig(),
		Def:           trainingSetDef,
	}
	jobRunner, err := c.GetJobRunner("CREATE_TRAINING_SET", tsRunnerConfig)
	if err != nil {
		return err
	}
	completionWatcher, err := jobRunner.Run()
	if err != nil {
		return err
	}
	if err := completionWatcher.Wait(); err != nil {
		return err
	}
	if err := c.SetResourceStatus(metadata.TRAINING_SET_VARIANT, name, variant, ResourceStatus.Ready); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) getJobIfOwner(mtx *concurrency.Mutex, jobKey string) (CoordinatorJob, error) {
	keyValueClient := clientv3.NewKV(c.ETCDClient)
	txn := (keyValueClient).Txn(context.Background())
	response, err := txn.If(mtx.IsOwner()).Then(clientv3.OpGet(jobKey)).Commit()
	if err != nil {
		return nil, fmt.Errorf("transaction did not succeed: %v", err)
	}
	if !response.Succeeded {
		return nil, fmt.Errorf("transaction did not succeed")
	}
	var responseData []byte
	if len(response.Responses[0].GetResponseRange().GetKvs()) > 0 {
		responseData = response.Responses[0].GetResponseRange().GetKvs()[0].Value
	} else {
		return nil, fmt.Errorf("Coordinator job does not exist. %v")
	}
	job := &CoordinatorJob{}
	err := job.Deserialize(responseData)
	if err != nil {
		return nil, fmt.Errorf("Could not deserialize coordinator job: %v", err)
	}
	return *job, nil

}

func (c *Coordinator) incrementJobAttempts(mtx *concurrency.Mutex, job CoordinatorJob, jobKey string) error {
	job.Attempts += 1
	serializedJob, err := job.Serialize()
	if err != nil {
		return fmt.Errorf("Could not serialize coordinator job. %v", err)
	}
	keyValueClient := clientv3.NewKV(c.ETCDClient)
	txn := (keyValueClient).Txn(context.Background())
	if _, err := txn.If(l.IsOwner()).Then(clientv3.OpPut(jobKey, serialized)).Commit(); err != nil {
		return fmt.Errorf("Could not set iterated coordinator job. %v", err)
	}
	return nil
}

func (c *Coordinator) deleteJob(mtx *concurrency.Mutex, jobKey string) error {
	keyValueClient := clientv3.NewKV(c.ETCDClient)
	txn = (keyValueClient).Txn(context.Background())
	response, err := txn.If(mtx.IsOwner()).Then(clientv3.OpDelete(jobKey)).Commit()
	if err != nil {
		return fmt.Errorf("Delete job transaction failed: %v", err)
	}
	if !response.Succeeded {
		return fmt.Errorf("Delete job transaction failed")
	}
	didDelete := response.Responses[0].GetResponseDeleteRange().Deleted
	if didDelete != 1 {
		return fmt.Errorf("Job Already deleted")
	}
	return nil
}

func (c *Coordinator) syncHandleJob(jobKey string, s *concurrency.Session) error {
	mtx := concurrency.NewMutex(s, fmt.Sprintf("LOCK_%s", job_key))
	if err := mtx.Lock(context.Background()); err != nil {
		return err
	}
	defer func() {
		if err := mtx.Unlock(context.Background()); err != nil {
			panic(err)
		}
	}()
	job, err := c.getJobIfOwner(mtx, jobKey)
	if err != nil {
		return err
	}
	if job.Attempts > MAX_ATTEMPTS {
		if err := c.SetResourceStatus(job.Resource, ResourceStatus.Failed); err != nil {
			return fmt.Errorf("Could not set job status to failed: %v", err)
		}
		return nil
	}
	if err := c.incrementJobAttempts(mtx, job, jobKey); err != nil {
		return err
	}
	switch job.Resource.Type {
	case metadata.TRAINING_SET_VARIANT:
		if err := c.runTrainingSetJob(job.Resource.Name, job.Resource.Variant); err != nil {
			return fmt.Errorf("Training set job failed: %v", err)
		}
	}
	if err := c.deleteJob(mtx, jobKey); err != nil {
		return err
	}
	return nil
}
