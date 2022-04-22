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
	Metadata   *metadata.Client
	Logger     *zap.SugaredLogger
	EtcdClient *clientv3.Client
	KVClient   *clientv3.KV
	Spawner    JobSpawner
}

type JobSpawner interface {
	GetJobRunner(jobName string, config runner.Config) (runner.Runner, error)
}

type KubernetesJobSpawner struct{}

type MemoryJobSpawner struct{}

func (k *KubernetesJobSpawner) GetJobRunner(jobName string, config runner.Config) (runner.Runner, error) {
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
}

func (k *MemoryJobSpawner) GetJobRunner(jobName string, config runner.Config) (runner.Runner, error) {
	jobRunner, err := runner.Create(jobName, config)
	if err != nil {
		return nil, err
	}
	return jobRunner, nil
}

func NewCoordinator(meta *metadata.Client, logger *zap.SugaredLogger, cli *clientv3.Client, spawner JobSpawner) (*Coordinator, error) {
	logger.Debug("Creating new coordinator")
	kvc := clientv3.NewKV(cli)
	return &FeatureServer{
		Metadata:   meta,
		Logger:     logger,
		ETCDClient: cli,
		KVClient:   kvc,
		Spawner:    spawner,
	}, nil
}

const MAX_ATTEMPTS = 10

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

func (c *Coordinator) WatchForNewJobs() error {
	s, err := concurrency.NewSession(c.cli, concurrency.WithTTL(10))
	defer s.Close()
	getResp, err := c.KVClient.Get(context.Background(), "JOB_", clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range getResp.Kvs {
		go executeJob(string(kv.Key), s)
	}
	for {
		rch := c.EtcdClient.Watch(context.Background(), "JOB_", clientv3.WithPrefix())
		for wresp := range rch {
			for _, ev := range wresp.Events {
				if ev.Type == 0 {
					go executeJob(string(ev.Kv.Key), s)
				}

			}
		}
	}
}

func (c *Coordinator) runTrainingSetJob(resID metadata.ResourceID) error {
	ts, err := c.Metadata.GetTrainingSetVariant(context.Background(), metadata.NameVariant{resID.Name, resID.Variant})
	if err != nil {
		return nil, err
	}
	status := ts.GetStatus()
	if status == metadata.ResourceStatus.Ready || status == metadata.ResourceStatus.Failed {
		return fmt.Errorf("Training Set already set to %s", status)
	}
	if err := c.Metadata.SetStatus(context.Background(), resID, status); err != nil {
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
	providerResID := provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.OfflineResourceType.TrainingSet}
	if _, err := store.GetTrainingSet(providerResID); err == nil {
		return fmt.Errorf("Training set already exists")
	}
	features := ts.Features()
	featureList := make([]provider.ResourceID, len(features))
	for i, feature := range features {
		featureList[i] = provider.ResourceID{Name: feature.Name, Variant: feature.Variant, Type: provider.OfflineResourceType.Feature}
	}
	label, err := ts.FetchLabel()
	if err != nil {
		return err
	}
	trainingSetDef := provider.TrainingSetDef{
		ID:       providerResID,
		Label:    provider.ResourceID{Name: label.GetName(), Variant: label.GetVariant(), Type: provider.OfflineResourceType.Label},
		Features: featureList,
	}
	tsRunnerConfig := runner.TrainingSetRunnerConfig{
		OfflineType:   providerEntry.GetType(),
		OfflineConfig: providerEntry.GetSerializedConfig(),
		Def:           trainingSetDef,
	}
	jobRunner, err := c.Spawner.GetJobRunner("CREATE_TRAINING_SET", tsRunnerConfig)
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
	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.ResourceStatus.Ready); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) getJob(mtx *concurrency.Mutex, key string) (CoordinatorJob, error) {
	txn := c.KVClient.Txn(context.Background())
	response, err := txn.If(mtx.IsOwner()).Then(clientv3.OpGet(jobKey)).Commit()
	if err != nil {
		return nil, fmt.Errorf("transaction did not succeed: %v", err)
	}
	isOwner := response.Succeeded //response.Succeeded sets true if transaction "if" statement true
	if !isOwner {
		return fmt.Errorf("Was not owner of lock")
	}
	var responseData []byte
	responseData := response.Responses[0]
	responseKVs := responseData.GetResponseRange().GetKvs()
	if len(responseKVs) == 0 {
		return nil, fmt.Errorf("Coordinator job does not exist. %v")
	}
	responseValue = responseKVs[0].Value //Only single response for single key
	job := &CoordinatorJob{}
	err := job.Deserialize(responseValue)
	if err != nil {
		return nil, fmt.Errorf("Could not deserialize coordinator job: %v", err)
	}
	return *job
}

func (c *Coordinator) incrementJobAttempts(mtx *concurrency.Mutex, job CoordinatorJob, jobKey string) error {
	job.Attempts += 1
	serializedJob, err := job.Serialize()
	if err != nil {
		return fmt.Errorf("Could not serialize coordinator job. %v", err)
	}
	txn := c.KVClient.Txn(context.Background())
	response, err := txn.If(mtx.IsOwner()).Then(clientv3.OpPut(jobKey, serialized)).Commit()
	if err != nil {
		return fmt.Errorf("Could not set iterated coordinator job. %v", err)
	}
	isOwner := response.Succeeded //response.Succeeded sets true if transaction "if" statement true
	if !isOwner {
		return fmt.Errorf("Was not owner of lock")
	}
	return nil
}

func (c *Coordinator) deleteJob(mtx *concurrency.Mutex, key string) error {
	txn := c.KVClient.Txn(context.Background())
	response, err := txn.If(mtx.IsOwner()).Then(clientv3.OpDelete(jobKey)).Commit()
	if err != nil {
		return fmt.Errorf("Delete job transaction failed: %v", err)
	}
	isOwner := response.Succeeded //response.Succeeded sets true if transaction "if" statement true
	if !isOwner {
		return fmt.Errorf("Was not owner of lock")
	}
	responseData := response.Responses[0] //OpDelete always returns single response
	numDeleted := responseData.GetResponseDeleteRange().Deleted
	if numDeleted != 1 { //returns 0 if delete key did not exist
		return fmt.Errorf("Job Already deleted")
	}
	return nil
}

func (c *Coordinator) createJobLock() (*concurrency.Mutex, err) {
	mtx := concurrency.NewMutex(s, fmt.Sprintf("LOCK_%s", jobKey))
	if err := mtx.Lock(context.Background()); err != nil {
		return nil, err
	}
	return mtx, nil
}

func (c *Coordinator) markJobFailed(job CoordinatorJob) error {
	if err := c.SetResourceStatus(job.Resource, metadata.ResourceStatus.Failed); err != nil {
		return fmt.Errorf("Could not set job status to failed: %v", err)
	}
	return nil
}

func (c *Coordinator) executeJob(jobKey string, s *concurrency.Session) error {
	mtx, err := c.createJobLock(s, jobKey)
	if err != nil {
		return err
	}
	defer func() {
		if err := mtx.Unlock(context.Background()); err != nil {
			panic(err)
		}
	}()
	job, err := c.getJob(mtx, jobKey)
	if job.Attempts > MAX_ATTEMPTS {
		return c.markJobFailed(job)
	}
	if err := c.incrementJobAttempts(mtx, job, jobKey); err != nil {
		return err
	}
	switch job.Resource.Type {
	case metadata.TRAINING_SET_VARIANT:
		if err := c.runTrainingSetJob(job.Resource); err != nil {
			return fmt.Errorf("Training set job failed: %v", err)
		}
	}
	if err := c.deleteJob(mtx, jobKey); err != nil {
		return err
	}
	return nil
}
