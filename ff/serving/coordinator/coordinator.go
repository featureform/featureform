package coordinator

import (
	"context"
	"fmt"

	"go.uber.org/zap"

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

func GetLockKey(jobKey string) string {
	return fmt.Sprintf("LOCK_%s", jobKey)
}

func (k *KubernetesJobSpawner) GetJobRunner(jobName string, config runner.Config) (runner.Runner, error) {
	kubeConfig := runner.KubernetesRunnerConfig{
		EnvVars:  map[string]string{"NAME": runner.CREATE_TRAINING_SET, "CONFIG": string(config)},
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
	return &Coordinator{
		Metadata:   meta,
		Logger:     logger,
		EtcdClient: cli,
		KVClient:   &kvc,
		Spawner:    spawner,
	}, nil
}

const MAX_ATTEMPTS = 10

func (c *Coordinator) WatchForNewJobs() error {
	s, err := concurrency.NewSession(c.EtcdClient, concurrency.WithTTL(10))
	if err != nil {
		return err
	}
	defer s.Close()
	getResp, err := (*c.KVClient).Get(context.Background(), metadata.GetJobKey(metadata.ResourceID{}), clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range getResp.Kvs {
		go c.executeJob(string(kv.Key), s)
	}
	for {
		rch := c.EtcdClient.Watch(context.Background(), metadata.GetJobKey(metadata.ResourceID{}), clientv3.WithPrefix())
		for wresp := range rch {
			for _, ev := range wresp.Events {
				if ev.Type == 0 {
					go c.executeJob(string(ev.Kv.Key), s)
				}

			}
		}
	}
}

func (c *Coordinator) runTransformationJob(resID metadata.ResourceID) error {
	//it's morphin time
	return nil
}
//should only be triggered when we are registering an ONLINE feature, not an offline one
func (c *Coordinator) runFeatureMaterializeJob(resID metadata.ResourceID) error {
	feature, err := c.Metadata.GetFeatureVariant(context.Background(), metadata.NameVariant{resID.Name, resID.Variant})
	if err != nil {
		return err
	}
	status := feature.Status()
	if status == metadata.READY || status == metadata.FAILED {
		return fmt.Errorf("feature already set to %s", metadata.ResourceStatus(status))
	}
	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.PENDING); err != nil {
		return err
	}
	sourceNameVariant := feature.Source()
	source, err := c.Metadata.GetSourceVariant(context.Background(), sourceNameVariant)
	if err != nil {
		return err
	}
	sourceStatus := source.Status()
	if sourceStatus != metadata.READY {
		return fmt.Errorf("source of feature not ready")
	}
	sourceProvider, err := source.FetchProvider(c.Metadata, context.Background())
	if err != nil {
		return err
	}
	p, err := provider.Get(provider.Type(sourceProvider.Type()), sourceProvider.SerializedConfig())
	if err != nil {
		return err
	}
	sourceStore, err := p.AsOfflineStore()
	if err != nil {
		return err
	}
	featureProvider, err := feature.FetchProvider(c.Metadata, context.Background())
	if err != nil {
		return err
	}
	p, err = provider.Get(provider.Type(featureProvider.Type()), featureProvider.SerializedConfig())
	if err != nil {
		return err
	}
	featureStore, err := p.AsOnlineStore()
	if err != nil {
		return err
	}
	materializeRunner := runner.MaterializeRunner{
		Online:  featureStore,
		Offline: sourceStore,
		ID:      provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.Feature},
		Cloud:   "LOCAL",
	}
	completionWatcher, err := materializeRunner.Run()
	if err != nil {
		return err
	}
	if err := completionWatcher.Wait(); err != nil {
		return err
	}
	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.READY); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) runTrainingSetJob(resID metadata.ResourceID) error {
	ts, err := c.Metadata.GetTrainingSetVariant(context.Background(), metadata.NameVariant{resID.Name, resID.Variant})
	if err != nil {
		return err
	}
	status := ts.Status()
	if status == metadata.READY || status == metadata.FAILED {
		return fmt.Errorf("training Set already set to %s", metadata.ResourceStatus(status))
	}
	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.PENDING); err != nil {
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
	providerResID := provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.TrainingSet}
	if _, err := store.GetTrainingSet(providerResID); err == nil {
		return fmt.Errorf("training set already exists")
	}
	features := ts.Features()
	featureList := make([]provider.ResourceID, len(features))
	for i, feature := range features {
		featureList[i] = provider.ResourceID{Name: feature.Name, Variant: feature.Variant, Type: provider.Feature}
	}
	label, err := ts.FetchLabel(c.Metadata, context.Background())
	if err != nil {
		return err
	}
	trainingSetDef := provider.TrainingSetDef{
		ID:       providerResID,
		Label:    provider.ResourceID{Name: label.Name(), Variant: label.Variant(), Type: provider.Label},
		Features: featureList,
	}
	tsRunnerConfig := runner.TrainingSetRunnerConfig{
		OfflineType:   provider.Type(providerEntry.Type()),
		OfflineConfig: providerEntry.SerializedConfig(),
		Def:           trainingSetDef,
	}
	serialized, err := tsRunnerConfig.Serialize()
	if err != nil {
		return err
	}
	jobRunner, err := c.Spawner.GetJobRunner(runner.CREATE_TRAINING_SET, serialized)
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
	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.READY); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) getJob(mtx *concurrency.Mutex, key string) (*metadata.CoordinatorJob, error) {
	txn := (*c.KVClient).Txn(context.Background())
	response, err := txn.If(mtx.IsOwner()).Then(clientv3.OpGet(key)).Commit()
	if err != nil {
		return nil, fmt.Errorf("transaction did not succeed: %v", err)
	}
	isOwner := response.Succeeded //response.Succeeded sets true if transaction "if" statement true
	if !isOwner {
		return nil, fmt.Errorf("was not owner of lock")
	}
	responseData := response.Responses[0]
	responseKVs := responseData.GetResponseRange().GetKvs()
	if len(responseKVs) == 0 {
		return nil, fmt.Errorf("coordinator job does not exist")
	}
	responseValue := responseKVs[0].Value //Only single response for single key
	job := &metadata.CoordinatorJob{}
	if err := job.Deserialize(responseValue); err != nil {
		return nil, fmt.Errorf("could not deserialize coordinator job: %v", err)
	}
	return job, nil
}

func (c *Coordinator) incrementJobAttempts(mtx *concurrency.Mutex, job *metadata.CoordinatorJob, jobKey string) error {
	job.Attempts += 1
	serializedJob, err := job.Serialize()
	if err != nil {
		return fmt.Errorf("could not serialize coordinator job. %v", err)
	}
	txn := (*c.KVClient).Txn(context.Background())
	response, err := txn.If(mtx.IsOwner()).Then(clientv3.OpPut(jobKey, string(serializedJob))).Commit()
	if err != nil {
		return fmt.Errorf("could not set iterated coordinator job. %v", err)
	}
	isOwner := response.Succeeded //response.Succeeded sets true if transaction "if" statement true
	if !isOwner {
		return fmt.Errorf("was not owner of lock")
	}
	return nil
}

func (c *Coordinator) deleteJob(mtx *concurrency.Mutex, key string) error {
	txn := (*c.KVClient).Txn(context.Background())
	response, err := txn.If(mtx.IsOwner()).Then(clientv3.OpDelete(key)).Commit()
	if err != nil {
		return fmt.Errorf("delete job transaction failed: %v", err)
	}
	isOwner := response.Succeeded //response.Succeeded sets true if transaction "if" statement true
	if !isOwner {
		return fmt.Errorf("was not owner of lock")
	}
	responseData := response.Responses[0] //OpDelete always returns single response
	numDeleted := responseData.GetResponseDeleteRange().Deleted
	if numDeleted != 1 { //returns 0 if delete key did not exist
		return fmt.Errorf("job Already deleted")
	}
	return nil
}

func (c *Coordinator) hasJob(id metadata.ResourceID) (bool, error) {
	getResp, err := (*c.KVClient).Get(context.Background(), metadata.GetJobKey(id), clientv3.WithPrefix())
	if err != nil {
		return false, err
	}
	responseLength := len(getResp.Kvs)
	if responseLength > 0 {
		return true, nil
	}
	return false, nil
}

func (c *Coordinator) createJobLock(jobKey string, s *concurrency.Session) (*concurrency.Mutex, error) {
	mtx := concurrency.NewMutex(s, GetLockKey(jobKey))
	if err := mtx.Lock(context.Background()); err != nil {
		return nil, err
	}
	return mtx, nil
}

func (c *Coordinator) markJobFailed(job *metadata.CoordinatorJob) error {
	if err := c.Metadata.SetStatus(context.Background(), job.Resource, metadata.FAILED); err != nil {
		return fmt.Errorf("could not set job status to failed: %v", err)
	}
	return nil
}

func (c *Coordinator) executeJob(jobKey string, s *concurrency.Session) error {
	mtx, err := c.createJobLock(jobKey, s)
	if err != nil {
		return err
	}
	defer func() {
		if err := mtx.Unlock(context.Background()); err != nil {
			panic(err)
		}
	}()
	job, err := c.getJob(mtx, jobKey)
	if err != nil {
		return err
	}
	if job.Attempts > MAX_ATTEMPTS {
		return c.markJobFailed(job)
	}
	if err := c.incrementJobAttempts(mtx, job, jobKey); err != nil {
		return err
	}
	switch job.Resource.Type {
	case metadata.TRAINING_SET_VARIANT:
		if err := c.runTrainingSetJob(job.Resource); err != nil {
			return fmt.Errorf("training set job failed: %v", err)
		}
	case metadata.FEATURE_VARIANT:
		if err := c.runFeatureMaterializeJob(job.Resource); err != nil {
			return fmt.Errorf("feature materialize job failed: %v", err)
		}
	case metadata.TRANSFORMATION:
		if err := c.runTransformationJob(job.Resource); err != nil {
			return fmt.Errorf("transformation job failed: %v", err)
		}
	default:
		return fmt.Errorf("Not a valid resource type for running jobs")
	}
	if err := c.deleteJob(mtx, jobKey); err != nil {
		return err
	}
	return nil
}
