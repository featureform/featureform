package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	provider "github.com/featureform/serving/provider"
	runner "github.com/featureform/serving/runner"
	"github.com/featureform/serving/metadata"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Coordinator struct {
	Metadata *metadata.Client
	Logger   *zap.SugaredLogger
	ETCD_Client *clientv3.Client
	Spawner JobSpawner
}

func NewCoordinator(meta *metadata.Client, logger *zap.SugaredLogger, cli *clientv3.Client) (*Coordinator, error) {
	logger.Debug("Creating new coordinator")
	return &FeatureServer{
		Metadata: meta,
		Logger:   logger,
		ETCDClient: cli,
	}, nil
}

const MAX_ATTEMPTS = 10

const (
	Created ResourceStatus = "CREATED"
	Pending = "PENDING"
	Failed = "FAILED"
	Ready = "READY"
)

const (
	Kubernetes JobSpawner = "Kubernetes"
	Memory = "Memory"
)

type CoordinatorJob struct {
	Attempts int
	Type metadata.ResourceType
	Name string
	Variant string
}

func (c *CoordinatorJob) Serialize() []byte, error {
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
	ctx := context.Background()
	for {
		getResp, err := kvc.Get(ctx, "JOB", clientv3.WithPrefix())
		if err != nil {
			return err
		}
		for _, kv := range getResp.Kvs {
			go syncHandleJob(string(kv.Key), s)
		}
		time.Sleep(10 * time.Second)
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
			EnvVars: map[string]string{"NAME": "CREATE_TRAINING_SET", "CONFIG": string(getResp)},
			Image: "featureform/worker",
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
	ctx := context.Background()
	ts, err := c.Metadata.GetTrainingSetVariant(ctx, metadata.NameVariant{name, variant})
	if err != nil {
		return nil, err
	}
	status := ts.GetStatus()
	if status == ResourceStatus.Ready || status == ResourceStatus.Failed {
		return fmt.Errorf("Training Set set to %s", status)
	}
	if err := c.SetResourceStatus("TRAINING_SET_VARIANT", name, variant, ResourceStatus.Pending); err != nil {
		return err
	}
	providerEntry, err := ts.FetchProvider(c.Metadata, ctx)
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
	training_set_def := provider.TrainingSetDef{
		ID: provider.ResourceID{Name: name, Variant: variant},
		Label: provider.ResourceID{Name: label.GetName(), Variant: label.GetVariant()},
		Features: featureList,
	}
	ts_runner_config := runner.TrainingSetRunnerConfig{
		OfflineType: providerEntry.GetType(),
		OfflineConfig: providerEntry.GetSerializedConfig(),
		Def: training_set_def,
	}
	jobRunner, err := c.GetJobRunner("CREATE_TRAINING_SET", ts_runner_config)
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
	if err := c.SetResourceStatus("TRAINING_SET_VARIANT", name, variant, ResourceStatus.Ready); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) syncHandleJob(job_key string, s *concurrency.Session) error {
	l := concurrency.NewMutex(s, fmt.Sprintf("LOCK_%s", job_key))
	kvc := clientv3.NewKV(c.ETCD_Client)
	ctx := context.Background()
	get_job_data_txn := (kvc).Txn(ctx)
	if err := l.Lock(ctx); err != nil {
		return err
	}
	unlockAndExit := func() error{
		if err := l.Unlock(ctx); err != nil {
			return err
		}
		return nil
	}
	get_job_data_txn_resp, err := get_job_data_txn.If(l.IsOwner()).Then(clientv3.OpGet(job_key)).Commit()
	if err != nil {
		return fmt.Errorf("transaction did not succeed: %v. %v", err, unlockAndExit())
	}
	if !get_job_data_txn_resp.Succeeded {
		return fmt.Errorf("transaction did not succeed. %v", unlockAndExit())
	}
	var getResp []byte
	if len(get_job_data_txn_resp.Responses[0].GetResponseRange().GetKvs()) > 0{
		getResp = get_job_data_txn_resp.Responses[0].GetResponseRange().GetKvs()[0].Value
	} else {
		return fmt.Errorf("Coordinator job does not exist. %v", unlockAndExit())
	}
	c_job := &CoordinatorJob{}
	err := c_job.Deserialize(getResp)
	if err != nil {
		return fmt.Errorf("Could not deserialize coordinator job: %v. %v", err unlockAndExit())
	}
	if c_job.Attempts > MAX_ATTEMPTS {
		if err := c.SetResourceStatus(c_job.Type, c_job.Name, c_job.Variant, ResourceStatus.Failed); err != nil {
			return fmt.Errorf("Could not set job status to failed: %v. %v", err, unlockAndExit())
		}
		return unlockAndExit()
	}
	c_job.Attempts += 1
	serialized, err := c_job.Serialize()
	if err != nil {
		return fmt.Errorf("Could not serialize coordinator job. %v", unlockAndExit())
	}
	set_new_attempts_txn := (kvc).Txn(ctx)
	set_new_attempts_txn_resp, err := set_new_attempts_txn.If(l.IsOwner()).Then(clientv3.OpPut(job_key,serialized)).Commit()
	if err != nil {
		return fmt.Errorf("Could not set iterated coordinator job. %v", unlockAndExit())
	}
	prev_kv := set_new_attempts_txn_resp.GetResponsePut().GetPrevKv()
	if prev_kv.Key != job_key || prev_kv.Value != getResp {
		return fmt.Errorf("Prev key did not match actual prev key. %v", unlockAndExit())
	}
	switch c_job.Type {
	case metadata.TRAINING_SET_VARIANT:
		if err := runTrainingSetJob(c_job.Name c_job.Variant); err != nil {
			return fmt.Errorf("Training set job failed: %v. %v", err, unlockAndExit())
		}
	}
	delete_job_txn = (kvc).Txn(ctx)
	delete_job_txn_resp, err := delete_job_txn.If(l.IsOwner()).Then(clientv3.OpDelete(job_id)).Commit()
	if err != nil {
		return fmt.Errorf("Delete job transaction failed: %v. %v", err, unlockAndExit())
	}
	if !transaction.Succeeded {
		return fmt.Errorf("Delete job transaction failed. %v", unlockAndExit())
	}
	didDelete := delete_job_txn_resp.Responses[0].GetResponseDeleteRange().Deleted
	if didDelete != 1 {
		return fmt.Errorf("Job Already deleted: %v", unlockAndExit())
	}
	return unlockAndExit()
}

