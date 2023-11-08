package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	db "github.com/jackc/pgx/v4"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"

	cfg "github.com/featureform/config"
	"github.com/featureform/kubernetes"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/runner"
	"github.com/featureform/types"
)

func retryWithDelays(name string, retries int, delay time.Duration, idempotentFunction func() error) error {
	var err error
	for i := 0; i < retries; i++ {
		if err = idempotentFunction(); err == nil {
			return nil
		}
		time.Sleep(delay)
	}
	return fmt.Errorf("retried %s %d times unsuccessfully: Latest error message: %v", name, retries, err)
}

type Config []byte

func templateReplace(template string, replacements map[string]string, offlineStore provider.OfflineStore) (string, error) {
	formattedString := ""
	numEscapes := strings.Count(template, "{{")
	for i := 0; i < numEscapes; i++ {
		split := strings.SplitN(template, "{{", 2)
		afterSplit := strings.SplitN(split[1], "}}", 2)
		key := strings.TrimSpace(afterSplit[0])
		replacement, has := replacements[key]
		if !has {
			return "", fmt.Errorf("no key set")
		}

		if offlineStore.Type() == pt.BigQueryOffline {
			bqConfig := pc.BigQueryConfig{}
			bqConfig.Deserialize(offlineStore.Config())
			replacement = fmt.Sprintf("`%s.%s.%s`", bqConfig.ProjectId, bqConfig.DatasetId, replacement)
		} else {
			replacement = sanitize(replacement)
		}
		formattedString += fmt.Sprintf("%s%s", split[0], replacement)
		template = afterSplit[1]
	}
	formattedString += template
	return formattedString, nil
}

func getSourceMapping(template string, replacements map[string]string) ([]provider.SourceMapping, error) {
	sourceMap := []provider.SourceMapping{}
	numEscapes := strings.Count(template, "{{")
	for i := 0; i < numEscapes; i++ {
		split := strings.SplitN(template, "{{", 2)
		afterSplit := strings.SplitN(split[1], "}}", 2)
		key := strings.TrimSpace(afterSplit[0])
		replacement, has := replacements[key]
		if !has {
			return nil, fmt.Errorf("no key set")
		}
		sourceMap = append(sourceMap, provider.SourceMapping{Template: sanitize(replacement), Source: replacement})
		template = afterSplit[1]
	}
	return sourceMap, nil
}

type Coordinator struct {
	Metadata   *metadata.Client
	Logger     *zap.SugaredLogger
	EtcdClient *clientv3.Client
	KVClient   *clientv3.KV
	Spawner    JobSpawner
	Timeout    int
}

type ETCDConfig struct {
	Endpoints []string
	Username  string
	Password  string
}

func (c *ETCDConfig) Serialize() (Config, error) {
	config, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return config, nil
}

func (c *ETCDConfig) Deserialize(config Config) error {
	err := json.Unmarshal(config, c)
	if err != nil {
		return fmt.Errorf("deserialize etcd config: %v", err)
	}
	return nil
}

func (c *Coordinator) AwaitPendingSource(sourceNameVariant metadata.NameVariant) (*metadata.SourceVariant, error) {
	sourceStatus := metadata.PENDING
	for sourceStatus != metadata.READY {
		source, err := c.Metadata.GetSourceVariant(context.Background(), sourceNameVariant)
		if err != nil {
			return nil, err
		}
		sourceStatus := source.Status()
		if sourceStatus == metadata.FAILED {
			return nil, fmt.Errorf("source registration failed: name: %s, variant: %s", sourceNameVariant.Name, sourceNameVariant.Variant)
		}
		if sourceStatus == metadata.READY {
			return source, nil
		}
		time.Sleep(1 * time.Second)
	}
	return c.Metadata.GetSourceVariant(context.Background(), sourceNameVariant)
}

func (c *Coordinator) AwaitPendingFeature(featureNameVariant metadata.NameVariant) (*metadata.FeatureVariant, error) {
	featureStatus := metadata.PENDING
	for featureStatus != metadata.READY {
		feature, err := c.Metadata.GetFeatureVariant(context.Background(), featureNameVariant)
		if err != nil {
			return nil, err
		}
		featureStatus := feature.Status()
		if featureStatus == metadata.FAILED {
			return nil, fmt.Errorf("feature registration failed: name: %s, variant: %s", featureNameVariant.Name, featureNameVariant.Variant)
		}
		if featureStatus == metadata.READY {
			return feature, nil
		}
		time.Sleep(1 * time.Second)
	}
	return c.Metadata.GetFeatureVariant(context.Background(), featureNameVariant)
}

func (c *Coordinator) AwaitPendingLabel(labelNameVariant metadata.NameVariant) (*metadata.LabelVariant, error) {
	labelStatus := metadata.PENDING
	for labelStatus != metadata.READY {
		label, err := c.Metadata.GetLabelVariant(context.Background(), labelNameVariant)
		if err != nil {
			return nil, err
		}
		labelStatus := label.Status()
		if labelStatus == metadata.FAILED {
			return nil, fmt.Errorf("label registration failed: name: %s, variant: %s", labelNameVariant.Name, labelNameVariant.Variant)
		}
		if labelStatus == metadata.READY {
			return label, nil
		}
		time.Sleep(1 * time.Second)
	}
	return c.Metadata.GetLabelVariant(context.Background(), labelNameVariant)
}

type JobSpawner interface {
	GetJobRunner(jobName string, config runner.Config, resourceId metadata.ResourceID) (types.Runner, error)
}

type KubernetesJobSpawner struct {
	EtcdConfig clientv3.Config
}

type MemoryJobSpawner struct{}

func GetLockKey(jobKey string) string {
	return fmt.Sprintf("LOCK_%s", jobKey)
}

func (k *KubernetesJobSpawner) GetJobRunner(jobName string, config runner.Config, resourceId metadata.ResourceID) (types.Runner, error) {
	etcdConfig := &ETCDConfig{Endpoints: k.EtcdConfig.Endpoints, Username: k.EtcdConfig.Username, Password: k.EtcdConfig.Password}
	serializedETCD, err := etcdConfig.Serialize()
	if err != nil {
		return nil, err
	}
	pandasImage := cfg.GetPandasRunnerImage()
	workerImage := cfg.GetWorkerImage()
	fmt.Println("GETJOBRUNNERID:", resourceId)
	kubeConfig := kubernetes.KubernetesRunnerConfig{
		EnvVars: map[string]string{
			"NAME":             jobName,
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

func (k *MemoryJobSpawner) GetJobRunner(jobName string, config runner.Config, resourceId metadata.ResourceID) (types.Runner, error) {
	jobRunner, err := runner.Create(jobName, config)
	if err != nil {
		return nil, err
	}
	return jobRunner, nil
}

func NewCoordinator(meta *metadata.Client, logger *zap.SugaredLogger, cli *clientv3.Client, spawner JobSpawner) (*Coordinator, error) {
	logger.Info("Creating new coordinator")
	kvc := clientv3.NewKV(cli)
	return &Coordinator{
		Metadata:   meta,
		Logger:     logger,
		EtcdClient: cli,
		KVClient:   &kvc,
		Spawner:    spawner,
		Timeout:    600,
	}, nil
}

const MAX_ATTEMPTS = 3

func (c *Coordinator) checkError(err error, jobName string) {
	switch err.(type) {
	case JobDoesNotExistError:
		c.Logger.Info(err)
	case ResourceAlreadyFailedError:
		c.Logger.Infow("resource has failed previously. Ignoring....", "key", jobName)
	case ResourceAlreadyCompleteError:
		c.Logger.Infow("resource has already completed. Ignoring....", "key", jobName)
	default:
		c.Logger.Errorw("Error executing job", "job_name", jobName, "error", err)
	}
}

func (c *Coordinator) WatchForNewJobs() error {
	c.Logger.Info("Watching for new jobs")
	getResp, err := (*c.KVClient).Get(context.Background(), "JOB_", clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("get existing etcd jobs: %v", err)
	}
	for _, kv := range getResp.Kvs {
		go func(kv *mvccpb.KeyValue) {
			err := c.ExecuteJob(string(kv.Key))
			if err != nil {
				c.checkError(err, string(kv.Key))
			}
		}(kv)
	}
	for {
		rch := c.EtcdClient.Watch(context.Background(), "JOB_", clientv3.WithPrefix())
		for wresp := range rch {
			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.PUT {
					go func(ev *clientv3.Event) {
						err := c.ExecuteJob(string(ev.Kv.Key))
						if err != nil {
							c.checkError(err, string(ev.Kv.Key))
						}
					}(ev)
				}

			}
		}
	}
}

func (c *Coordinator) WatchForUpdateEvents() error {
	c.Logger.Info("Watching for new update events")
	for {
		rch := c.EtcdClient.Watch(context.Background(), "UPDATE_EVENT_", clientv3.WithPrefix())
		for wresp := range rch {
			for _, ev := range wresp.Events {
				if ev.Type == 0 {
					go func(ev *clientv3.Event) {
						err := c.signalResourceUpdate(string(ev.Kv.Key), string(ev.Kv.Value))
						if err != nil {
							c.Logger.Errorw("Error executing update event catch: Polling search", "error", err)
						}
					}(ev)
				}

			}
		}
	}
	return nil
}

func (c *Coordinator) WatchForScheduleChanges() error {
	c.Logger.Info("Watching for new update events")
	getResp, err := (*c.KVClient).Get(context.Background(), "SCHEDULEJOB_", clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("fetch existing etcd schedule jobs: %v", err)
	}
	for _, kv := range getResp.Kvs {
		go func(kv *mvccpb.KeyValue) {
			err := c.changeJobSchedule(string(kv.Key), string(kv.Value))
			if err != nil {
				c.Logger.Errorw("Error executing job schedule change: Initial search", "error", err)
			}
		}(kv)
	}
	for {
		rch := c.EtcdClient.Watch(context.Background(), "SCHEDULEJOB_", clientv3.WithPrefix())
		for wresp := range rch {
			for _, ev := range wresp.Events {
				if ev.Type == 0 {
					go func(ev *clientv3.Event) {
						err := c.changeJobSchedule(string(ev.Kv.Key), string(ev.Kv.Value))
						if err != nil {
							c.Logger.Errorw("Error executing job schedule change: Polling search", "error", err)
						}
					}(ev)
				}

			}
		}
	}
	return nil
}

func (c *Coordinator) mapNameVariantsToTables(sources []metadata.NameVariant) (map[string]string, error) {
	sourceMap := make(map[string]string)
	for _, nameVariant := range sources {
		source, err := c.Metadata.GetSourceVariant(context.Background(), nameVariant)
		if err != nil {
			return nil, err
		}
		if source.Status() != metadata.READY {
			return nil, fmt.Errorf("source in query not ready")
		}
		providerResourceID := provider.ResourceID{Name: source.Name(), Variant: source.Variant()}
		var tableName string
		sourceProvider, err := source.FetchProvider(c.Metadata, context.Background())
		if err != nil {
			return nil, fmt.Errorf("could not fetch source provider: %v", err)
		}

		if (sourceProvider.Type() == "SPARK_OFFLINE" || sourceProvider.Type() == "K8S_OFFLINE") && (source.IsDFTransformation() || source.IsSQLTransformation()) {
			providerResourceID.Type = provider.Transformation
			tableName, err = provider.GetTransformationTableName(providerResourceID)
			if err != nil {
				return nil, err
			}
		} else {
			providerResourceID.Type = provider.Primary
			tableName, err = provider.GetPrimaryTableName(providerResourceID)
			if err != nil {
				return nil, err
			}
		}
		sourceMap[nameVariant.ClientString()] = tableName
	}
	return sourceMap, nil
}

func sanitize(ident string) string {
	return db.Identifier{ident}.Sanitize()
}

func (c *Coordinator) verifyCompletionOfSources(sources []metadata.NameVariant) error {
	allReady := false
	for !allReady {
		sourceVariants, err := c.Metadata.GetSourceVariants(context.Background(), sources)
		if err != nil {
			return fmt.Errorf("could not get source variants: %v ", err)
		}
		total := len(sourceVariants)
		totalReady := 0
		for _, sourceVariant := range sourceVariants {
			if sourceVariant.Status() == metadata.READY {
				totalReady += 1
			}
			if sourceVariant.Status() == metadata.FAILED {
				fmt.Errorf("dependent source variant, %s, failed", sourceVariant.Name())
			}
		}
		allReady = total == totalReady
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (c *Coordinator) runTransformationJob(transformationConfig provider.TransformationConfig, resID metadata.ResourceID, schedule string, sourceProvider *metadata.Provider) error {
	transformation, err := c.Metadata.GetSourceVariant(context.Background(), metadata.NameVariant{resID.Name, resID.Variant})
	if err != nil {
		return fmt.Errorf("get label variant: %v", err)
	}
	status := transformation.Status()

	if status == metadata.READY {
		return ResourceAlreadyCompleteError{
			resourceID: resID,
		}
	}
	if status == metadata.FAILED {
		return ResourceAlreadyFailedError{
			resourceID: resID,
		}
	}
	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.PENDING, ""); err != nil {
		return fmt.Errorf("set pending status for transformation job: %v", err)
	}

	createTransformationConfig := runner.CreateTransformationConfig{
		OfflineType:          pt.Type(sourceProvider.Type()),
		OfflineConfig:        sourceProvider.SerializedConfig(),
		TransformationConfig: transformationConfig,
		IsUpdate:             false,
	}
	c.Logger.Debugw("Transformation Serialize Config")
	serialized, err := createTransformationConfig.Serialize()
	if err != nil {
		return fmt.Errorf("serialize transformation config: %v", err)
	}
	c.Logger.Debugw("Transformation Get Job Runner")
	jobRunner, err := c.Spawner.GetJobRunner(runner.CREATE_TRANSFORMATION, serialized, resID)
	if err != nil {
		return fmt.Errorf("failed to create transformation job runner: %v", err)
	}
	c.Logger.Debugw("Transformation Run Job")
	completionWatcher, err := jobRunner.Run()
	if err != nil {
		return fmt.Errorf("failed to create transformation job: %v", err)
	}
	c.Logger.Debugw("Transformation Waiting For Completion")
	if err := completionWatcher.Wait(); err != nil {
		return fmt.Errorf("transformation failed to complete: %v", err)
	}
	c.Logger.Debugw("Transformation Setting Status")
	if err := retryWithDelays("set status to ready", 5, time.Millisecond*10, func() error { return c.Metadata.SetStatus(context.Background(), resID, metadata.READY, "") }); err != nil {
		return fmt.Errorf("failed to set transformation status: %v", err)
	}
	c.Logger.Debugw("Transformation Complete")
	if schedule != "" {
		scheduleCreateTransformationConfig := runner.CreateTransformationConfig{
			OfflineType:          pt.Type(sourceProvider.Type()),
			OfflineConfig:        sourceProvider.SerializedConfig(),
			TransformationConfig: transformationConfig,
			IsUpdate:             true,
		}
		serializedUpdate, err := scheduleCreateTransformationConfig.Serialize()
		if err != nil {
			return fmt.Errorf("serialize schedule transformation config: %v", err)
		}
		jobRunnerUpdate, err := c.Spawner.GetJobRunner(runner.CREATE_TRANSFORMATION, serializedUpdate, resID)
		if err != nil {
			return fmt.Errorf("run ransformation schedule job runner: %v", err)
		}
		cronRunner, isCronRunner := jobRunnerUpdate.(kubernetes.CronRunner)
		if !isCronRunner {
			return fmt.Errorf("kubernetes runner does not implement schedule")
		}
		if err := cronRunner.ScheduleJob(kubernetes.CronSchedule(schedule)); err != nil {
			return fmt.Errorf("schedule transformation job in kubernetes: %v", err)
		}
		if err := c.Metadata.SetStatus(context.Background(), resID, metadata.READY, ""); err != nil {
			return fmt.Errorf("set transformation succesful schedule status: %v", err)
		}
	}
	return nil
}

func (c *Coordinator) runSQLTransformationJob(transformSource *metadata.SourceVariant, resID metadata.ResourceID, offlineStore provider.OfflineStore, schedule string, sourceProvider *metadata.Provider) error {
	c.Logger.Info("Running SQL transformation job on resource: ", resID)
	templateString := transformSource.SQLTransformationQuery()
	sources := transformSource.SQLTransformationSources()

	err := c.verifyCompletionOfSources(sources)
	if err != nil {
		return fmt.Errorf("the sources were not completed: %s", err)
	}

	sourceMap, err := c.mapNameVariantsToTables(sources)
	if err != nil {
		return fmt.Errorf("map name: %v sources: %v", err, sources)
	}
	sourceMapping, err := getSourceMapping(templateString, sourceMap)
	if err != nil {
		return fmt.Errorf("getSourceMapping replace: %v source map: %v, template: %s", err, sourceMap, templateString)
	}

	var query string
	query, err = templateReplace(templateString, sourceMap, offlineStore)
	if err != nil {
		return fmt.Errorf("template replace: %v source map: %v, template: %s", err, sourceMap, templateString)
	}

	c.Logger.Debugw("Created transformation query", "query", query)
	providerResourceID := provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.Transformation}
	transformationConfig := provider.TransformationConfig{
		Type:          provider.SQLTransformation,
		TargetTableID: providerResourceID,
		Query:         query,
		SourceMapping: sourceMapping,
		Args:          transformSource.TransformationArgs(),
	}

	err = c.runTransformationJob(transformationConfig, resID, schedule, sourceProvider)
	if err != nil {
		return err
	}

	return nil
}

func (c *Coordinator) runDFTransformationJob(transformSource *metadata.SourceVariant, resID metadata.ResourceID, offlineStore provider.OfflineStore, schedule string, sourceProvider *metadata.Provider) error {
	c.Logger.Info("Running DF transformation job on resource: ", resID)
	code := transformSource.DFTransformationQuery()
	sources := transformSource.DFTransformationSources()

	err := c.verifyCompletionOfSources(sources)
	if err != nil {
		return fmt.Errorf("the sources were not completed: %s", err)
	}

	sourceMap, err := c.mapNameVariantsToTables(sources)
	if err != nil {
		return fmt.Errorf("map name: %v sources: %v", err, sources)
	}

	sourceMapping, err := getOrderedSourceMappings(sources, sourceMap)
	if err != nil {
		return fmt.Errorf("failed to get ordered source mappings due to %v", err)
	}

	c.Logger.Debugw("Created transformation query")
	providerResourceID := provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.Transformation}
	transformationConfig := provider.TransformationConfig{
		Type:          provider.DFTransformation,
		TargetTableID: providerResourceID,
		Code:          code,
		SourceMapping: sourceMapping,
		Args:          transformSource.TransformationArgs(),
	}

	err = c.runTransformationJob(transformationConfig, resID, schedule, sourceProvider)
	if err != nil {
		return err
	}

	return nil
}

func getOrderedSourceMappings(sources []metadata.NameVariant, sourceMap map[string]string) ([]provider.SourceMapping, error) {
	sourceMapping := make([]provider.SourceMapping, len(sources))
	for i, nv := range sources {
		sourceKey := nv.ClientString()
		tableName, hasKey := sourceMap[sourceKey]
		if !hasKey {
			return nil, fmt.Errorf("key %s not in source map", sourceKey)
		}
		sourceMapping[i] = provider.SourceMapping{Template: sourceKey, Source: tableName}
	}
	return sourceMapping, nil
}

func (c *Coordinator) runPrimaryTableJob(source *metadata.SourceVariant, resID metadata.ResourceID, offlineStore provider.OfflineStore, schedule string) error {
	c.Logger.Info("Running primary table job on resource: ", resID)
	providerResourceID := provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.Primary}
	if !source.IsPrimaryDataSQLTable() {
		return fmt.Errorf("%s is not a primary table", source.Name())
	}
	sourceName := source.PrimaryDataSQLTableName()
	if sourceName == "" {
		return fmt.Errorf("source name is not set")
	}
	if _, err := offlineStore.RegisterPrimaryFromSourceTable(providerResourceID, sourceName); err != nil {
		return fmt.Errorf("unable to register primary table from %s in %s: %v", sourceName, offlineStore.Type().String(), err)
	}
	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.READY, ""); err != nil {
		return fmt.Errorf("set done status for registering primary table: %v", err)
	}
	return nil
}

func (c *Coordinator) runRegisterSourceJob(resID metadata.ResourceID, schedule string) error {
	c.Logger.Info("Running register source job on resource: ", resID)
	source, err := c.Metadata.GetSourceVariant(context.Background(), metadata.NameVariant{resID.Name, resID.Variant})
	if err != nil {
		return fmt.Errorf("get source variant from metadata: %v", err)
	}
	sourceProvider, err := source.FetchProvider(c.Metadata, context.Background())
	if err != nil {
		return fmt.Errorf("fetch source's dependent provider in metadata: %v", err)
	}
	p, err := provider.Get(pt.Type(sourceProvider.Type()), sourceProvider.SerializedConfig())
	if err != nil {
		return fmt.Errorf("failed to initialize provider: %v", err)
	}
	sourceStore, err := p.AsOfflineStore()
	if err != nil {
		return fmt.Errorf("convert source provider to offline store interface: %v", err)
	}
	defer func(sourceStore provider.OfflineStore) {
		err := sourceStore.Close()
		if err != nil {
			c.Logger.Errorf("could not close offline store: %v", err)
		}
	}(sourceStore)
	if source.IsSQLTransformation() {
		return c.runSQLTransformationJob(source, resID, sourceStore, schedule, sourceProvider)
	} else if source.IsDFTransformation() {
		return c.runDFTransformationJob(source, resID, sourceStore, schedule, sourceProvider)
	} else if source.IsPrimaryDataSQLTable() {
		return c.runPrimaryTableJob(source, resID, sourceStore, schedule)
	} else {
		return fmt.Errorf("source type not implemented")
	}
}

func (c *Coordinator) runLabelRegisterJob(resID metadata.ResourceID, schedule string) error {
	c.Logger.Info("Running label register job: ", resID)
	label, err := c.Metadata.GetLabelVariant(context.Background(), metadata.NameVariant{resID.Name, resID.Variant})
	if err != nil {
		return fmt.Errorf("get label variant: %v", err)
	}
	status := label.Status()
	if status == metadata.READY {
		return ResourceAlreadyCompleteError{
			resourceID: resID,
		}
	}
	if status == metadata.FAILED {
		return ResourceAlreadyFailedError{
			resourceID: resID,
		}
	}
	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.PENDING, ""); err != nil {
		return fmt.Errorf("set pending status for label variant: %v", err)
	}

	sourceNameVariant := label.Source()
	c.Logger.Infow("feature obj", "name", label.Name(), "source", label.Source(), "location", label.Location(), "location_col", label.LocationColumns())

	source, err := c.AwaitPendingSource(sourceNameVariant)
	if err != nil {
		return fmt.Errorf("source of could not complete job: %v", err)
	}
	sourceProvider, err := source.FetchProvider(c.Metadata, context.Background())
	if err != nil {
		return fmt.Errorf("could not fetch online provider: %v", err)
	}
	p, err := provider.Get(pt.Type(sourceProvider.Type()), sourceProvider.SerializedConfig())
	if err != nil {
		return fmt.Errorf("could not get offline provider config: %v", err)
	}
	sourceStore, err := p.AsOfflineStore()
	if err != nil {
		return fmt.Errorf("convert source provider to offline store interface: %v", err)
	}
	defer func(sourceStore provider.OfflineStore) {
		err := sourceStore.Close()
		if err != nil {
			c.Logger.Errorf("could not close offline store: %v", err)
		}
	}(sourceStore)
	var sourceTableName string
	if source.IsSQLTransformation() || source.IsDFTransformation() {
		sourceResourceID := provider.ResourceID{sourceNameVariant.Name, sourceNameVariant.Variant, provider.Transformation}
		sourceTable, err := sourceStore.GetTransformationTable(sourceResourceID)
		if err != nil {
			return err
		}
		sourceTableName = sourceTable.GetName()
	} else if source.IsPrimaryDataSQLTable() {
		sourceResourceID := provider.ResourceID{sourceNameVariant.Name, sourceNameVariant.Variant, provider.Primary}
		sourceTable, err := sourceStore.GetPrimaryTable(sourceResourceID)
		if err != nil {
			return err
		}
		sourceTableName = sourceTable.GetName()
	}

	labelID := provider.ResourceID{
		Name:    resID.Name,
		Variant: resID.Variant,
		Type:    provider.Label,
	}
	tmpSchema := label.LocationColumns().(metadata.ResourceVariantColumns)
	schema := provider.ResourceSchema{
		Entity:      tmpSchema.Entity,
		Value:       tmpSchema.Value,
		TS:          tmpSchema.TS,
		SourceTable: sourceTableName,
	}
	c.Logger.Debugw("Creating Label Resource Table", "id", labelID, "schema", schema)
	_, err = sourceStore.RegisterResourceFromSourceTable(labelID, schema)
	if err != nil {
		return fmt.Errorf("register from source: %v", err)
	}
	c.Logger.Debugw("Resource Table Created", "id", labelID, "schema", schema)

	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.READY, ""); err != nil {
		return fmt.Errorf("set ready status for label variant: %v", err)
	}
	return nil
}

func (c *Coordinator) runFeatureMaterializeJob(resID metadata.ResourceID, schedule string) error {
	c.Logger.Info("Running feature materialization job on resource: ", resID)
	feature, err := c.Metadata.GetFeatureVariant(context.Background(), metadata.NameVariant{resID.Name, resID.Variant})
	if err != nil {
		return fmt.Errorf("get feature variant from metadata: %v", err)
	}
	status := feature.Status()
	featureType := feature.Type()
	if status == metadata.READY {
		return ResourceAlreadyCompleteError{
			resourceID: resID,
		}
	}
	if status == metadata.FAILED {
		return ResourceAlreadyFailedError{
			resourceID: resID,
		}
	}
	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.PENDING, ""); err != nil {
		return fmt.Errorf("set feature variant status to pending: %v", err)
	}

	sourceNameVariant := feature.Source()
	c.Logger.Infow("feature obj", "name", feature.Name(), "source", feature.Source(), "location", feature.Location(), "location_col", feature.LocationColumns())

	source, err := c.AwaitPendingSource(sourceNameVariant)
	if err != nil {
		return fmt.Errorf("source of could not complete job: %v", err)
	}
	sourceProvider, err := source.FetchProvider(c.Metadata, context.Background())
	if err != nil {
		return fmt.Errorf("could not fetch online provider: %v", err)
	}
	p, err := provider.Get(pt.Type(sourceProvider.Type()), sourceProvider.SerializedConfig())
	if err != nil {
		return err
	}
	sourceStore, err := p.AsOfflineStore()
	if err != nil {
		return err
	}
	defer func(sourceStore provider.OfflineStore) {
		err := sourceStore.Close()
		if err != nil {
			c.Logger.Errorf("could not close offline store: %v", err)
		}
	}(sourceStore)
	featureProvider, err := feature.FetchProvider(c.Metadata, context.Background())
	if err != nil {
		return fmt.Errorf("could not fetch online provider: %v", err)
	}
	var vType provider.ValueType
	if feature.IsEmbedding() {
		vType = provider.VectorType{
			ScalarType:  provider.ScalarType(featureType),
			Dimension:   feature.Dimension(),
			IsEmbedding: true,
		}
	} else {
		vType = provider.ScalarType(featureType)
	}
	if err != nil {
		return err
	}
	materializedRunnerConfig := runner.MaterializedRunnerConfig{
		OnlineType:    pt.Type(featureProvider.Type()),
		OfflineType:   pt.Type(sourceProvider.Type()),
		OnlineConfig:  featureProvider.SerializedConfig(),
		OfflineConfig: sourceProvider.SerializedConfig(),
		ResourceID:    provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.Feature},
		VType:         provider.ValueTypeJSONWrapper{ValueType: vType},
		Cloud:         runner.LocalMaterializeRunner,
		IsUpdate:      false,
	}
	serialized, err := materializedRunnerConfig.Serialize()
	if err != nil {
		return fmt.Errorf("could not get online provider config: %v", err)
	}
	var sourceTableName string
	if source.IsSQLTransformation() || source.IsDFTransformation() {
		sourceResourceID := provider.ResourceID{sourceNameVariant.Name, sourceNameVariant.Variant, provider.Transformation}
		sourceTable, err := sourceStore.GetTransformationTable(sourceResourceID)
		if err != nil {
			return err
		}
		sourceTableName = sourceTable.GetName()
	} else if source.IsPrimaryDataSQLTable() {
		sourceResourceID := provider.ResourceID{sourceNameVariant.Name, sourceNameVariant.Variant, provider.Primary}
		sourceTable, err := sourceStore.GetPrimaryTable(sourceResourceID)
		if err != nil {
			return err
		}
		sourceTableName = sourceTable.GetName()
	}

	featID := provider.ResourceID{
		Name:    resID.Name,
		Variant: resID.Variant,
		Type:    provider.Feature,
	}
	tmpSchema := feature.LocationColumns().(metadata.ResourceVariantColumns)
	schema := provider.ResourceSchema{
		Entity:      tmpSchema.Entity,
		Value:       tmpSchema.Value,
		TS:          tmpSchema.TS,
		SourceTable: sourceTableName,
	}
	c.Logger.Debugw("Creating Resource Table", "id", featID, "schema", schema)
	_, err = sourceStore.RegisterResourceFromSourceTable(featID, schema)
	if err != nil {
		return fmt.Errorf("materialize feature register: %v", err)
	}
	c.Logger.Debugw("Resource Table Created", "id", featID, "schema", schema)
	needsOnlineMaterialization := strings.Split(string(featureProvider.Type()), "_")[1] == "ONLINE"
	if needsOnlineMaterialization {
		c.Logger.Info("Starting Materialize")
		jobRunner, err := c.Spawner.GetJobRunner(runner.MATERIALIZE, serialized, resID)
		if err != nil {
			return fmt.Errorf("could not use %s as online store: %w", featureProvider.Name(), err)
		}
		completionWatcher, err := jobRunner.Run()
		if err != nil {
			return fmt.Errorf("failed to run job: %w", err)
		}
		if err := completionWatcher.Wait(); err != nil {
			return fmt.Errorf("failed to complete job: %w", err)
		}
	}
	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.READY, ""); err != nil {
		return fmt.Errorf("failed to set status: %v", err)
	}
	if schedule != "" && needsOnlineMaterialization {
		scheduleMaterializeRunnerConfig := runner.MaterializedRunnerConfig{
			OnlineType:    pt.Type(featureProvider.Type()),
			OfflineType:   pt.Type(sourceProvider.Type()),
			OnlineConfig:  featureProvider.SerializedConfig(),
			OfflineConfig: sourceProvider.SerializedConfig(),
			ResourceID:    provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.Feature},
			VType:         provider.ValueTypeJSONWrapper{ValueType: vType},
			Cloud:         runner.LocalMaterializeRunner,
			IsUpdate:      true,
		}
		serializedUpdate, err := scheduleMaterializeRunnerConfig.Serialize()
		if err != nil {
			return fmt.Errorf("serialize materialize runner config: %v", err)
		}
		jobRunnerUpdate, err := c.Spawner.GetJobRunner(runner.MATERIALIZE, serializedUpdate, resID)
		if err != nil {
			return fmt.Errorf("creating materialize job schedule job runner: %v", err)
		}
		cronRunner, isCronRunner := jobRunnerUpdate.(kubernetes.CronRunner)
		if !isCronRunner {
			return fmt.Errorf("kubernetes runner does not implement schedule")
		}
		if err := cronRunner.ScheduleJob(kubernetes.CronSchedule(schedule)); err != nil {
			return fmt.Errorf("schedule materialize job in kubernetes: %v", err)
		}
		if err := c.Metadata.SetStatus(context.Background(), resID, metadata.READY, ""); err != nil {
			return fmt.Errorf("set succesful update status for materialize job in kubernetes: %v", err)
		}
	}
	return nil
}

func (c *Coordinator) runTrainingSetJob(resID metadata.ResourceID, schedule string) error {
	c.Logger.Info("Running training set job on resource: ", "name", resID.Name, "variant", resID.Variant)
	ts, err := c.Metadata.GetTrainingSetVariant(context.Background(), metadata.NameVariant{resID.Name, resID.Variant})
	if err != nil {
		return fmt.Errorf("fetch training set variant from metadata: %v", err)
	}
	status := ts.Status()
	if status == metadata.READY {
		return ResourceAlreadyCompleteError{
			resourceID: resID,
		}
	}
	if status == metadata.FAILED {
		return ResourceAlreadyFailedError{
			resourceID: resID,
		}
	}
	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.PENDING, ""); err != nil {
		return fmt.Errorf("set training set variant status to pending: %v", err)
	}
	providerEntry, err := ts.FetchProvider(c.Metadata, context.Background())
	if err != nil {
		return fmt.Errorf("fetch training set variant offline provider: %v", err)
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return fmt.Errorf("fetch offline store interface of training set provider: %v", err)
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		return fmt.Errorf("convert training set provider to offline store interface: %v", err)
	}
	defer func(store provider.OfflineStore) {
		err := store.Close()
		if err != nil {
			c.Logger.Errorf("could not close offline store: %v", err)
		}
	}(store)
	providerResID := provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.TrainingSet}

	if _, err := store.GetTrainingSet(providerResID); err == nil {
		return fmt.Errorf("training set (%v) already exists: %v", resID, err)
	}
	features := ts.Features()
	featureList := make([]provider.ResourceID, len(features))
	for i, feature := range features {
		featureList[i] = provider.ResourceID{Name: feature.Name, Variant: feature.Variant, Type: provider.Feature}
		featureResource, err := c.Metadata.GetFeatureVariant(context.Background(), feature)
		if err != nil {
			return fmt.Errorf("failed to get fetch dependent feature: %v", err)
		}
		sourceNameVariant := featureResource.Source()
		_, err = c.AwaitPendingSource(sourceNameVariant)
		if err != nil {
			return fmt.Errorf("source of feature could not complete job: %v", err)
		}
		_, err = c.AwaitPendingFeature(metadata.NameVariant{feature.Name, feature.Variant})
		if err != nil {
			return fmt.Errorf("feature could not complete job: %v", err)
		}
	}

	lagFeatures := ts.LagFeatures()
	lagFeaturesList := make([]provider.LagFeatureDef, len(lagFeatures))
	for i, lagFeature := range lagFeatures {
		lagFeaturesList[i] = provider.LagFeatureDef{
			FeatureName:    lagFeature.GetFeature(),
			FeatureVariant: lagFeature.GetVariant(),
			LagName:        lagFeature.GetName(),
			LagDelta:       lagFeature.GetLag().AsDuration(), // see if need to convert it to time.Duration
		}
	}

	label, err := ts.FetchLabel(c.Metadata, context.Background())
	if err != nil {
		return fmt.Errorf("fetch training set label: %v", err)
	}
	labelSourceNameVariant := label.Source()
	_, err = c.AwaitPendingSource(labelSourceNameVariant)
	if err != nil {
		return fmt.Errorf("source of label could not complete job: %v", err)
	}
	label, err = c.AwaitPendingLabel(metadata.NameVariant{label.Name(), label.Variant()})
	if err != nil {
		return fmt.Errorf("label could not complete job: %v", err)
	}
	trainingSetDef := provider.TrainingSetDef{
		ID:          providerResID,
		Label:       provider.ResourceID{Name: label.Name(), Variant: label.Variant(), Type: provider.Label},
		Features:    featureList,
		LagFeatures: lagFeaturesList,
	}
	tsRunnerConfig := runner.TrainingSetRunnerConfig{
		OfflineType:   pt.Type(providerEntry.Type()),
		OfflineConfig: providerEntry.SerializedConfig(),
		Def:           trainingSetDef,
		IsUpdate:      false,
	}
	serialized, _ := tsRunnerConfig.Serialize()
	jobRunner, err := c.Spawner.GetJobRunner(runner.CREATE_TRAINING_SET, serialized, resID)
	if err != nil {
		return fmt.Errorf("failed to run training set job runner: %v", err)
	}
	completionWatcher, err := jobRunner.Run()
	if err != nil {
		return fmt.Errorf("failed to start training set job: %v", err)
	}
	if err := completionWatcher.Wait(); err != nil {
		return fmt.Errorf("training set job failed to complete: %v", err)
	}
	if err := c.Metadata.SetStatus(context.Background(), resID, metadata.READY, ""); err != nil {
		return fmt.Errorf("failed to set training set status: %v", err)
	}
	if schedule != "" {
		scheduleTrainingSetRunnerConfig := runner.TrainingSetRunnerConfig{
			OfflineType:   pt.Type(providerEntry.Type()),
			OfflineConfig: providerEntry.SerializedConfig(),
			Def:           trainingSetDef,
			IsUpdate:      true,
		}
		serializedUpdate, err := scheduleTrainingSetRunnerConfig.Serialize()
		if err != nil {
			return fmt.Errorf("serialize training set schedule runner config: %v", err)
		}
		jobRunnerUpdate, err := c.Spawner.GetJobRunner(runner.CREATE_TRAINING_SET, serializedUpdate, resID)
		if err != nil {
			return fmt.Errorf("spawn training set job runner: %v", err)
		}
		cronRunner, isCronRunner := jobRunnerUpdate.(kubernetes.CronRunner)
		if !isCronRunner {
			return fmt.Errorf("kubernetes runner does not implement schedule")
		}
		if err := cronRunner.ScheduleJob(kubernetes.CronSchedule(schedule)); err != nil {
			return fmt.Errorf("schedule training set job in kubernetes: %v", err)
		}
		if err := c.Metadata.SetStatus(context.Background(), resID, metadata.READY, ""); err != nil {
			return fmt.Errorf("update training set scheduler job status: %v", err)
		}
	}
	return nil
}

func (c *Coordinator) getJob(mtx *concurrency.Mutex, key string) (*metadata.CoordinatorJob, error) {
	c.Logger.Debugf("Checking existence of job with key %s\n", key)
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
		return nil, JobDoesNotExistError{key: key}
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
	c.Logger.Info("Deleting job with key: ", key)
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
	c.Logger.Info("Succesfully deleted job with key: ", key)
	return nil
}

func (c *Coordinator) hasJob(id metadata.ResourceID) (bool, error) {
	getResp, err := (*c.KVClient).Get(context.Background(), metadata.GetJobKey(id), clientv3.WithPrefix())
	if err != nil {
		return false, fmt.Errorf("fetch jobs from etcd with prefix %s: %v", metadata.GetJobKey(id), err)
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
		c.Logger.Errorw("could not create job lock restarting.....", "error", err)
		os.Exit(1)
	}
	return mtx, nil
}

func (c *Coordinator) ExecuteJob(jobKey string) error {
	c.Logger.Info("Executing new job with key ", jobKey)
	s, err := concurrency.NewSession(c.EtcdClient, concurrency.WithTTL(10000000))
	if err != nil {
		return fmt.Errorf("new session: %v", err)
	}
	defer s.Close()
	mtx, err := c.createJobLock(jobKey, s)
	if err != nil {
		return fmt.Errorf("job lock: %v", err)
	}
	defer func() {
		if err := mtx.Unlock(context.Background()); err != nil {
			c.Logger.Debugw("Error unlocking mutex:", "error", err)
		}
	}()
	job, err := c.getJob(mtx, jobKey)
	if err != nil {
		return err
	}
	c.Logger.Debugf("Job %s is on attempt %d", jobKey, job.Attempts)
	if job.Attempts > MAX_ATTEMPTS {
		if err := c.deleteJob(mtx, jobKey); err != nil {
			c.Logger.Debugw("Error deleting job", "error", err)
			return fmt.Errorf("job delete: %v", err)
		}
		return fmt.Errorf("job failed after %d attempts. Cancelling coordinator flow", MAX_ATTEMPTS)
	}
	if err := c.incrementJobAttempts(mtx, job, jobKey); err != nil {
		return fmt.Errorf("increment attempt: %v", err)
	}
	type jobFunction func(metadata.ResourceID, string) error
	fns := map[metadata.ResourceType]jobFunction{
		metadata.TRAINING_SET_VARIANT: c.runTrainingSetJob,
		metadata.FEATURE_VARIANT:      c.runFeatureMaterializeJob,
		metadata.LABEL_VARIANT:        c.runLabelRegisterJob,
		metadata.SOURCE_VARIANT:       c.runRegisterSourceJob,
	}
	jobFunc, has := fns[job.Resource.Type]
	if !has {
		return fmt.Errorf("not a valid resource type for running jobs")
	}

	if err := jobFunc(job.Resource, job.Schedule); err != nil {
		switch err.(type) {
		case ResourceAlreadyFailedError:
			return err
		default:
			statusErr := c.Metadata.SetStatus(context.Background(), job.Resource, metadata.FAILED, err.Error())
			return fmt.Errorf("%s job failed: %w: %v", job.Resource.Type, err, statusErr)
		}
	}
	c.Logger.Info("Successfully executed job with key: ", jobKey)
	if err := c.deleteJob(mtx, jobKey); err != nil {
		c.Logger.Debugw("Error deleting job", "error", err)
		return fmt.Errorf("job delete: %v", err)
	}
	return nil
}

type ResourceUpdatedEvent struct {
	ResourceID metadata.ResourceID
	Completed  time.Time
}

func (c *ResourceUpdatedEvent) Serialize() (Config, error) {
	config, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return config, nil
}

func (c *ResourceUpdatedEvent) Deserialize(config Config) error {
	err := json.Unmarshal(config, c)
	if err != nil {
		return fmt.Errorf("deserialize resource update event: %v", err)
	}
	return nil
}

func (c *Coordinator) signalResourceUpdate(key string, value string) error {
	c.Logger.Info("Updating metdata with latest resource update status and time", key)
	s, err := concurrency.NewSession(c.EtcdClient, concurrency.WithTTL(1))
	if err != nil {
		return fmt.Errorf("create new concurrency session for resource update job: %v", err)
	}
	defer s.Close()
	mtx, err := c.createJobLock(key, s)
	if err != nil {
		return fmt.Errorf("create lock on resource update job with key %s: %v", key, err)
	}
	defer func() {
		if err := mtx.Unlock(context.Background()); err != nil {
			c.Logger.Debugw("Error unlocking mutex:", "error", err)
		}
	}()
	resUpdatedEvent := &ResourceUpdatedEvent{}
	if err := resUpdatedEvent.Deserialize(Config(value)); err != nil {
		return fmt.Errorf("deserialize resource update event: %v", err)
	}
	if err := c.Metadata.SetStatus(context.Background(), resUpdatedEvent.ResourceID, metadata.READY, ""); err != nil {
		return fmt.Errorf("set resource update status: %v", err)
	}
	c.Logger.Info("Succesfully set update status for update job with key: ", key)
	if err := c.deleteJob(mtx, key); err != nil {
		return fmt.Errorf("delete resource update job: %v", err)
	}
	return nil
}

func (c *Coordinator) changeJobSchedule(key string, value string) error {
	c.Logger.Info("Updating schedule of currently made cronjob in kubernetes: ", key)
	s, err := concurrency.NewSession(c.EtcdClient, concurrency.WithTTL(1))
	if err != nil {
		return fmt.Errorf("create new concurrency session for resource update job: %v", err)
	}
	defer func(s *concurrency.Session) {
		err := s.Close()
		if err != nil {
			c.Logger.Debugw("Error closing scheduling session", "error", err)
		}
	}(s)
	mtx, err := c.createJobLock(key, s)
	if err != nil {
		return fmt.Errorf("create lock on resource update job with key %s: %v", key, err)
	}
	defer func() {
		if err := mtx.Unlock(context.Background()); err != nil {
			c.Logger.Debugw("Error unlocking mutex:", "error", err)
		}
	}()
	coordinatorScheduleJob := &metadata.CoordinatorScheduleJob{}
	if err := coordinatorScheduleJob.Deserialize(Config(value)); err != nil {
		return fmt.Errorf("deserialize coordinator schedule job: %v", err)
	}
	namespace, err := kubernetes.GetCurrentNamespace()
	if err != nil {
		return fmt.Errorf("could not get kubernetes namespace: %v", err)
	}
	jobName := kubernetes.CreateJobName(coordinatorScheduleJob.Resource)
	jobClient, err := kubernetes.NewKubernetesJobClient(jobName, namespace)
	if err != nil {
		return fmt.Errorf("create new kubernetes job client: %v", err)
	}
	cronJob, err := jobClient.GetCronJob()
	if err != nil {
		return fmt.Errorf("fetch cron job from kubernetes with name %s: %v", jobName, err)
	}
	cronJob.Spec.Schedule = coordinatorScheduleJob.Schedule
	if _, err := jobClient.UpdateCronJob(cronJob); err != nil {
		return fmt.Errorf("update kubernetes cron job: %v", err)
	}
	if err := c.Metadata.SetStatus(context.Background(), coordinatorScheduleJob.Resource, metadata.READY, ""); err != nil {
		return fmt.Errorf("set schedule job update status in metadata: %v", err)
	}
	c.Logger.Info("Successfully updated schedule for job in kubernetes with key: ", key)
	if err := c.deleteJob(mtx, key); err != nil {
		return fmt.Errorf("delete update schedule job in etcd: %v", err)
	}
	return nil
}
