package tasks

import (
	"context"
	"fmt"
	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/runner"
	"github.com/featureform/scheduling"
	"go.uber.org/zap"
)

type FeatureTask struct {
	BaseTask
	logger *zap.SugaredLogger
}

func (t *FeatureTask) Run() error {
	taskMetadata, err := t.metadata.Tasks.GetTaskByID(t.taskDef.TaskId)
	if err != nil {
		return err
	}

	nv, ok := taskMetadata.Target.(scheduling.NameVariant)
	if !ok {
		return fferr.NewInternalErrorf("cannot create a source from target type: %s", taskMetadata.TargetType)
	}

	t.logger.Info("Running feature materialization job on resource: ", nv)

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Feature details...")
	if err != nil {
		return err
	}

	feature, err := t.metadata.GetFeatureVariant(context.Background(), metadata.NameVariant{Name: nv.Name, Variant: nv.Variant})
	if err != nil {
		return err
	}
	t.logger.Infow("feature variant", "name", feature.Name(), "source", feature.Source(), "location", feature.Location(), "location_col", feature.LocationColumns())

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for dependencies to complete...")
	if err != nil {
		return err
	}

	sourceNameVariant := feature.Source()
	source, err := t.AwaitPendingSource(sourceNameVariant)
	if err != nil {
		return err
	}

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Offline Store...")
	if err != nil {
		return err
	}

	sourceProvider, err := source.FetchProvider(t.metadata, context.Background())
	if err != nil {
		return err
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
			t.logger.Errorf("could not close offline store: %v", err)
		}
	}(sourceStore)

	var featureProvider *metadata.Provider
	if feature.Provider() != "" {
		featureProvider, err = feature.FetchProvider(t.metadata, context.Background())
		if err != nil {
			return err
		}
	}

	var vType provider.ValueType
	if feature.IsEmbedding() {
		vType = provider.VectorType{
			ScalarType:  provider.ScalarType(feature.Type()),
			Dimension:   feature.Dimension(),
			IsEmbedding: true,
		}
	} else {
		vType = provider.ScalarType(feature.Type())
	}

	var sourceTableName string
	var sourceResourceID provider.ResourceID
	if source.IsSQLTransformation() || source.IsDFTransformation() {
		sourceResourceID = provider.ResourceID{Name: sourceNameVariant.Name, Variant: sourceNameVariant.Variant, Type: provider.Transformation}
		sourceTable, err := sourceStore.GetTransformationTable(sourceResourceID)
		if err != nil {
			return err
		}
		sourceTableName = sourceTable.GetName()
	} else if source.IsPrimaryDataSQLTable() {
		sourceResourceID = provider.ResourceID{Name: sourceNameVariant.Name, Variant: sourceNameVariant.Variant, Type: provider.Primary}
		sourceTable, err := sourceStore.GetPrimaryTable(sourceResourceID)
		if err != nil {
			return err
		}
		sourceTableName = sourceTable.GetName()
	}

	featID := provider.ResourceID{
		Name:    nv.Name,
		Variant: nv.Variant,
		Type:    provider.Feature,
	}
	tmpSchema := feature.LocationColumns().(metadata.ResourceVariantColumns)
	schema := provider.ResourceSchema{
		Entity:      tmpSchema.Entity,
		Value:       tmpSchema.Value,
		TS:          tmpSchema.TS,
		SourceTable: sourceTableName,
	}
	t.logger.Debugw("Creating Resource Table", "id", featID, "schema", schema)

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Registering Feature from dataset...")
	if err != nil {
		return err
	}

	_, err = sourceStore.RegisterResourceFromSourceTable(featID, schema)
	if err != nil {
		return err
	}
	t.logger.Debugw("Resource Table Created", "id", featID, "schema", schema)

	materializedRunnerConfig := runner.MaterializedRunnerConfig{
		OfflineType:   pt.Type(sourceProvider.Type()),
		OfflineConfig: sourceProvider.SerializedConfig(),
		ResourceID:    provider.ResourceID{Name: nv.Name, Variant: nv.Variant, Type: provider.Feature},
		VType:         provider.ValueTypeJSONWrapper{ValueType: vType},
		Cloud:         runner.LocalMaterializeRunner,
		IsUpdate:      false,
	}

	if featureProvider != nil {
		materializedRunnerConfig.OnlineType = pt.Type(featureProvider.Type())
		materializedRunnerConfig.OnlineConfig = featureProvider.SerializedConfig()
	} else {
		materializedRunnerConfig.OnlineType = pt.NONE
	}

	isImportToS3Enabled, err := t.checkS3Import(featureProvider)
	if err != nil {
		return err
	}

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Starting Materialization...")
	if err != nil {
		return err
	}

	var materializationErr error
	resID := metadata.ResourceID{Name: nv.Name, Variant: nv.Variant, Type: metadata.FEATURE_VARIANT}
	if isImportToS3Enabled {
		materializationErr = t.materializeFeatureViaS3Import(resID, materializedRunnerConfig, sourceStore)
	} else {
		materializationErr = t.materializeFeature(resID, materializedRunnerConfig)
	}
	if materializationErr != nil {
		return materializationErr
	}

	t.logger.Debugw("Setting status to ready", "id", featID)
	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Materialization Complete...")
	if err != nil {
		return err
	}
	return nil
}

func (t *FeatureTask) checkS3Import(featureProvider *metadata.Provider) (bool, error) {
	if featureProvider != nil && featureProvider.Type() == string(pt.DynamoDBOnline) {
		t.logger.Debugw("Feature provider is DynamoDB")
		config := pc.DynamodbConfig{}
		if err := config.Deserialize(featureProvider.SerializedConfig()); err != nil {
			return false, err
		}
		return config.ImportFromS3, nil
	}
	return false, nil
}

func (t *FeatureTask) materializeFeatureViaS3Import(id metadata.ResourceID, config runner.MaterializedRunnerConfig, sourceStore provider.OfflineStore) error {
	t.logger.Infow("Materializing Feature Via S3 Import", "id", id)
	err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Starting Materialization via S3 to Dynamo Import...")
	if err != nil {
		return err
	}
	sparkOfflineStore, isSparkOfflineStore := sourceStore.(*provider.SparkOfflineStore)
	if !isSparkOfflineStore {
		return fferr.NewInvalidArgumentError(fmt.Errorf("offline store is not spark offline store"))
	}
	if sparkOfflineStore.Store.FilestoreType() != filestore.S3 {
		return fferr.NewInvalidArgumentError(fmt.Errorf("offline file store must be S3; %s is not supported", sparkOfflineStore.Store.FilestoreType()))
	}
	serialized, err := config.Serialize()
	if err != nil {
		return err
	}
	jobRunner, err := t.Spawner.GetJobRunner(runner.S3_IMPORT_DYNAMODB, serialized, id)
	if err != nil {
		return err
	}
	completionWatcher, err := jobRunner.Run()
	if err != nil {
		return err
	}

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for Materialization to complete...")
	if err != nil {
		return err
	}

	if err := completionWatcher.Wait(); err != nil {
		return err
	}
	t.logger.Info("Successfully materialized feature via S3 import to DynamoDB", "id", id)
	return nil
}

func (t *FeatureTask) materializeFeature(id metadata.ResourceID, config runner.MaterializedRunnerConfig) error {
	t.logger.Infow("Starting Feature Materialization", "id", id)
	err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Starting Materialization via Copy...")
	if err != nil {
		return err
	}
	serialized, err := config.Serialize()
	if err != nil {
		return err
	}
	jobRunner, err := t.Spawner.GetJobRunner(runner.MATERIALIZE, serialized, id)
	if err != nil {
		return err
	}
	completionWatcher, err := jobRunner.Run()
	if err != nil {
		return err
	}

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for Materialization to complete...")
	if err != nil {
		return err
	}

	if err := completionWatcher.Wait(); err != nil {
		return err
	}
	return nil
}
