package tasks

import (
	"context"
	"fmt"
	"github.com/featureform/fferr"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/runner"
	"github.com/featureform/scheduling"
	db "github.com/jackc/pgx/v4"
	"strings"
	"time"
)

type SourceTask struct {
	BaseTask
}

func (t *SourceTask) Run() error {
	nv, ok := t.taskDef.Target.(scheduling.NameVariant)
	if !ok {
		return fferr.NewInternalErrorf("cannot create a source from target type: %s", t.taskDef.TargetType)
	}

	t.logger.Info("Running register source job on resource: ", nv)
	err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Metadata...")
	if err != nil {
		return err
	}
	source, err := t.metadata.GetSourceVariant(context.Background(), metadata.NameVariant{nv.Name, nv.Variant})
	if err != nil {
		return err
	}
	if err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Provider..."); err != nil {
		return err
	}
	sourceProvider, err := source.FetchProvider(t.metadata, context.Background())
	if err != nil {
		return err
	}
	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, fmt.Sprintf("Initializing Offline Store: %s...", sourceProvider.Type()))
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
	resID := metadata.ResourceID{Name: nv.Name, Variant: nv.Variant, Type: metadata.SOURCE_VARIANT}
	if source.IsSQLTransformation() {
		return t.runSQLTransformationJob(source, resID, sourceStore, sourceProvider)
	} else if source.IsDFTransformation() {
		return t.runDFTransformationJob(source, resID, sourceStore, sourceProvider)
	} else if source.IsPrimaryDataSQLTable() {
		return t.runPrimaryTableJob(source, resID, sourceStore)
	} else {
		return fferr.NewInternalError(fmt.Errorf("source type not implemented"))
	}
}

func (t *SourceTask) runSQLTransformationJob(transformSource *metadata.SourceVariant, resID metadata.ResourceID, offlineStore provider.OfflineStore, sourceProvider *metadata.Provider) error {
	t.logger.Info("Running SQL transformation job on resource: ", resID)
	err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Queries...")
	if err != nil {
		return err
	}
	templateString := transformSource.SQLTransformationQuery()
	sources := transformSource.SQLTransformationSources()

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for dependent jobs to complete..."); err != nil {
		return err
	}

	if err := t.verifyCompletionOfSources(sources); err != nil {
		return err
	}

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Mapping Name Variants to Tables..."); err != nil {
		return err
	}

	sourceMap, err := t.mapNameVariantsToTables(sources)
	if err != nil {
		return err
	}
	sourceMapping, err := getSourceMapping(templateString, sourceMap)
	if err != nil {
		return err
	}

	var query string
	query, err = templateReplace(templateString, sourceMap, offlineStore)
	if err != nil {
		return err
	}

	t.logger.Debugw("Created transformation query", "query", query)
	providerResourceID := provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.Transformation}
	transformationConfig := provider.TransformationConfig{
		Type:          provider.SQLTransformation,
		TargetTableID: providerResourceID,
		Query:         query,
		SourceMapping: sourceMapping,
		Args:          transformSource.TransformationArgs(),
	}

	if err := t.runTransformationJob(transformationConfig, resID, sourceProvider); err != nil {
		return err
	}

	return nil
}

func (t *SourceTask) runDFTransformationJob(transformSource *metadata.SourceVariant, resID metadata.ResourceID, offlineStore provider.OfflineStore, sourceProvider *metadata.Provider) error {
	t.logger.Info("Running DF transformation job on resource: ", resID)
	err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Queries...")
	if err != nil {
		return err
	}
	code := transformSource.DFTransformationQuery()
	sources := transformSource.DFTransformationSources()

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for dependent jobs to complete...")
	if err != nil {
		return err
	}

	err = t.verifyCompletionOfSources(sources)
	if err != nil {
		return err
	}

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Mapping Name Variants to Tables...")
	if err != nil {
		return err
	}

	sourceMap, err := t.mapNameVariantsToTables(sources)
	if err != nil {
		return err
	}

	sourceMapping, err := getOrderedSourceMappings(sources, sourceMap)
	if err != nil {
		return err
	}

	t.logger.Debugw("Created transformation query")
	providerResourceID := provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.Transformation}
	transformationConfig := provider.TransformationConfig{
		Type:          provider.DFTransformation,
		TargetTableID: providerResourceID,
		Code:          code,
		SourceMapping: sourceMapping,
		Args:          transformSource.TransformationArgs(),
	}

	err = t.runTransformationJob(transformationConfig, resID, sourceProvider)
	if err != nil {
		return err
	}

	return nil
}

func (t *SourceTask) runTransformationJob(transformationConfig provider.TransformationConfig, resID metadata.ResourceID, sourceProvider *metadata.Provider) error {
	createTransformationConfig := runner.CreateTransformationConfig{
		OfflineType:          pt.Type(sourceProvider.Type()),
		OfflineConfig:        sourceProvider.SerializedConfig(),
		TransformationConfig: transformationConfig,
		IsUpdate:             false,
	}
	t.logger.Debugw("Transformation Serialize Config")
	serialized, err := createTransformationConfig.Serialize()
	if err != nil {
		return err
	}
	t.logger.Debugw("Transformation Get Job Runner")

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching job runner...")
	if err != nil {
		return err
	}

	jobRunner, err := t.Spawner.GetJobRunner(runner.CREATE_TRANSFORMATION, serialized, resID)
	if err != nil {
		return err
	}
	t.logger.Debugw("Transformation Run Job")
	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Starting Transformation...")
	if err != nil {
		return err
	}
	completionWatcher, err := jobRunner.Run()
	if err != nil {
		return err
	}
	t.logger.Debugw("Transformation Waiting For Completion")

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for Transformation to complete...")
	if err != nil {
		return err
	}
	if err := completionWatcher.Wait(); err != nil {
		return err
	}
	t.logger.Debugw("Transformation Complete")

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Transformation Complete.")
	if err != nil {
		return err
	}
	return nil
}

func (t *SourceTask) runPrimaryTableJob(source *metadata.SourceVariant, resID metadata.ResourceID, offlineStore provider.OfflineStore) error {
	t.logger.Info("Running primary table job on resource: ", resID)
	providerResourceID := provider.ResourceID{Name: resID.Name, Variant: resID.Variant, Type: provider.Primary}
	if !source.IsPrimaryDataSQLTable() {
		return fferr.NewInvalidArgumentError(fmt.Errorf("%s is not a primary table", source.Name()))
	}
	sourceName := source.PrimaryDataSQLTableName()
	if sourceName == "" {
		return fferr.NewInvalidArgumentError(fmt.Errorf("source name is not set"))
	}
	err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Starting Registration...")
	if err != nil {
		return err
	}
	if _, err := offlineStore.RegisterPrimaryFromSourceTable(providerResourceID, sourceName); err != nil {
		return err
	}
	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Registration Complete.")
	if err != nil {
		return err
	}
	return nil
}

func (t *SourceTask) mapNameVariantsToTables(sources []metadata.NameVariant) (map[string]string, error) {
	sourceMap := make(map[string]string)
	for _, nameVariant := range sources {
		source, err := t.metadata.GetSourceVariant(context.Background(), nameVariant)
		if err != nil {
			return nil, err
		}
		if source.Status() != scheduling.READY {
			return nil, fferr.NewResourceNotReadyError(source.Name(), source.Variant(), "SOURCE_VARIANT", nil)
		}
		providerResourceID := provider.ResourceID{Name: source.Name(), Variant: source.Variant()}
		var tableName string
		sourceProvider, err := source.FetchProvider(t.metadata, context.Background())
		if err != nil {
			return nil, err
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

func (t *SourceTask) verifyCompletionOfSources(sources []metadata.NameVariant) error {
	allReady := false
	for !allReady {
		sourceVariants, err := t.metadata.GetSourceVariants(context.Background(), sources)
		if err != nil {
			return err
		}
		total := len(sourceVariants)
		totalReady := 0
		for _, sourceVariant := range sourceVariants {
			if sourceVariant.Status() == scheduling.READY {
				totalReady += 1
			}
			if sourceVariant.Status() == scheduling.FAILED {
				wrapped := fferr.NewResourceFailedError(sourceVariant.Name(), sourceVariant.Variant(), fferr.SOURCE_VARIANT, fmt.Errorf("required dataset is in a failed state"))
				wrapped.AddDetail("resource_status", sourceVariant.Status().String())
				return wrapped
			}
		}
		allReady = total == totalReady
		time.Sleep(1 * time.Second)
	}
	return nil
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
			return nil, fferr.NewInvalidArgumentError(fmt.Errorf("value %s not found in replacements: %v", key, replacements))
		}
		sourceMap = append(sourceMap, provider.SourceMapping{Template: sanitize(replacement), Source: replacement})
		template = afterSplit[1]
	}
	return sourceMap, nil
}

func getOrderedSourceMappings(sources []metadata.NameVariant, sourceMap map[string]string) ([]provider.SourceMapping, error) {
	sourceMapping := make([]provider.SourceMapping, len(sources))
	for i, nv := range sources {
		sourceKey := nv.ClientString()
		tableName, hasKey := sourceMap[sourceKey]
		if !hasKey {
			return nil, fferr.NewInternalError(fmt.Errorf("key %s not in source map", sourceKey))
		}
		sourceMapping[i] = provider.SourceMapping{Template: sourceKey, Source: tableName}
	}
	return sourceMapping, nil
}

func templateReplace(template string, replacements map[string]string, offlineStore provider.OfflineStore) (string, error) {
	formattedString := ""
	numEscapes := strings.Count(template, "{{")
	for i := 0; i < numEscapes; i++ {
		split := strings.SplitN(template, "{{", 2)
		afterSplit := strings.SplitN(split[1], "}}", 2)
		key := strings.TrimSpace(afterSplit[0])
		replacement, has := replacements[key]
		if !has {
			return "", fferr.NewInvalidArgumentError(fmt.Errorf("value %s not found in replacements: %v", key, replacements))
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

func sanitize(ident string) string {
	return db.Identifier{ident}.Sanitize()
}
