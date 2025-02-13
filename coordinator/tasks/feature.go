// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package tasks

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/featureform/logging"
	"github.com/featureform/provider/provider_schema"

	"github.com/featureform/fferr"
	"github.com/featureform/filestore"
	"github.com/featureform/helpers"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pl "github.com/featureform/provider/location"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
	"github.com/featureform/runner"
	"github.com/featureform/scheduling"
)

type FeatureTask struct {
	BaseTask
}

func (t *FeatureTask) Run() error {
	_, ctx, logger := t.logger.InitializeRequestID(context.TODO())
	logger.Infow("Running Feature Task")
	nv, ok := t.taskDef.Target.(scheduling.NameVariant)
	if !ok {
		return fferr.NewInternalErrorf("cannot create a feature from target type: %s", t.taskDef.TargetType)
	}

	logger = logger.WithResource(logging.FeatureVariant, nv.Name, nv.Variant).
		With("task_id", t.taskDef.TaskId, "task_run_id", t.taskDef.ID)

	resID := metadata.ResourceID{Name: nv.Name, Variant: nv.Variant, Type: metadata.FEATURE_VARIANT}
	if t.isDelete {
		logger.Debugw("Handling deletion")
		return t.handleDeletion(ctx, resID, logger)
	}

	logger.Info("Running feature materialization job on resource: ", nv)
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Feature details..."); err != nil {
		return err
	}

	feature, err := t.metadata.GetFeatureVariant(ctx, metadata.NameVariant{Name: nv.Name, Variant: nv.Variant})
	if err != nil {
		return err
	}
	logger.Infow("Running task", "source", feature.Source(), "location", feature.Location(), "location_col", feature.LocationColumns())
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for dependencies to complete..."); err != nil {
		return err
	}

	sourceNameVariant := feature.Source()
	source, err := t.awaitPendingSource(sourceNameVariant)
	if err != nil {
		return err
	}

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Offline Store..."); err != nil {
		return err
	}

	sourceProvider, err := source.FetchProvider(t.metadata, ctx)
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

	var inferenceStore *metadata.Provider
	if feature.Provider() != "" {
		inferenceStore, err = feature.FetchProvider(t.metadata, ctx)
		if err != nil {
			return err
		}
	}

	vType, typeErr := feature.Type()
	if typeErr != nil {
		return typeErr
	}

	logger.Debugw("Getting feature's source location")
	var sourceLocation pl.Location
	var sourceLocationErr error
	if source.IsSQLTransformation() || source.IsDFTransformation() {
		sourceLocation, sourceLocationErr = source.GetTransformationLocation()
	} else if source.IsPrimaryData() {
		sourceLocation, sourceLocationErr = source.GetPrimaryLocation()
	}

	if sourceLocationErr != nil {
		return sourceLocationErr
	}
	logger = logger.With("source_location", sourceLocation, "source_location_type", sourceLocation.Type())
	logger.Debugw("Feature's source location")
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
		SourceTable: sourceLocation,
		EntityMappings: metadata.EntityMappings{
			Mappings: []metadata.EntityMapping{
				{Name: feature.Entity(), EntityColumn: tmpSchema.Entity},
			},
			ValueColumn: tmpSchema.Value,
		},
	}
	logger = logger.With("schema", schema)
	logger.Debugw("Creating Resource Table")
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Registering Feature from dataset..."); err != nil {
		return err
	}

	if _, err := sourceStore.RegisterResourceFromSourceTable(featID, schema); err != nil {
		return err
	}
	logger.Debugw("Resource Table Created")

	maxJobDurationEnv := helpers.GetEnv("MAX_JOB_DURATION", "48h")
	maxJobDuration, err := time.ParseDuration(maxJobDurationEnv)

	if err != nil {
		logger.Errorw("Failed to parse MAX_JOB_DURATION", "error", err)
		return fferr.NewInternalErrorf("could not parse MAX_JOB_DURATION: %v", err)
	}

	resourceSnowflakeConfig := &metadata.ResourceSnowflakeConfig{}
	if sourceStore.Type() == pt.SnowflakeOffline {
		tempConfig, err := feature.ResourceSnowflakeConfig()
		if err != nil {
			return err
		}
		resourceSnowflakeConfig = tempConfig
	}

	providerResID := provider.ResourceID{Name: nv.Name, Variant: nv.Variant, Type: provider.Feature}
	materializedRunnerConfig := runner.MaterializedRunnerConfig{
		OfflineType:   pt.Type(sourceProvider.Type()),
		OfflineConfig: sourceProvider.SerializedConfig(),
		ResourceID:    providerResID,
		VType:         types.ValueTypeJSONWrapper{ValueType: vType},
		Cloud:         runner.LocalMaterializeRunner,
		IsUpdate:      t.isUpdate,
		Options: provider.MaterializationOptions{
			Output:                  filestore.Parquet,
			ShouldIncludeHeaders:    true,
			MaxJobDuration:          maxJobDuration,
			JobName:                 fmt.Sprintf("featureform-materialization--%s--%s", nv.Name, nv.Variant),
			ResourceSnowflakeConfig: resourceSnowflakeConfig,
			Schema:                  schema,
		},
	}

	if inferenceStore != nil {
		materializedRunnerConfig.OnlineType = pt.Type(inferenceStore.Type())
		materializedRunnerConfig.OnlineConfig = inferenceStore.SerializedConfig()
	} else {
		materializedRunnerConfig.OnlineType = pt.NONE
	}

	supportsDirectCopy := false
	var onlineStore provider.OnlineStore
	if inferenceStore != nil {
		onlineProvider, err := provider.Get(pt.Type(inferenceStore.Type()), inferenceStore.SerializedConfig())
		if err != nil {
			logger.Errorw("Failed to get online provider", "error", err)
			return err
		}
		casted, err := onlineProvider.AsOnlineStore()
		if err != nil {
			logger.Errorw("Failed to cast provider as online store", "error", err)
			return err
		}
		onlineStore = casted
		// Direct copy means the provider can copy to the online store itself
		matOpt := provider.DirectCopyOptionType(onlineStore)
		supports, err := sourceStore.SupportsMaterializationOption(matOpt)
		if err != nil {
			return err
		}
		supportsDirectCopy = supports
	}

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Starting Materialization..."); err != nil {
		return err
	}

	var materializationErr error
	if supportsDirectCopy {
		if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Materializing via direct copy..."); err != nil {
			return err
		}
		// Create the table to copy into
		if _, err := onlineStore.CreateTable(nv.Name, nv.Variant, vType); err != nil {
			_, isTableExistsErr := err.(*fferr.DatasetAlreadyExistsError)
			if !isTableExistsErr {
				return err
			}
		}
		_, materializationErr = sourceStore.CreateMaterialization(providerResID, provider.MaterializationOptions{
			MaxJobDuration: maxJobDuration,
			JobName:        fmt.Sprintf("featureform-materialization--%s--%s", nv.Name, nv.Variant),
			DirectCopyTo:   onlineStore,
		})
	} else {
		materializationErr = t.materializeFeature(resID, materializedRunnerConfig)
	}
	if materializationErr != nil {
		return materializationErr
	}

	logger.Debugw("Setting status to ready")
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Materialization Complete..."); err != nil {
		return err
	}
	return nil
}

func (t *FeatureTask) handleDeletion(ctx context.Context, resID metadata.ResourceID, logger logging.Logger) error {
	logger.Infow("Deleting feature")
	featureTableName, tableNameErr := provider_schema.ResourceToTableName(provider_schema.Materialization, resID.Name, resID.Variant)
	if tableNameErr != nil {
		logger.Errorw("Failed to get table name for feature", "error", tableNameErr)
		return tableNameErr
	}
	featureLocation := pl.NewSQLLocation(featureTableName)
	logger = logger.With("location", featureLocation)
	logger.Debugw("Deleting feature at location")

	nv := metadata.NameVariant{
		Name:    resID.Name,
		Variant: resID.Variant,
	}

	logger.Debugw("Fetching feature that's staged for deletion")
	featureToDelete, err := t.metadata.GetStagedForDeletionFeatureVariant(ctx, nv, logger)
	if err != nil {
		logger.Errorw("Failed to fetch feature staged for deletion", "error", err)
		return err
	}

	logger.Debugf("Deleting feature at location")
	offlineStoreLocations := featureToDelete.GetOfflineStoreLocations()
	logger = logger.With("offline_store_locations", offlineStoreLocations)
	logger.Debug("Getting offline store")
	sourceStore, err := getOfflineStore(ctx, t.BaseTask, t.metadata, &offlineProviderFeatureAdapter{feature: featureToDelete}, logger)
	if err != nil {
		logger.Errorw("Failed to get store", "error", err)
		return err
	}

	logger.Debug("Deleting feature at locations", "locations", offlineStoreLocations)
	for _, offlineStoreLocation := range offlineStoreLocations {
		logger = logger.With("location", offlineStoreLocation)
		logger.Debugw("Deleting feature at location")
		proto, fromProtoErr := pl.FromProto(offlineStoreLocation)
		if fromProtoErr != nil {
			logger.Errorw("Failed to convert location to proto", "error", fromProtoErr)
			return fromProtoErr
		}
		if deleteErr := sourceStore.Delete(proto); deleteErr != nil {
			var notFoundErr *fferr.DatasetNotFoundError
			if errors.As(deleteErr, &notFoundErr) {
				logger.Info("Table doesn't exist at location, continuing...")
			} else {
				logger.Errorw("Failed to delete feature from offline store", "error", deleteErr)
				return deleteErr
			}
		}
	}

	logger.Info("Successfully deleted feature from offline store")
	deleteFromOnlineStoreErr := t.deleteFromOnlineStore(ctx, featureToDelete, logger, nv)
	if deleteFromOnlineStoreErr != nil {
		logger.Errorw("Failed to delete feature from online store", "error", deleteFromOnlineStoreErr)
		return deleteFromOnlineStoreErr
	}
	logger.Info("Successfully deleted feature from online store")

	logger.Debug("Finalizing delete")
	if err := t.metadata.FinalizeDelete(ctx, resID); err != nil {
		logger.Errorw("Failed to finalize delete", "error", err)
		return err
	}

	logger.Info("Successfully deleted feature")
	return nil
}

func (t *FeatureTask) deleteFromOnlineStore(ctx context.Context, featureToDelete *metadata.FeatureVariant, logger logging.Logger, nv metadata.NameVariant) error {
	logger.Debug("Deleting feature from online store")
	if featureToDelete.Provider() == "" {
		logger.Debugw("Feature does not contain inference store, skipping deletion from online store")
		return nil
	}

	logger = logger.With("online_provider", featureToDelete.Provider())
	inferenceStore, err := featureToDelete.FetchProvider(t.metadata, ctx)
	if err != nil {
		logger.Errorw("Failed to fetch inference store", "error", err)
		return err
	}

	if inferenceStore == nil {
		logger.Errorw("Received nil inference store but feature contains provider")
		return fferr.NewInternalErrorf("received nil inference store but feature contains provider")
	}

	logger.Debugw("Attempting to delete feature from online store", "name", nv.Name, "variant", nv.Variant)
	onlineProvider, err := provider.Get(pt.Type(inferenceStore.Type()), inferenceStore.SerializedConfig())
	if err != nil {
		logger.Errorw("Failed to get online provider", "error", err)
		return err
	}
	casted, err := onlineProvider.AsOnlineStore()
	if err != nil {
		logger.Errorw("Failed to cast provider as online store", "error", err)
		return err
	}

	onlineDeleteErr := casted.DeleteTable(nv.Name, nv.Variant)
	if onlineDeleteErr != nil {
		var notFoundErr *fferr.DatasetNotFoundError
		if errors.As(onlineDeleteErr, &notFoundErr) {
			logger.Infow("Table not found in online store, continuing...")
			// continuing
		} else {
			logger.Errorw("Failed to delete feature from online store", "error", onlineDeleteErr)
			return onlineDeleteErr
		}
	}
	logger.Info("Deleted feature from online store")

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
	jobRunner, err := t.spawner.GetJobRunner(runner.MATERIALIZE, serialized, id)
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
