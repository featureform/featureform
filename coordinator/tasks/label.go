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
	"github.com/featureform/logging"

	"github.com/featureform/provider/provider_schema"

	"github.com/featureform/fferr"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pl "github.com/featureform/provider/location"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/scheduling"
)

type LabelTask struct {
	BaseTask
}

func (t *LabelTask) Run() error {
	_, ctx, logger := t.logger.InitializeRequestID(context.TODO())
	nv, ok := t.taskDef.Target.(scheduling.NameVariant)
	if !ok {
		return fferr.NewInternalErrorf("cannot create a label from target type: %s", t.taskDef.TargetType)
	}

	nameVariant := metadata.NameVariant{Name: nv.Name, Variant: nv.Variant}
	err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Label details...")
	if err != nil {
		return err
	}
	resID := metadata.ResourceID{Name: nv.Name, Variant: nv.Variant, Type: metadata.LABEL_VARIANT}
	logger = logger.WithResource(logging.LabelVariant, resID.Name, resID.Variant).
		With("task_id", t.taskDef.TaskId, "task_run_id", t.taskDef.ID)

	if t.isDelete {
		logger.Debugw("Handling deletion")
		return t.handleDeletion(ctx, resID, logger)
	}

	label, err := t.metadata.GetLabelVariant(ctx, nameVariant)
	if err != nil {
		return err
	}

	sourceNameVariant := label.Source()
	logger.Infow("feature obj", "name", label.Name(), "source", label.Source(), "location", label.Location(), "location_col", label.LocationColumns())

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for dependencies to complete..."); err != nil {
		return err
	}

	source, err := t.awaitPendingSource(sourceNameVariant)
	if err != nil {
		return err
	}

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Offline Store..."); err != nil {
		return err
	}

	sourceStore, getStoreErr := getOfflineStore(ctx, t.BaseTask, t.metadata, source, logger)
	if getStoreErr != nil {
		return getStoreErr
	}

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

	labelID := provider.ResourceID{
		Name:    nameVariant.Name,
		Variant: nameVariant.Variant,
		Type:    provider.Label,
	}
	tmpSchema := label.LocationColumns().(metadata.ResourceVariantColumns)
	schema := provider.ResourceSchema{
		Entity:      tmpSchema.Entity,
		Value:       tmpSchema.Value,
		TS:          tmpSchema.TS,
		SourceTable: sourceLocation,
	}
	logger.Debugw("Creating Label Resource Table", "id", labelID, "schema", schema)

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Registering Label from dataset..."); err != nil {
		return err
	}
	logger.Debugw("Checking source store type", "type", fmt.Sprintf("%T", sourceStore))
	opts := make([]provider.ResourceOption, 0)
	if sourceStore.Type() == pt.SnowflakeOffline {
		tempConfig, err := label.ResourceSnowflakeConfig()
		if err != nil {
			return err
		}
		snowflakeDynamicTableConfigOpts := &provider.ResourceSnowflakeConfigOption{
			Config:    tempConfig.DynamicTableConfig,
			Warehouse: tempConfig.Warehouse,
		}
		opts = append(opts, snowflakeDynamicTableConfigOpts)
	}

	if _, err := sourceStore.RegisterResourceFromSourceTable(labelID, schema, opts...); err != nil {
		logger.Errorw("Failed to register resource from source table", "id", labelID, "opts length", len(opts), "error", err)
		return err
	}
	logger.Debugw("Resource Table Created", "id", labelID, "schema", schema)

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Registration complete..."); err != nil {
		return err
	}

	return nil
}

func (t *LabelTask) handleDeletion(ctx context.Context, resID metadata.ResourceID, logger logging.Logger) error {
	logger.Infow("Deleting label")
	labelToDelete, err := t.metadata.GetStagedForDeletionLabelVariant(
		ctx,
		metadata.NameVariant{
			Name:    resID.Name,
			Variant: resID.Variant,
		},
		logger,
	)
	if err != nil {
		logger.Errorw("Failed to get staged for deletion label", "error", err)
		return err
	}

	logger.Infow("Deleting label", "resource_id", resID)
	labelTableName, tableNameErr := provider_schema.ResourceToTableName(provider_schema.Label, resID.Name, resID.Variant)
	if tableNameErr != nil {
		logger.Errorw("Failed to get table name", "error", tableNameErr)
		return tableNameErr
	}

	sourceStore, err := getOfflineStore(ctx, t.BaseTask, t.metadata, labelToDelete, logger)
	if err != nil {
		logger.Errorw("Failed to get store", "error", err)
		return err
	}

	labelLocation := pl.NewSQLLocation(labelTableName)
	logger = logger.With("location", labelLocation)

	logger.Debugw("Deleting label at location")
	if deleteErr := sourceStore.Delete(labelLocation); deleteErr != nil {
		var notFoundErr *fferr.DatasetNotFoundError
		if errors.As(deleteErr, &notFoundErr) {
			logger.Infow("Table doesn't exist at location, continuing...")
		} else {
			return deleteErr
		}
	}
	logger.Infow("Successfully deleted label at location")

	logger.Debugw("Finalizing delete")
	if err := t.metadata.FinalizeDelete(ctx, resID); err != nil {
		logger.Errorw("Failed to finalize delete", "error", err)
		return err
	}

	return nil
}
