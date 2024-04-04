package tasks

import (
	"context"
	"github.com/featureform/fferr"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/scheduling"
	"go.uber.org/zap"
)

type LabelTask struct {
	BaseTask
	logger *zap.SugaredLogger
}

func (t *LabelTask) Run() error {
	taskMetadata, err := t.metadata.Tasks.GetTaskByID(t.taskDef.TaskId)
	if err != nil {
		return err
	}

	nv, ok := taskMetadata.Target.(scheduling.NameVariant)
	if !ok {
		return fferr.NewInternalErrorf("cannot create a source from target type: %s", taskMetadata.TargetType)
	}

	nameVariant := metadata.NameVariant{Name: nv.Name, Variant: nv.Variant}
	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Label details...")
	if err != nil {
		return err
	}

	label, err := t.metadata.GetLabelVariant(context.Background(), metadata.NameVariant{Name: nameVariant.Name, Variant: nameVariant.Variant})
	if err != nil {
		return err
	}

	sourceNameVariant := label.Source()
	t.logger.Infow("feature obj", "name", label.Name(), "source", label.Source(), "location", label.Location(), "location_col", label.LocationColumns())

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for dependencies to complete...")
	if err != nil {
		return err
	}

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
	var sourceTableName string
	if source.IsSQLTransformation() || source.IsDFTransformation() {
		sourceResourceID := provider.ResourceID{Name: sourceNameVariant.Name, Variant: sourceNameVariant.Variant, Type: provider.Transformation}
		sourceTable, err := sourceStore.GetTransformationTable(sourceResourceID)
		if err != nil {
			return err
		}
		sourceTableName = sourceTable.GetName()
	} else if source.IsPrimaryDataSQLTable() {
		sourceResourceID := provider.ResourceID{Name: sourceNameVariant.Name, Variant: sourceNameVariant.Variant, Type: provider.Primary}
		sourceTable, err := sourceStore.GetPrimaryTable(sourceResourceID)
		if err != nil {
			return err
		}
		sourceTableName = sourceTable.GetName()
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
		SourceTable: sourceTableName,
	}
	t.logger.Debugw("Creating Label Resource Table", "id", labelID, "schema", schema)

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Registering Label from dataset...")
	if err != nil {
		return err
	}

	_, err = sourceStore.RegisterResourceFromSourceTable(labelID, schema)
	if err != nil {
		return err
	}
	t.logger.Debugw("Resource Table Created", "id", labelID, "schema", schema)

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Registration complete...")
	if err != nil {
		return err
	}

	return nil
}
