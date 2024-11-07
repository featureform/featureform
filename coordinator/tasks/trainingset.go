// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/runner"
	"github.com/featureform/scheduling"
)

type TrainingSetTask struct {
	BaseTask
}

func (t *TrainingSetTask) Run() error {
	fmt.Printf("%#v\n", t.taskDef.Target)
	nv, ok := t.taskDef.Target.(scheduling.NameVariant)
	if !ok {
		return fferr.NewInternalErrorf("cannot create a source from target type: %s", t.taskDef.TargetType)
	}

	t.logger.Info("Running training set job on resource: ", "name", nv.Name, "variant", nv.Variant)
	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Starting Training Set Creation..."); err != nil {
		return err
	}

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Training Set configuration..."); err != nil {
		return err
	}

	ts, err := t.metadata.GetTrainingSetVariant(context.Background(), metadata.NameVariant{Name: nv.Name, Variant: nv.Variant})
	if err != nil {
		return err
	}

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Offline Store..."); err != nil {
		return err
	}

	providerEntry, err := ts.FetchProvider(t.metadata, context.Background())
	if err != nil {
		return err
	}
	t.logger.Debugw("Training set provider", "type", providerEntry.Type(), "config", providerEntry.SerializedConfig())
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return err
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		return err
	}
	t.logger.Debugw("Training set offline store", "type", fmt.Sprintf("%T", store))
	defer func(store provider.OfflineStore) {
		err := store.Close()
		if err != nil {
			t.logger.Errorf("could not close offline store: %v", err)
		}
	}(store)
	providerResID := provider.ResourceID{Name: nv.Name, Variant: nv.Variant, Type: provider.TrainingSet}

	if _, err := store.GetTrainingSet(providerResID); err == nil {
		return err
	}

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for dependencies to complete..."); err != nil {
		return err
	}
	featureSourceMappings := make([]provider.SourceMapping, len(ts.Features()))
	features := ts.Features()
	featureList := make([]provider.ResourceID, len(features))
	for i, feature := range features {
		featureList[i] = provider.ResourceID{Name: feature.Name, Variant: feature.Variant, Type: provider.Feature}
		featureResource, err := t.metadata.GetFeatureVariant(context.Background(), feature)
		if err != nil {
			return err
		}
		sourceNameVariant := featureResource.Source()
		sourceVariant, err := t.awaitPendingSource(sourceNameVariant)
		if err != nil {
			return err
		}
		_, err = t.AwaitPendingFeature(metadata.NameVariant{Name: feature.Name, Variant: feature.Variant})
		if err != nil {
			return err
		}
		featureSourceMappings[i], err = t.getFeatureSourceMapping(featureResource, sourceVariant)
		if err != nil {
			return err
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

	label, err := ts.FetchLabel(t.metadata, context.Background())
	if err != nil {
		return err
	}
	labelSourceNameVariant := label.Source()
	_, err = t.awaitPendingSource(labelSourceNameVariant)
	if err != nil {
		return err
	}
	label, err = t.AwaitPendingLabel(metadata.NameVariant{Name: label.Name(), Variant: label.Variant()})
	if err != nil {
		return err
	}
	labelSourceMapping, err := t.getLabelSourceMapping(label)
	if err != nil {
		return err
	}
	resourceSnowflakeConfig := &metadata.ResourceSnowflakeConfig{}
	if store.Type() == pt.SnowflakeOffline {
		tempConfig, err := ts.ResourceSnowflakeConfig()
		if err != nil {
			return err
		}
		resourceSnowflakeConfig = tempConfig
	}

	trainingSetDef := provider.TrainingSetDef{
		ID:                      providerResID,
		Label:                   provider.ResourceID{Name: label.Name(), Variant: label.Variant(), Type: provider.Label},
		LabelSourceMapping:      labelSourceMapping,
		Features:                featureList,
		FeatureSourceMappings:   featureSourceMappings,
		LagFeatures:             lagFeaturesList,
		ResourceSnowflakeConfig: resourceSnowflakeConfig,
	}
	tsRunnerConfig := runner.TrainingSetRunnerConfig{
		OfflineType:   pt.Type(providerEntry.Type()),
		OfflineConfig: providerEntry.SerializedConfig(),
		Def:           trainingSetDef,
		IsUpdate:      t.isUpdate,
	}
	serialized, _ := tsRunnerConfig.Serialize()

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Job runner..."); err != nil {
		return err
	}

	resID := metadata.ResourceID{Name: nv.Name, Variant: nv.Variant, Type: metadata.TRAINING_SET_VARIANT}
	jobRunner, err := t.spawner.GetJobRunner(runner.CREATE_TRAINING_SET, serialized, resID)
	if err != nil {
		return err
	}

	err = t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Starting execution...")
	if err != nil {
		return err
	}

	completionWatcher, err := jobRunner.Run()
	if err != nil {
		return err
	}

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Waiting for completion..."); err != nil {
		return err
	}

	if err := completionWatcher.Wait(); err != nil {
		return err
	}

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Training Set creation complete..."); err != nil {
		return err
	}

	return nil
}

func (t *TrainingSetTask) getLabelSourceMapping(label *metadata.LabelVariant) (provider.SourceMapping, error) {
	logger := t.logger.With("Label", label.Name(), "variant", label.Variant())
	labelProvider, err := label.FetchProvider(t.metadata, context.Background())
	if err != nil {
		return provider.SourceMapping{}, err
	}
	logger.Debugw("Label Provider", "type", labelProvider.Type())
	labelSource, err := ps.ResourceToTableName(provider.Label.String(), label.Name(), label.Variant())
	if err != nil {
		return provider.SourceMapping{}, err
	}
	labelLocation, err := t.getResourceLocation(labelProvider, labelSource)
	if err != nil {
		return provider.SourceMapping{}, err
	}
	return provider.SourceMapping{
		Source:         labelSource,
		ProviderType:   pt.Type(labelProvider.Type()),
		ProviderConfig: labelProvider.SerializedConfig(),
		Location:       labelLocation,
	}, nil
}

// **NOTE**: Given a feature's provider will always be an online store, we actually need its source's provider to grab the data for the training set.
func (t *TrainingSetTask) getFeatureSourceMapping(feature *metadata.FeatureVariant, source *metadata.SourceVariant) (provider.SourceMapping, error) {
	logger := t.logger.With("Feature", feature.Name(), "variant", feature.Variant())
	sourceProvider, err := source.FetchProvider(t.metadata, context.Background())
	if err != nil {
		return provider.SourceMapping{}, err
	}
	logger.Debugw("Feature Source Provider", "type", sourceProvider.Type())
	featureSource, err := t.getFeatureSourceTableName(sourceProvider, feature)
	if err != nil {
		return provider.SourceMapping{}, err
	}
	featureLocation, err := t.getResourceLocation(sourceProvider, featureSource)
	if err != nil {
		return provider.SourceMapping{}, err
	}
	return provider.SourceMapping{
		Source:         featureSource,
		ProviderType:   pt.Type(sourceProvider.Type()),
		ProviderConfig: sourceProvider.SerializedConfig(),
		Location:       featureLocation,
	}, nil
}

func (t *TrainingSetTask) AwaitPendingFeature(featureNameVariant metadata.NameVariant) (*metadata.FeatureVariant, error) {
	featureStatus := scheduling.PENDING
	for featureStatus != scheduling.READY {
		feature, err := t.metadata.GetFeatureVariant(context.Background(), featureNameVariant)
		if err != nil {
			return nil, err
		}
		featureStatus = feature.Status()
		if featureStatus == scheduling.FAILED {
			err := fferr.NewResourceFailedError(featureNameVariant.Name, featureNameVariant.Variant, fferr.FEATURE_VARIANT, fmt.Errorf("required feature is in a failed state"))
			return nil, err
		}
		if featureStatus == scheduling.READY {
			return feature, nil
		}
		time.Sleep(1 * time.Second)
	}
	return t.metadata.GetFeatureVariant(context.Background(), featureNameVariant)
}

func (t *TrainingSetTask) AwaitPendingLabel(labelNameVariant metadata.NameVariant) (*metadata.LabelVariant, error) {
	labelStatus := scheduling.PENDING
	for labelStatus != scheduling.READY {
		label, err := t.metadata.GetLabelVariant(context.Background(), labelNameVariant)
		if err != nil {
			return nil, err
		}
		labelStatus = label.Status()
		if labelStatus == scheduling.FAILED {
			err := fferr.NewResourceFailedError(labelNameVariant.Name, labelNameVariant.Variant, fferr.LABEL_VARIANT, fmt.Errorf("required label is in a failed state"))
			return nil, err
		}
		if labelStatus == scheduling.READY {
			return label, nil
		}
		time.Sleep(1 * time.Second)
	}
	return t.metadata.GetLabelVariant(context.Background(), labelNameVariant)
}

// TODO: (Erik) expand to handle other provider types and fully qualified table names (i.e. with database and schema)
func (t *TrainingSetTask) getResourceLocation(provider *metadata.Provider, tableName string) (pl.Location, error) {
	var location pl.Location
	var err error
	switch pt.Type(provider.Type()) {
	case pt.SnowflakeOffline:
		config := pc.SnowflakeConfig{}
		if err := config.Deserialize(provider.SerializedConfig()); err != nil {
			return nil, err
		}
		// TODO: (Erik) determine if we want to use the Catalog location instead of SQL location; technically,
		// Snowflake references tables in a catalog no differently than it does other table types.
		location = pl.NewSQLLocationWithDBSchemaTable(config.Database, config.Schema, tableName)
	default:
		t.logger.Errorw("unsupported provider type: %s", provider.Type())
	}
	return location, err
}

func (t *TrainingSetTask) getFeatureSourceTableName(provder *metadata.Provider, feature *metadata.FeatureVariant) (string, error) {
	var resourceType provider.OfflineResourceType
	switch pt.Type(provder.Type()) {
	case pt.SnowflakeOffline:
		// NOTE: Previously, we created resource tables for features prior to materialization; given these table were purely intermediaries between
		// transformations and materializations, and given the additional cost associated with Dynamic Iceberg Tables in Snowflake, we've removed these
		// resources tables in favor of directly materializing features.
		resourceType = provider.FeatureMaterialization
	case pt.MemoryOffline, pt.MySqlOffline, pt.PostgresOffline, pt.ClickHouseOffline, pt.RedshiftOffline, pt.SparkOffline, pt.BigQueryOffline, pt.K8sOffline:
		resourceType = provider.Feature
	default:
		return "", fferr.NewInternalErrorf("unsupported provider type: %s", provder.Type())
	}

	return ps.ResourceToTableName(resourceType.String(), feature.Name(), feature.Variant())
}
