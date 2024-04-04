package tasks

import (
	"context"
	"fmt"
	"github.com/featureform/fferr"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/runner"
	"github.com/featureform/scheduling"
	"time"
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
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return err
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		return err
	}
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

	features := ts.Features()
	featureList := make([]provider.ResourceID, len(features))
	for i, feature := range features {
		featureList[i] = provider.ResourceID{Name: feature.Name, Variant: feature.Variant, Type: provider.Feature}
		featureResource, err := t.metadata.GetFeatureVariant(context.Background(), feature)
		if err != nil {
			return err
		}
		sourceNameVariant := featureResource.Source()
		_, err = t.AwaitPendingSource(sourceNameVariant)
		if err != nil {
			return err
		}
		_, err = t.AwaitPendingFeature(metadata.NameVariant{Name: feature.Name, Variant: feature.Variant})
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
	_, err = t.AwaitPendingSource(labelSourceNameVariant)
	if err != nil {
		return err
	}
	label, err = t.AwaitPendingLabel(metadata.NameVariant{Name: label.Name(), Variant: label.Variant()})
	if err != nil {
		return err
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

	if err := t.metadata.Tasks.AddRunLog(t.taskDef.TaskId, t.taskDef.ID, "Fetching Job runner..."); err != nil {
		return err
	}

	resID := metadata.ResourceID{Name: nv.Name, Variant: nv.Variant, Type: metadata.TRAINING_SET_VARIANT}
	jobRunner, err := t.Spawner.GetJobRunner(runner.CREATE_TRAINING_SET, serialized, resID)
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
