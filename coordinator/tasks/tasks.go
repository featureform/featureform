package tasks

import (
	"context"
	"fmt"
	"github.com/featureform/coordinator/spawner"
	"github.com/featureform/fferr"
	"github.com/featureform/metadata"
	"github.com/featureform/scheduling"
	"go.uber.org/zap"
	"time"
)

type Task interface {
	Run() error
}

type BaseTask struct {
	metadata *metadata.Client
	taskDef  scheduling.TaskRunMetadata
	Spawner  spawner.JobSpawner
	logger   *zap.SugaredLogger
}

func (bt *BaseTask) waitForRunCompletion(id []scheduling.TaskRunID) error {
	return nil
}

func (t *BaseTask) AwaitPendingSource(sourceNameVariant metadata.NameVariant) (*metadata.SourceVariant, error) {
	sourceStatus := scheduling.PENDING
	for sourceStatus != scheduling.READY {
		source, err := t.metadata.GetSourceVariant(context.Background(), sourceNameVariant)
		if err != nil {
			return nil, err
		}
		var sourceType fferr.ResourceType
		if source.IsTransformation() {
			sourceType = fferr.TRANSFORMATION
		} else {
			sourceType = fferr.PRIMARY_DATASET
		}
		sourceStatus = source.Status()
		if sourceStatus == scheduling.FAILED {
			err := fferr.NewResourceFailedError(sourceNameVariant.Name, sourceNameVariant.Variant, sourceType, fmt.Errorf("required dataset is in a failed state"))
			return nil, err
		}
		if sourceStatus == scheduling.READY {
			return source, nil
		}
		time.Sleep(1 * time.Second)
	}
	return t.metadata.GetSourceVariant(context.Background(), sourceNameVariant)
}
