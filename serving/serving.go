// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package serving

import (
	"context"
	"fmt"
	"github.com/pkg/errors"

	"github.com/featureform/metadata"
	"github.com/featureform/metrics"
	pb "github.com/featureform/proto"
	"github.com/featureform/provider"
	pt "github.com/featureform/provider/provider_type"

	"go.uber.org/zap"
)

type FeatureServer struct {
	pb.UnimplementedFeatureServer
	Metrics  metrics.MetricsHandler
	Metadata *metadata.Client
	Logger   *zap.SugaredLogger
}

func NewFeatureServer(meta *metadata.Client, promMetrics metrics.MetricsHandler, logger *zap.SugaredLogger) (*FeatureServer, error) {
	logger.Debug("Creating new training data server")
	return &FeatureServer{
		Metadata: meta,
		Metrics:  promMetrics,
		Logger:   logger,
	}, nil
}

func (serv *FeatureServer) TrainingData(req *pb.TrainingDataRequest, stream pb.Feature_TrainingDataServer) error {
	id := req.GetId()
	name, variant := id.GetName(), id.GetVersion()
	featureObserver := serv.Metrics.BeginObservingTrainingServe(name, variant)
	defer featureObserver.Finish()
	logger := serv.Logger.With("Name", name, "Variant", variant)
	logger.Info("Serving training data")
	if model := req.GetModel(); model != nil {
		trainingSets := []metadata.NameVariant{{Name: name, Variant: variant}}
		err := serv.Metadata.CreateModel(stream.Context(), metadata.ModelDef{Name: model.GetName(), Trainingsets: trainingSets})
		if err != nil {
			return err
		}
	}
	iter, err := serv.getTrainingSetIterator(name, variant)
	if err != nil {
		logger.Errorw("Failed to get training set iterator", "Error", err)
		featureObserver.SetError()
		return err
	}
	for iter.Next() {
		sRow, err := serializedRow(iter.Features(), iter.Label())
		if err != nil {
			return err
		}
		if err := stream.Send(sRow); err != nil {
			logger.Errorw("Failed to write to stream", "Error", err)
			featureObserver.SetError()
			return err
		}
		featureObserver.ServeRow()
	}
	if err := iter.Err(); err != nil {
		logger.Errorw("Dataset error", "Error", err)
		featureObserver.SetError()
		return err
	}
	return nil
}

func (serv *FeatureServer) getTrainingSetIterator(name, variant string) (provider.TrainingSetIterator, error) {
	ctx := context.TODO()
	serv.Logger.Infow("Getting Training Set Iterator", "name", name, "variant", variant)
	ts, err := serv.Metadata.GetTrainingSetVariant(ctx, metadata.NameVariant{name, variant})
	if err != nil {
		return nil, errors.Wrap(err, "could not get training set variant")
	}
	serv.Logger.Debugw("Fetching Training Set Provider", "name", name, "variant", variant)
	providerEntry, err := ts.FetchProvider(serv.Metadata, ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not get fetch provider")
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, errors.Wrap(err, "could not get provider")
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		// This means that the provider of the training set isn't an offline store.
		// That shouldn't be possible.
		return nil, errors.Wrap(err, "could not open as offline store")
	}
	serv.Logger.Debugw("Get Training Set From Store", "name", name, "variant", variant)
	return store.GetTrainingSet(provider.ResourceID{Name: name, Variant: variant})
}

func (serv *FeatureServer) FeatureServe(ctx context.Context, req *pb.FeatureServeRequest) (*pb.FeatureRow, error) {
	features := req.GetFeatures()
	entities := req.GetEntities()
	serv.Logger.Infow("Serving features", "Features", features, "Entities", entities)
	entityMap := make(map[string]string)
	for _, entity := range entities {
		entityMap[entity.GetName()] = entity.GetValue()
	}
	if model := req.GetModel(); model != nil {
		modelFeatures := make([]metadata.NameVariant, len(features))
		for i, feature := range req.GetFeatures() {
			modelFeatures[i] = metadata.NameVariant{Name: feature.Name, Variant: feature.Version}
		}
		serv.Logger.Infow("Creating model", "Name", model.GetName())
		err := serv.Metadata.CreateModel(ctx, metadata.ModelDef{Name: model.GetName(), Features: modelFeatures})
		if err != nil {
			return nil, err
		}
	}

	nvs := make([]metadata.NameVariant, len(features))

	serv.Logger.Info("Converting NameVariants")

	for i, feature := range req.GetFeatures() {
		nvs[i] = metadata.NameVariant{Name: feature.GetName(), Variant: feature.GetVersion()}
	}

	serv.Logger.Infow("Getting feature variants", "NameVariants", nvs)

	metas, err := serv.Metadata.GetFeatureVariants(ctx, nvs)
	if err != nil {
		serv.Logger.Errorw("Failed to get feature variants", "Error", err)
		return nil, err
	}

	vals := make([]*pb.Value, len(features))
	valueCh := make(chan *pb.Value, len(metas))
	errCh := make(chan error, len(metas))

	serv.Logger.Info("Getting feature values")

	for _, meta := range metas {
		go func(meta *metadata.FeatureVariant) {
			value, err := serv.getFeatureValues(ctx, meta, entityMap)
			if err != nil {
				errCh <- err
				return
			}
			valueCh <- value
		}(meta)
	}

	serv.Logger.Info("Adding values to response")

	for i := 0; i < len(metas); i++ {
		select {
		case value := <-valueCh:
			vals[i] = value
		case err := <-errCh:
			return nil, err
		}
	}

	serv.Logger.Info("Serving Complete")

	return &pb.FeatureRow{
		Values: vals,
	}, nil
}

func (serv *FeatureServer) getFeatureValues(ctx context.Context, meta *metadata.FeatureVariant, entityMap map[string]string) (*pb.Value, error) {
	obs := serv.Metrics.BeginObservingOnlineServe(meta.Name(), meta.Variant())
	defer obs.Finish()
	logger := serv.Logger.With("Name", meta.Name(), "Variant", meta.Variant())

	var val interface{}
	switch meta.Mode() {
	case metadata.PRECOMPUTED:
		logger.Debug("Checking entity")
		entity, has := entityMap[meta.Entity()]
		if !has {
			logger.Errorw("Entity not found", "Entity", meta.Entity())
			obs.SetError()
			return nil, fmt.Errorf("No value for entity %s", meta.Entity())
		}
		logger.Debug("Getting provider")
		providerEntry, err := meta.FetchProvider(serv.Metadata, ctx)
		if err != nil {
			logger.Errorw("fetching provider metadata failed", "Error", err)
			obs.SetError()
			return nil, err
		}
		logger.Debug("Initializing provider")
		p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
		if err != nil {
			logger.Errorw("failed to get provider", "Error", err)
			obs.SetError()
			return nil, err
		}
		logger.Debug("Online store check")
		store, err := p.AsOnlineStore()
		if err != nil {
			logger.Errorw("failed to use provider as onlinestore for feature", "Error", err)
			obs.SetError()
			// This means that the provider of the feature isn't an online store.
			// That shouldn't be possible.
			return nil, err
		}
		logger.Debug("Getting table")
		table, err := store.GetTable(meta.Name(), meta.Variant())
		if err != nil {
			logger.Errorw("feature not found", "Error", err)
			obs.SetError()
			return nil, err
		}
		logger.Debug("Getting value")
		val, err = table.Get(entity)
		if err != nil {
			logger.Errorw("entity not found", "Error", err)
			obs.SetError()
			return nil, err
		}
	case metadata.CLIENT_COMPUTED:
		val = meta.LocationFunction()
	default:
		return nil, fmt.Errorf("unknown computation mode %v", meta.Mode())
	}
	logger.Debug("Returned feature values")
	f, err := newFeature(val)
	logger.Debug("Casted feature value")
	if err != nil {
		logger.Errorw("invalid feature type", "Error", err)
		obs.SetError()
		return nil, err
	}
	obs.ServeRow()
	return f.Serialized(), nil
}
