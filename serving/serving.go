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
	vals := make(chan *pb.Value, len(features))
	errc := make(chan error, len(req.GetFeatures()))

	for i, feature := range req.GetFeatures() {
		go func(i int, feature *pb.FeatureID) {
			name, variant := feature.GetName(), feature.GetVersion()
			val, err := serv.getFeatureValue(ctx, name, variant, entityMap)
			if err != nil {
				errc <- fmt.Errorf("error getting feature value: %w", err)
				serv.Logger.Errorw("Could not get feature value", "Name", name, "Variant", variant, "Error", err.Error())
				return
			}
			vals <- val
		}(i, feature)
		serv.Logger.Infow("End of goroutine", "Name", feature.Name, "Variant", feature.Version)
	}

	results := make([]*pb.Value, len(req.GetFeatures()))
	for i := 0; i < len(req.GetFeatures()); i++ {
		result := <-vals
		if err := <-errc; err != nil {
			serv.Logger.Errorw("Could not get feature value", "Error", err.Error())
		}
		results[i] = result
	}
	serv.Logger.Info("Serving Complete")

	return &pb.FeatureRow{
		Values: results,
	}, nil
}

func (serv *FeatureServer) getFeatureValue(ctx context.Context, name, variant string, entityMap map[string]string) (*pb.Value, error) {
	obs := serv.Metrics.BeginObservingOnlineServe(name, variant)
	defer obs.Finish()
	logger := serv.Logger.With("Name", name, "Variant", variant)
	logger.Debug("Getting metadata")
	// THIS IS SLOW 80ms
	meta, err := serv.Metadata.GetFeatureVariant(ctx, metadata.NameVariant{name, variant})
	if err != nil {
		logger.Errorw("metadata lookup failed", "Err", err)
		obs.SetError()
		return nil, err
	}
	logger.Debug("Returned metadata")

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
		// THIS IS SLOW 100ms
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
		// 70ms
		table, err := store.GetTable(name, variant)
		if err != nil {
			logger.Errorw("feature not found", "Error", err)
			obs.SetError()
			return nil, err
		}
		logger.Debug("Getting value")
		//60ms
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
