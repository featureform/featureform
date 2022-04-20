// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package newserving

import (
	"context"
	"fmt"

	"github.com/featureform/serving/metadata"
	"github.com/featureform/serving/metrics"
	pb "github.com/featureform/serving/proto"
	"github.com/featureform/serving/provider"

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
	ts, err := serv.Metadata.GetTrainingSetVariant(ctx, metadata.NameVariant{name, variant})
	if err != nil {
		return nil, err
	}
	providerEntry, err := ts.FetchProvider(serv.Metadata, ctx)
	if err != nil {
		return nil, err
	}
	p, err := provider.Get(provider.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, err
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		// This means that the provider of the training set isn't an offline store.
		// That shouldn't be possible.
		return nil, err
	}
	return store.GetTrainingSet(provider.ResourceID{Name: name, Variant: variant})
}

func (serv *FeatureServer) FeatureServe(ctx context.Context, req *pb.FeatureServeRequest) (*pb.FeatureRow, error) {
	features := req.GetFeatures()
	entities := req.GetEntities()
	entityMap := make(map[string]string)
	for _, entity := range entities {
		entityMap[entity.GetName()] = entity.GetValue()
	}
	vals := make([]*pb.Value, len(features))
	for i, feature := range req.GetFeatures() {
		name, variant := feature.GetName(), feature.GetVersion()
		serv.Logger.Infow("Serving feature", "Name", name, "Variant", variant)
		val, err := serv.getFeatureValue(ctx, name, variant, entityMap)
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return &pb.FeatureRow{
		Values: vals,
	}, nil
}

func (serv *FeatureServer) getFeatureValue(ctx context.Context, name, variant string, entityMap map[string]string) (*pb.Value, error) {
	obs := serv.Metrics.BeginObservingOnlineServe(name, variant)
	defer obs.Finish()
	logger := serv.Logger.With("Name", name, "Variant", variant)
	logger.Debug("Getting metadata")
	meta, err := serv.Metadata.GetFeatureVariant(ctx, metadata.NameVariant{name, variant})
	if err != nil {
		logger.Errorw("metadata lookup failed", "Err", err)
		obs.SetError()
		return nil, err
	}
	entity, has := entityMap[meta.Entity()]
	if !has {
		logger.Errorw("Entity not found", "Entity", meta.Entity())
		obs.SetError()
		return nil, fmt.Errorf("No value for entity %s", meta.Entity())
	}
	providerEntry, err := meta.FetchProvider(serv.Metadata, ctx)
	if err != nil {
		logger.Errorw("fetching provider metadata failed", "Error", err)
		obs.SetError()
		return nil, err
	}
	p, err := provider.Get(provider.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		logger.Errorw("failed to get provider", "Error", err)
		obs.SetError()
		return nil, err
	}
	store, err := p.AsOnlineStore()
	if err != nil {
		logger.Errorw("failed to use provider as onlinestore for feature", "Error", err)
		obs.SetError()
		// This means that the provider of the feature isn't an online store.
		// That shouldn't be possible.
		return nil, err
	}
	table, err := store.GetTable(name, variant)
	if err != nil {
		logger.Errorw("feature not found", "Error", err)
		obs.SetError()
		return nil, err
	}
	val, err := table.Get(entity)
	if err != nil {
		logger.Errorw("entity not found", "Error", err)
		obs.SetError()
		return nil, err
	}
	f, err := newFeature(val)
	if err != nil {
		logger.Errorw("invalid feature type", "Error", err)
		obs.SetError()
		return nil, err
	}
	obs.ServeRow()
	return f.Serialized(), nil
}
