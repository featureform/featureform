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

func (serv *FeatureServer) SourceData(req *pb.SourceDataRequest, stream pb.Feature_SourceDataServer) error {
	id := req.GetId()
	name, variant := id.GetName(), id.GetVersion()
	limit := req.GetLimit()
	logger := serv.Logger.With("Name", name, "Variant", variant)
	logger.Info("Serving source data")
	iter, err := serv.getSourceDataIterator(name, variant, limit)
	if err != nil {
		logger.Errorw("Failed to get source data iterator", "Error", err)
		return err
	}
	for iter.Next() {
		sRow, err := serializedSourceRow(iter.Values())
		if err != nil {
			return err
		}
		if err := stream.Send(sRow); err != nil {
			logger.Errorw("Failed to write to source data stream", "Error", err)
			return err
		}
	}
	if err := iter.Err(); err != nil {
		logger.Errorw("Source data set error", "Error", err)
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

func (serv *FeatureServer) getSourceDataIterator(name, variant string, limit int64) (provider.GenericTableIterator, error) {
	ctx := context.TODO()
	serv.Logger.Infow("Getting Source Variant Iterator", "name", name, "variant", variant)
	sv, err := serv.Metadata.GetSourceVariant(ctx, metadata.NameVariant{name, variant})
	if err != nil {
		return nil, errors.Wrap(err, "could not get source variant")
	}
	providerEntry, err := sv.FetchProvider(serv.Metadata, ctx)
	serv.Logger.Debugw("Fetched Source Variant Provider", "name", providerEntry.Name(), "type", providerEntry.Type())
	if err != nil {
		return nil, errors.Wrap(err, "could not get fetch provider")
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, errors.Wrap(err, "could not get provider")
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		return nil, errors.Wrap(err, "could not open as offline store")
	}
	var primary provider.PrimaryTable
	var providerErr error
	if sv.IsTransformation() {
		t, err := store.GetTransformationTable(provider.ResourceID{Name: name, Variant: variant, Type: provider.Transformation})
		if err != nil {
			providerErr = err
		} else {
			providerErr = nil
			primary = t.(provider.PrimaryTable)
		}
	} else {
		primary, providerErr = store.GetPrimaryTable(provider.ResourceID{Name: name, Variant: variant, Type: provider.Primary})
	}
	if providerErr != nil {
		return nil, errors.Wrap(err, "could not get primary table")
	}
	return primary.IterateSegment(limit)
}

// TODO: test serving embedding features
func (serv *FeatureServer) FeatureServe(ctx context.Context, req *pb.FeatureServeRequest) (*pb.FeatureRow, error) {
	features := req.GetFeatures()
	entities := req.GetEntities()
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
	vals := make([]*pb.Value, len(features))
	for i, feature := range req.GetFeatures() {
		name, variant := feature.GetName(), feature.GetVersion()
		serv.Logger.Infow("Serving feature", "Name", name, "Variant", variant)
		val, err := serv.getFeatureValue(ctx, name, variant, entityMap)
		if err != nil {
			return nil, errors.Wrap(err, "could not get feature value")
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

	var val interface{}
	switch meta.Mode() {
	case metadata.PRECOMPUTED:
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
		p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
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
	f, err := newValue(val)
	if err != nil {
		logger.Errorw("invalid feature type", "Error", err)
		obs.SetError()
		return nil, err
	}
	obs.ServeRow()
	return f.Serialized(), nil
}

func (serv *FeatureServer) SourceColumns(ctx context.Context, req *pb.SourceColumnRequest) (*pb.SourceDataColumns, error) {
	id := req.GetId()
	name, variant := id.GetName(), id.GetVersion()
	serv.Logger.Infow("Getting source columns", "Name", name, "Variant", variant)
	it, err := serv.getSourceDataIterator(name, variant, 0) // Set limit to zero to fetch columns only
	if err != nil {
		return nil, err
	}
	defer it.Close()
	return &pb.SourceDataColumns{
		Columns: it.Columns(),
	}, nil
}

func (serv *FeatureServer) Nearest(ctx context.Context, req *pb.NearestRequest) (*pb.NearestResponse, error) {
	id := req.GetId()
	name, variant := id.GetName(), id.GetVersion()
	serv.Logger.Infow("Searching nearest", "Name", name, "Variant", variant)
	meta, err := serv.Metadata.GetFeatureVariant(ctx, metadata.NameVariant{name, variant})
	if err != nil {
		serv.Logger.Errorw("metadata lookup failed", "Err", err)
		return nil, err
	}
	providerEntry, err := meta.FetchProvider(serv.Metadata, ctx)
	if err != nil {
		serv.Logger.Errorw("fetching provider metadata failed", "Error", err)
		return nil, err
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		serv.Logger.Errorw("failed to get provider", "Error", err)
		return nil, err
	}
	store, err := p.AsOnlineStore()
	if err != nil {
		serv.Logger.Errorw("failed to use provider as online store for feature", "Error", err)
		return nil, err
	}
	table, err := store.GetTable(name, variant)
	if err != nil {
		serv.Logger.Errorw("feature not found", "Error", err)
		return nil, err
	}
	vectorTable, ok := table.(provider.VectorStoreTable)
	if !ok {
		serv.Logger.Errorw("failed to use table as vector store table", "Error", err)
	}
	searchVector := req.GetVector()
	k := req.GetK()
	if searchVector == nil {
		return nil, fmt.Errorf("no embedding provided")
	}
	_, err = vectorTable.Nearest(name, variant, searchVector.Value, k) // handle results
	if err != nil {
		serv.Logger.Errorw("nearest search failed", "Error", err)
		return nil, err
	}
	return nil, nil
}
