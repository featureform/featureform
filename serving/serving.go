// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package serving

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"sync"

	"github.com/featureform/metadata"
	"github.com/featureform/metrics"
	pb "github.com/featureform/proto"
	"github.com/featureform/provider"
	pt "github.com/featureform/provider/provider_type"

	"go.uber.org/zap"
)

type FeatureServer struct {
	pb.UnimplementedFeatureServer
	Metrics   metrics.MetricsHandler
	Metadata  *metadata.Client
	Logger    *zap.SugaredLogger
	Providers *sync.Map
	Tables    *sync.Map
	Features  *sync.Map
}

func NewFeatureServer(meta *metadata.Client, promMetrics metrics.MetricsHandler, logger *zap.SugaredLogger) (*FeatureServer, error) {
	logger.Debug("Creating new training data server")
	return &FeatureServer{
		Metadata:  meta,
		Metrics:   promMetrics,
		Logger:    logger,
		Providers: &sync.Map{},
		Tables:    &sync.Map{},
		Features:  &sync.Map{},
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
	serv.Logger.Infow("Serving Features")
	//serv.Logger.Infow("Serving features", "Features", features, "Entities", entities)
	entityMap := make(map[string][]string)
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
	vals := make(chan *pb.ValueList, len(features))
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
		if len(errc) > 0 {
			break
		}
	}

	results := make([]*pb.ValueList, len(req.GetFeatures()))
	for i := 0; i < len(req.GetFeatures()); i++ {
		if len(errc) != 0 {
			err := <-errc
			serv.Logger.Errorw("Could not get feature value", "Error", err.Error())
			break
		}
		results[i] = <-vals
	}
	serv.Logger.Infow("Serving Complete")

	return &pb.FeatureRow{
		Values: results,
	}, nil
}

func (serv *FeatureServer) getNVCacheKey(name, variant string) string {
	return fmt.Sprintf("%s:%s", name, variant)
}

func (serv *FeatureServer) getFeatureValue(ctx context.Context, name, variant string, entityMap map[string][]string) (*pb.ValueList, error) {
	obs := serv.Metrics.BeginObservingOnlineServe(name, variant)
	defer obs.Finish()
	logger := serv.Logger.With("Name", name, "Variant", variant)
	// THIS IS SLOW 80ms
	var meta *metadata.FeatureVariant
	if feature, has := serv.Features.Load(serv.getNVCacheKey(name, variant)); has {
		meta = feature.(*metadata.FeatureVariant)
	} else {
		metaFeature, err := serv.Metadata.GetFeatureVariant(ctx, metadata.NameVariant{name, variant})
		if err != nil {
			logger.Errorw("metadata lookup failed", "Err", err)
			obs.SetError()
			return nil, fmt.Errorf("metadata lookup failed: %w", err)
		}
		meta = metaFeature
		fmt.Println("SETTING KEY FOR FEATURE: ", serv.getNVCacheKey(name, variant))
		serv.Features.Range(func(key, value interface{}) bool {
			fmt.Println(key, value, "->", serv.getNVCacheKey(name, variant))
			return true
		})
		serv.Features.Store(serv.getNVCacheKey(name, variant), meta)
	}

	var values []interface{}
	switch meta.Mode() {
	case metadata.PRECOMPUTED:
		entity, has := entityMap[meta.Entity()]
		if !has {
			logger.Errorw("Entity not found", "Entity", meta.Entity())
			obs.SetError()
			return nil, fmt.Errorf("no value for entity %s", meta.Entity())
		}
		if store, has := serv.Providers.Load(meta.Provider()); has {
			var featureTable provider.OnlineStoreTable
			if table, has := serv.Tables.Load(serv.getNVCacheKey(name, variant)); has {
				featureTable = table.(provider.OnlineStoreTable)
			} else {
				table, err := store.(provider.OnlineStore).GetTable(name, variant)
				if err != nil {
					logger.Errorw("feature not found", "Error", err)
					obs.SetError()
					return nil, fmt.Errorf("feature not found: %w", err)
				}
				serv.Tables.Store(serv.getNVCacheKey(name, variant), table)
				featureTable = table
			}
			//for _, entityVal := range entity {
			//	val, err := featureTable.(provider.OnlineStoreTable).Get(entityVal)
			//	if err != nil {
			//		logger.Errorw("entity not found", "Error", err)
			//		obs.SetError()
			//		return nil, fmt.Errorf("entity not found: %w", err)
			//	}
			//	values = append(values, val)
			//}
			valCh := make(chan interface{}, len(entity))
			errCh := make(chan error, len(entity))

			for _, entityVal := range entity {
				// Start a goroutine for each entity
				go func(ev string) {
					val, err := featureTable.(provider.OnlineStoreTable).Get(ev)
					if err != nil {
						// Push error into the error channel
						errCh <- fmt.Errorf("entity not found: %w", err)
						return
					}
					// If no error, push value into the value channel
					valCh <- val
				}(entityVal)
			}

			// Collect results
			for range entity {
				select {
				case err := <-errCh:
					// If we get an error, stop and return it
					logger.Errorw("entity not found", "Error", err)
					obs.SetError()
					return nil, err
				case val := <-valCh:
					// Otherwise, add the value to the slice
					values = append(values, val)
				}
			}
		} else {
			providerEntry, err := meta.FetchProvider(serv.Metadata, ctx)
			if err != nil {
				logger.Errorw("fetching provider metadata failed", "Error", err)
				obs.SetError()
				return nil, fmt.Errorf("fetching provider metadata failed: %w", err)
			}
			p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
			if err != nil {
				logger.Errorw("failed to get provider", "Error", err)
				obs.SetError()
				return nil, fmt.Errorf("failed to get provider: %w", err)
			}
			store, err := p.AsOnlineStore()
			if err != nil {
				logger.Errorw("failed to use provider as onlinestore for feature", "Error", err)
				obs.SetError()
				// This means that the provider of the feature isn't an online store.
				// That shouldn't be possible.
				return nil, fmt.Errorf("failed to use provider as onlinestore for feature: %w", err)
			}
			serv.Providers.Store(meta.Provider(), store)
			// 70ms
			var featureTable provider.OnlineStoreTable
			if table, has := serv.Tables.Load(serv.getNVCacheKey(name, variant)); has {
				featureTable = table.(provider.OnlineStoreTable)
			} else {
				table, err := store.GetTable(name, variant)
				if err != nil {
					logger.Errorw("feature not found", "Error", err)
					obs.SetError()
					return nil, fmt.Errorf("feature not found: %w", err)
				}
				serv.Tables.Store(serv.getNVCacheKey(name, variant), table)
				featureTable = table
			}
			//for _, entityVal := range entity {
			//	val, err := featureTable.(provider.OnlineStoreTable).Get(entityVal)
			//	if err != nil {
			//		logger.Errorw("entity not found", "Error", err)
			//		obs.SetError()
			//		return nil, fmt.Errorf("entity not found: %w", err)
			//	}
			//	values = append(values, val)
			//}
			valCh := make(chan interface{}, len(entity))
			errCh := make(chan error, len(entity))

			for _, entityVal := range entity {
				// Start a goroutine for each entity
				go func(ev string) {
					val, err := featureTable.(provider.OnlineStoreTable).Get(ev)
					if err != nil {
						// Push error into the error channel
						errCh <- fmt.Errorf("entity not found: %w", err)
						return
					}
					// If no error, push value into the value channel
					valCh <- val
				}(entityVal)
			}

			// Collect results
			for range entity {
				select {
				case err := <-errCh:
					// If we get an error, stop and return it
					logger.Errorw("entity not found", "Error", err)
					obs.SetError()
					return nil, err
				case val := <-valCh:
					// Otherwise, add the value to the slice
					values = append(values, val)
				}
			}
		}
	case metadata.CLIENT_COMPUTED:
		values = append(values, meta.LocationFunction())
	default:
		return nil, fmt.Errorf("unknown computation mode %v", meta.Mode())
	}
	castedValues := &pb.ValueList{}
	for _, val := range values {
		f, err := newFeature(val)
		if err != nil {
			logger.Errorw("invalid feature type", "Error", err)
			obs.SetError()
			return nil, fmt.Errorf("invalid feature type: %w", err)
		}
		castedValues.Values = append(castedValues.Values, f.Serialized())
	}
	obs.ServeRow()
	return castedValues, nil
}
