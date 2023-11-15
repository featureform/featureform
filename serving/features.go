package serving

import (
	"context"
	"fmt"
	"github.com/featureform/metadata"
	"github.com/featureform/metrics"
	pb "github.com/featureform/proto"
	"github.com/featureform/provider"
	pt "github.com/featureform/provider/provider_type"
	"sort"
)

type indexedValue struct {
	index int
	value interface{}
}

type indexedFeatureRow struct {
	index  int
	values *pb.ValueList
}

func (serv *FeatureServer) getFeatureRows(ctx context.Context, features []*pb.FeatureID, entityMap map[string][]string) ([]*pb.ValueList, error) {
	vals := make(chan indexedFeatureRow, len(features))
	errc := make(chan error, len(features))

	var err error

	// This function creates async requests to fetch feature values
	// so that everything can be done in parallel.
	serv.sendFeatureRequests(ctx, features, entityMap, vals, errc)

	// This function collects the results of the async requests
	// from the channels from the previous function.
	valueLists, err := serv.collectFeatures(features, vals, errc)
	if err != nil {
		return nil, err
	}

	// We need to sort the returned results by order that the features were requested in.
	sort.Slice(valueLists, func(i, j int) bool {
		return valueLists[i].index < valueLists[j].index
	})

	var results []*pb.ValueList
	for _, val := range valueLists {
		results = append(results, val.values)
	}
	return results, nil
}

func (serv *FeatureServer) sendFeatureRequests(ctx context.Context, features []*pb.FeatureID, entityMap map[string][]string, vals chan indexedFeatureRow, errc chan error) {
	// We asynchronously start fetches for each feature in the request
	for i, feature := range features {
		go func(i int, feature *pb.FeatureID) {
			name, variant := feature.GetName(), feature.GetVersion()

			// Features can have multiple values (one per entity)
			valueList, err := serv.getFeatureValues(ctx, name, variant, entityMap)
			if err != nil {
				errc <- fmt.Errorf("error getting feature value: %w", err)
				serv.Logger.Errorw("Could not get feature value", "Name", name, "Variant", variant, "Error", err.Error())
				return
			}

			vals <- indexedFeatureRow{index: i, values: valueList}
		}(i, feature)
	}
}

func (serv *FeatureServer) collectFeatures(features []*pb.FeatureID, vals chan indexedFeatureRow, errc chan error) ([]indexedFeatureRow, error) {
	var valueLists []indexedFeatureRow

	for {
		select {
		case internalError := <-errc:
			err := internalError
			return nil, err
		case val := <-vals:
			valueLists = append(valueLists, val)
			if len(valueLists) == len(features) {
				return valueLists, nil
			}
		}

	}
}

func (serv *FeatureServer) getFeatureValues(ctx context.Context, name, variant string, entityMap map[string][]string) (*pb.ValueList, error) {

	obs := serv.Metrics.BeginObservingOnlineServe(name, variant)
	ctx = context.WithValue(ctx, observer{}, obs)
	defer obs.Finish()

	meta, err := serv.cacheFeatureMetadata(ctx, name, variant)
	if err != nil {
		return nil, err
	}

	var values []interface{}
	switch meta.Mode() {
	case metadata.PRECOMPUTED:
		precomputedValues, err := serv.getPrecomputedValues(ctx, entityMap, meta)
		if err != nil {
			return nil, err
		}
		for _, val := range precomputedValues {
			values = append(values, val.value)
		}
	case metadata.CLIENT_COMPUTED:
		values = append(values, meta.LocationFunction())
	default:
		return nil, fmt.Errorf("unknown computation mode %v", meta.Mode())
	}

	return serv.castValues(ctx, values)
}

func (serv *FeatureServer) cacheFeatureMetadata(ctx context.Context, name, variant string) (*metadata.FeatureVariant, error) {
	logger := serv.Logger
	obs := ctx.Value(observer{}).(metrics.FeatureObserver)
	// Checking if we've already cached a reference to the metadata for this feature. Otherwise
	// fetch it and cache it
	if feature, has := serv.Features.Load(serv.getNVCacheKey(name, variant)); has {
		return feature.(*metadata.FeatureVariant), nil
	} else {
		metaFeature, err := serv.Metadata.GetFeatureVariant(ctx, metadata.NameVariant{name, variant})
		if err != nil {
			logger.Errorw("metadata lookup failed", "Err", err)
			obs.SetError()
			return nil, fmt.Errorf("metadata lookup failed: %w", err)
		}
		serv.Features.Range(func(key, value interface{}) bool {
			return true
		})
		serv.Features.Store(serv.getNVCacheKey(name, variant), metaFeature)
		return metaFeature, nil
	}
}

func (serv *FeatureServer) getPrecomputedValues(ctx context.Context, entityMap map[string][]string, meta *metadata.FeatureVariant) ([]indexedValue, error) {
	logger := serv.Logger
	obs := ctx.Value(observer{}).(metrics.FeatureObserver)
	entities, has := entityMap[meta.Entity()]
	if !has {
		logger.Errorw("Entity not found", "Entity", meta.Entity())
		obs.SetError()
		return nil, fmt.Errorf("no value for entity %s", meta.Entity())
	}

	store, err := serv.cacheFeatureProvider(ctx, meta)
	if err != nil {
		logger.Errorw("Could not fetch provider", "Entity", meta.Entity())
		obs.SetError()
		return nil, fmt.Errorf("could not fetch online provider %s", meta.Provider())
	}

	featureTable, err := serv.cacheFeatureTable(ctx, store, meta.Name(), meta.Variant())
	if err != nil {
		return nil, err
	}

	featureValues, err := serv.getEntityValues(ctx, entities, featureTable)
	if err != nil {
		return nil, err
	}
	return featureValues, nil

}

func (serv *FeatureServer) cacheFeatureProvider(ctx context.Context, meta *metadata.FeatureVariant) (provider.OnlineStore, error) {
	if store, has := serv.Providers.Load(meta.Provider()); has {
		return store.(provider.OnlineStore), nil
	} else {
		store, err := serv.initializeFeatureProvider(ctx, meta)
		if err != nil {
			return nil, err
		}
		serv.Providers.Store(meta.Provider(), store)
		return store.(provider.OnlineStore), nil
	}
}

func (serv *FeatureServer) initializeFeatureProvider(ctx context.Context, meta *metadata.FeatureVariant) (provider.OnlineStore, error) {
	logger := serv.Logger
	obs := ctx.Value(observer{}).(metrics.FeatureObserver)
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
	return store, nil
}

func (serv *FeatureServer) cacheFeatureTable(ctx context.Context, store provider.OnlineStore, name, variant string) (provider.OnlineStoreTable, error) {
	obs := ctx.Value(observer{}).(metrics.FeatureObserver)

	var featureTable provider.OnlineStoreTable
	if table, has := serv.Tables.Load(serv.getNVCacheKey(name, variant)); has {
		featureTable = table.(provider.OnlineStoreTable)
	} else {
		table, err := store.GetTable(name, variant)
		if err != nil {
			serv.Logger.Errorw("feature not found", "Error", err)
			obs.SetError()
			return nil, fmt.Errorf("feature not found: %w", err)
		}
		serv.Tables.Store(serv.getNVCacheKey(name, variant), table)
		featureTable = table
	}
	return featureTable, nil
}

func (serv *FeatureServer) getEntityValues(ctx context.Context, entities []string, featureTable provider.OnlineStoreTable) ([]indexedValue, error) {
	obs := ctx.Value(observer{}).(metrics.FeatureObserver)

	valCh := make(chan indexedValue, len(entities))
	errCh := make(chan error, len(entities))

	for i, entityVal := range entities {
		// Start a goroutine for each entity
		go func(index int, ev string) {
			val, err := featureTable.(provider.OnlineStoreTable).Get(ev)
			if err != nil {
				// Push error into the error channel
				errCh <- fmt.Errorf("entity not found: %w", err)
				return
			}
			// If no error, push value into the value channel
			valCh <- indexedValue{index: index, value: val}
		}(i, entityVal)
	}

	// Collect results
	var results []indexedValue
	for range entities {
		select {
		case err := <-errCh:
			// If we get an error, stop and return it
			serv.Logger.Errorw("entity not found", "Error", err)
			obs.SetError()
			return nil, err
		case val := <-valCh:
			// Otherwise, add the value to the slice
			results = append(results, val)
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].index < results[j].index
	})
	return results, nil
}

func (serv *FeatureServer) castValues(ctx context.Context, values []interface{}) (*pb.ValueList, error) {
	obs := ctx.Value(observer{}).(metrics.FeatureObserver)
	castedValues := &pb.ValueList{}
	for _, val := range values {
		f, err := newValue(val)
		if err != nil {
			serv.Logger.Errorw("invalid feature type", "Error", err)
			obs.SetError()
			return nil, err
		}
		castedValues.Values = append(castedValues.Values, f.Serialized())
	}
	obs.ServeRow()
	return castedValues, nil
}
