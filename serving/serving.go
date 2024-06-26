// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package serving

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/featureform/fferr"
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
			return fferr.NewInternalError(err)
		}
		featureObserver.ServeRow()
	}
	if err := iter.Err(); err != nil {
		logger.Errorw("Dataset error", "Error", err)
		featureObserver.SetError()
		return err
	}

	logger.Info("Creating model")
	if model := req.GetModel(); model != nil {
		trainingSets := []metadata.NameVariant{{Name: name, Variant: variant}}
		err := serv.Metadata.CreateModel(stream.Context(), metadata.ModelDef{Name: model.GetName(), Trainingsets: trainingSets})
		if err != nil {
			return err
		}
	}

	return nil
}

type splitContext struct {
	stream          pb.Feature_TrainTestSplitServer
	req             *pb.TrainTestSplitRequest
	trainIterator   *provider.TrainingSetIterator
	testIterator    *provider.TrainingSetIterator
	isTestFinished  *bool
	isTrainFinished *bool
	logger          *zap.SugaredLogger
}

func (serv *FeatureServer) TrainTestSplit(stream pb.Feature_TrainTestSplitServer) error {
	var (
		trainIter, testIter provider.TrainingSetIterator
		isTrainFinished     bool
		isTestFinished      bool
	)

	for {
		if isTrainFinished && isTestFinished {
			serv.Logger.Infow("Both iterators are finished, closing stream")
			// returning nil will close the stream
			return nil
		}

		req, err := stream.Recv()

		if err == io.EOF {
			serv.Logger.Infow("Stream closed by client")
			return nil
		}

		if err != nil {
			return err
		}

		id := req.GetId()
		name, variant := id.GetName(), id.GetVersion()
		logger := serv.Logger.With("Name", name, "Variant", variant, "RequestType", req.GetRequestType().String())

		featureObserver := serv.Metrics.BeginObservingTrainingServe(name, variant)
		defer featureObserver.Finish()

		splitContext := splitContext{
			stream:          stream,
			req:             req,
			trainIterator:   &trainIter,
			testIterator:    &testIter,
			isTestFinished:  &isTestFinished,
			isTrainFinished: &isTrainFinished,
			logger:          logger,
		}

		switch req.GetRequestType() {
		case pb.RequestType_INITIALIZE:
			if err := serv.handleSplitInitializeRequest(&splitContext); err != nil {
				featureObserver.SetError()
				return err
			}
		default:
			if err := serv.handleSplitDataRequest(&splitContext); err != nil {
				featureObserver.SetError()
				return err
			}
		}
	}
}

func (serv *FeatureServer) handleSplitInitializeRequest(splitContext *splitContext) error {
	splitContext.logger.Infow("Initializing dataset", "id", splitContext.req.Id, "shuffle", splitContext.req.Shuffle, "testSize", splitContext.req.TestSize)

	trainTestSplitDef := provider.TrainTestSplitDef{
		TrainingSetName:    splitContext.req.Id.GetName(),
		TrainingSetVariant: splitContext.req.Id.GetVersion(),
		TestSize:           splitContext.req.TestSize,
		Shuffle:            splitContext.req.Shuffle,
		RandomState:        int(splitContext.req.RandomState),
	}

	cleanupFunc, err := serv.createTrainTestSplit(trainTestSplitDef)
	defer cleanupFunc()
	if err != nil {
		return fferr.NewInternalError(err)
	}
	train, test, err := serv.getTrainTestSplitIterators(trainTestSplitDef)
	if err != nil {
		splitContext.logger.Errorw("Failed to get training set iterator", "Error", err)
		return err
	}

	*splitContext.trainIterator = train
	*splitContext.testIterator = test

	initResponse := &pb.BatchTrainTestSplitResponse{
		RequestType: pb.RequestType_INITIALIZE,
		Result:      &pb.BatchTrainTestSplitResponse_Initialized{Initialized: true},
	}

	if err := splitContext.stream.Send(initResponse); err != nil {
		splitContext.logger.Errorw("Failed to send init response", "error", err)
		return err
	}

	return nil
}

func (serv *FeatureServer) handleSplitDataRequest(splitContext *splitContext) error {
	var thisIter provider.TrainingSetIterator
	switch splitContext.req.GetRequestType() {
	case pb.RequestType_TRAINING:
		thisIter = *splitContext.trainIterator
	case pb.RequestType_TEST:
		thisIter = *splitContext.testIterator
	default:
		return fmt.Errorf("invalid request type")
	}

	rows := 0
	trainingDataRows := make([]*pb.TrainingDataRow, 0)

	for rows < int(splitContext.req.BatchSize) {
		if thisIter.Next() {
			sRow, err := serializedRow(thisIter.Features(), thisIter.Label())
			if err != nil {
				return err
			}
			trainingDataRows = append(trainingDataRows, sRow)
			rows++
		} else {
			// if we reach the end of the iterator mid-batch, we'll send the processed rows so far and end the iteration
			serv.handleFinishedIterator(trainingDataRows, splitContext)
		}
	}

	response := &pb.BatchTrainTestSplitResponse{
		Result: &pb.BatchTrainTestSplitResponse_Data{
			Data: &pb.TrainingDataRows{Rows: trainingDataRows},
		},
	}

	if err := splitContext.stream.Send(response); err != nil {
		splitContext.logger.Errorw("Failed to write to stream", "Error", err)
		return err
	}

	if thisIter.Err() != nil {
		splitContext.logger.Errorw("Dataset error", "Error", thisIter.Err())
		return thisIter.Err()
	}

	return nil
}

func (serv *FeatureServer) handleFinishedIterator(trainingDataRows []*pb.TrainingDataRow, splitContext *splitContext) {
	if splitContext.req.GetRequestType() == pb.RequestType_TEST {
		*splitContext.isTestFinished = true
	} else if splitContext.req.GetRequestType() == pb.RequestType_TRAINING {
		*splitContext.isTrainFinished = true
	}

	if *splitContext.isTestFinished && *splitContext.isTrainFinished {
		return // return so that we can close the stream
	} else {
		response := &pb.BatchTrainTestSplitResponse{
			Result: &pb.BatchTrainTestSplitResponse_Data{
				Data: &pb.TrainingDataRows{Rows: trainingDataRows},
			},
			IteratorDone: true,
		}

		if err := splitContext.stream.Send(response); err != nil {
			splitContext.logger.Errorw("Failed to write to stream", "Error", err)
		}
	}
}

func (serv *FeatureServer) TrainingDataColumns(ctx context.Context, req *pb.TrainingDataColumnsRequest) (*pb.TrainingColumns, error) {
	id := req.GetId()
	name, variant := id.GetName(), id.GetVersion()
	serv.Logger.Infow("Getting training set columns", "Name", name, "Variant", variant)
	ts, err := serv.Metadata.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: name, Variant: variant})
	if err != nil {
		return nil, err
	}
	fv := ts.Features()
	features := make([]string, len(fv))
	for i, f := range fv {
		features[i] = fmt.Sprintf("feature__%s__%s", f.Name, f.Variant)
	}
	lv := ts.Label()
	label := fmt.Sprintf("label__%s__%s", lv.Name, lv.Variant)
	return &pb.TrainingColumns{
		Features: features,
		Label:    label,
	}, nil
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
		sRow, err := SerializedSourceRow(iter.Values())
		if err != nil {
			return err
		}
		if err := stream.Send(sRow); err != nil {
			logger.Errorw("Failed to write to source data stream", "Error", err)
			return fferr.NewInternalError(err)
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
	ts, err := serv.Metadata.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: name, Variant: variant})
	if err != nil {
		return nil, err
	}
	serv.Logger.Debugw("Fetching Training Set Provider", "name", name, "variant", variant)
	providerEntry, err := ts.FetchProvider(serv.Metadata, ctx)
	if err != nil {
		return nil, err
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, err
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		serv.Logger.Errorw("Training set provider is not an offline store", "Error", err)
		return nil, err
	}
	serv.Logger.Debugw("Get Training Set From Store", "name", name, "variant", variant)
	return store.GetTrainingSet(provider.ResourceID{Name: name, Variant: variant})
}

func (serv *FeatureServer) createTrainTestSplit(def provider.TrainTestSplitDef) (func() error, error) {
	ctx := context.TODO()
	serv.Logger.Infow("Creating Train Test Split", "TrainTestSplitDef", fmt.Sprintf("%+v", def))
	ts, err := serv.Metadata.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: def.TrainingSetName, Variant: def.TrainingSetVariant})
	if err != nil {
		return nil, err
	}
	providerEntry, err := ts.FetchProvider(serv.Metadata, ctx)
	if err != nil {
		return nil, err
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, err
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		return nil, err
	}

	return store.CreateTrainTestSplit(def)
}

func (serv *FeatureServer) getTrainTestSplitIterators(def provider.TrainTestSplitDef) (provider.TrainingSetIterator, provider.TrainingSetIterator, error) {
	ctx := context.TODO()
	serv.Logger.Infow("Getting Training Set Iterator", "name", def.TrainingSetName, "variant", def.TrainingSetVariant)
	ts, err := serv.Metadata.GetTrainingSetVariant(ctx, metadata.NameVariant{def.TrainingSetName, def.TrainingSetVariant})
	if err != nil {
		return nil, nil, err
	}
	serv.Logger.Debugw("Fetching Training Set Provider", "name", def.TrainingSetName, "variant", def.TrainingSetVariant)
	providerEntry, err := ts.FetchProvider(serv.Metadata, ctx)
	if err != nil {
		return nil, nil, err
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, nil, err
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		serv.Logger.Errorw("Training set provider is not an offline store", "Error", err)
		return nil, nil, err
	}
	return store.GetTrainTestSplit(def)
}

func (serv *FeatureServer) getBatchFeatureIterator(ids []provider.ResourceID) (provider.BatchFeatureIterator, error) {
	ctx := context.TODO()
	_, err := serv.checkEntityOfFeature(ids)
	if err != nil {
		return nil, err
	}

	// Assuming that all the features have the same offline provider
	feat, err := serv.Metadata.GetFeatureVariant(ctx, metadata.NameVariant{ids[0].Name, ids[0].Variant})
	if err != nil {
		return nil, err
	}
	serv.Logger.Debugw("Fetching Feature Provider from ", "name", ids[0].Name, "variant", ids[0].Variant)
	featureSource, err := feat.FetchSource(serv.Metadata, ctx)
	if err != nil {
		return nil, err
	}
	providerEntry, err := featureSource.FetchProvider(serv.Metadata, ctx)
	if err != nil {
		return nil, err
	}
	providerName := providerEntry.Name()
	err = serv.checkFeatureSources(providerName, ids, ctx)
	if err != nil {
		return nil, err
	}

	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, err
	}

	store, err := p.AsOfflineStore()
	if err != nil {
		// This means that the provider of the feature isn't an offline store.
		// That shouldn't be possible.
		return nil, err
	}
	return store.GetBatchFeatures(ids)
}

func (serv *FeatureServer) checkFeatureSources(firstProvider string, ids []provider.ResourceID, ctx context.Context) error {
	for id := range ids {
		id_feature, err := serv.Metadata.GetFeatureVariant(ctx, metadata.NameVariant{Name: ids[id].Name, Variant: ids[id].Variant})
		if err != nil {
			return err
		}
		id_featureSource, err := id_feature.FetchSource(serv.Metadata, ctx)
		if err != nil {
			return err
		}
		id_providerEntry, err := id_featureSource.FetchProvider(serv.Metadata, ctx)
		if err != nil {
			return err
		}
		if id_providerEntry.Name() != firstProvider {
			return err
		}
	}
	return nil
}

// Takes in a list of feature names and returns true if they all have the same entity name
func (serv *FeatureServer) checkEntityOfFeature(ids []provider.ResourceID) (bool, error) {
	ctx := context.TODO()
	entityName := ""
	for _, resourceID := range ids {
		serv.Logger.Infow("Getting Feature Variant Iterator", "name", resourceID.Name, "variant", resourceID.Variant)
		feature, err := serv.Metadata.GetFeatureVariant(ctx, metadata.NameVariant{Name: resourceID.Name, Variant: resourceID.Variant})
		if err != nil {
			return false, err
		}
		correspondingEntity := feature.Entity()
		if entityName == "" {
			entityName = correspondingEntity
		} else if correspondingEntity != entityName {
			return false, fferr.NewInternalError(fmt.Errorf("entity names are not the same"))
		}
	}
	return true, nil
}

func (serv *FeatureServer) getSourceDataIterator(name, variant string, limit int64) (provider.GenericTableIterator, error) {
	ctx := context.TODO()
	serv.Logger.Infow("Getting Source Variant Iterator", "name", name, "variant", variant)
	sv, err := serv.Metadata.GetSourceVariant(ctx, metadata.NameVariant{Name: name, Variant: variant})
	if err != nil {
		return nil, err
	}
	// TODO: Determine if we want to add a backoff here to wait for the source
	if sv.Status() != metadata.READY {
		return nil, fferr.NewResourceNotReadyError(name, variant, "SOURCE_VARIANT", fmt.Errorf("current status: %s", sv.Status()))
	}
	providerEntry, err := sv.FetchProvider(serv.Metadata, ctx)
	serv.Logger.Debugw("Fetched Source Variant Provider", "name", providerEntry.Name(), "type", providerEntry.Type())
	if err != nil {
		return nil, err
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, err
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		return nil, err
	}
	var primary provider.PrimaryTable
	var providerErr error
	if sv.IsTransformation() {
		serv.Logger.Debugw("Getting transformation table", "name", name, "variant", variant)
		t, err := store.GetTransformationTable(provider.ResourceID{Name: name, Variant: variant, Type: provider.Transformation})
		if err != nil {
			serv.Logger.Errorw("Could not get transformation table", "name", name, "variant", variant, "Error", err)
			providerErr = err
		} else {
			// TransformationTable inherits from PrimaryTable, which is where
			// IterateSegment is defined; we assert this type to get access to
			// the method. This assertion should never fail.
			if tbl, isPrimaryTable := t.(provider.PrimaryTable); !isPrimaryTable {
				serv.Logger.Errorw("transformation table is not a primary table", "name", name, "variant", variant)
				providerErr = fferr.NewInvalidResourceTypeError(name, variant, fferr.SOURCE_VARIANT, fmt.Errorf("transformation table is not a primary table"))
			} else {
				primary = tbl
			}
		}
	} else {
		serv.Logger.Debugw("Getting primary table", "name", name, "variant", variant)
		primary, providerErr = store.GetPrimaryTable(provider.ResourceID{Name: name, Variant: variant, Type: provider.Primary})
	}
	if providerErr != nil {
		serv.Logger.Errorw("Could not get primary table", "name", name, "variant", variant, "Error", providerErr)
		return nil, providerErr
	}
	serv.Logger.Debugw("Getting source data iterator", "name", name, "variant", variant, "limit", limit)
	return primary.IterateSegment(limit)
}

func (serv *FeatureServer) addModel(ctx context.Context, model *pb.Model, features []*pb.FeatureID) error {
	modelFeatures := make([]metadata.NameVariant, len(features))
	for i, feature := range features {
		modelFeatures[i] = metadata.NameVariant{Name: feature.Name, Variant: feature.Version}
	}
	serv.Logger.Infow("Creating model", "Name", model.GetName())
	err := serv.Metadata.CreateModel(ctx, metadata.ModelDef{Name: model.GetName(), Features: modelFeatures})
	if err != nil {
		return err
	}
	return nil
}

type observer struct{}

func (serv *FeatureServer) FeatureServe(ctx context.Context, req *pb.FeatureServeRequest) (*pb.FeatureRow, error) {
	features := req.GetFeatures()
	entities := req.GetEntities()
	entityMap := make(map[string][]string)

	for _, entity := range entities {
		entityMap[entity.GetName()] = entity.GetValues()
	}

	rows, err := serv.getFeatureRows(ctx, features, entityMap)
	if err != nil {
		return nil, err
	}

	if model := req.GetModel(); model != nil {
		err := serv.addModel(ctx, model, features)
		if err != nil {
			return nil, err
		}
	}

	return &pb.FeatureRow{
		ValueLists: rows,
	}, nil
}

func (serv *FeatureServer) getNVCacheKey(name, variant string) string {
	return fmt.Sprintf("%s:%s", name, variant)
}

func (serv *FeatureServer) BatchFeatureServe(req *pb.BatchFeatureServeRequest, stream pb.Feature_BatchFeatureServeServer) error {
	features := req.GetFeatures()
	resourceIDList := make([]provider.ResourceID, len(features))
	for i, feature := range features {
		name, variant := feature.GetName(), feature.GetVersion()
		serv.Logger.Infow("Serving feature", "Name", name, "Variant", variant)
		resourceIDList[i] = provider.ResourceID{Name: name, Variant: variant, Type: provider.Feature}
	}
	iter, err := serv.getBatchFeatureIterator(resourceIDList)
	if err != nil {
		return err
	}
	for iter.Next() {
		sRow, err := serializedBatchRow(iter.Entity(), iter.Features())
		if err != nil {
			return err
		}
		if err := stream.Send(sRow); err != nil {
			return fferr.NewInternalError(err)
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	return nil
}

func (serv *FeatureServer) SourceColumns(ctx context.Context, req *pb.SourceColumnRequest) (*pb.SourceDataColumns, error) {
	id := req.GetId()
	name, variant := id.GetName(), id.GetVersion()
	serv.Logger.Infow("Getting source columns", "Name", name, "Variant", variant)
	it, err := serv.getSourceDataIterator(name, variant, 0) // Set limit to zero to fetch columns only
	if err != nil {
		return nil, err
	}
	if it == nil {
		serv.Logger.Errorw("source data iterator is nil", "Name", name, "Variant", variant)
		return nil, fferr.NewDatasetNotFoundError(name, variant, fmt.Errorf("source data iterator is nil"))
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
	fv, err := serv.Metadata.GetFeatureVariant(ctx, metadata.NameVariant{Name: name, Variant: variant})
	if err != nil {
		serv.Logger.Errorw("metadata lookup failed", "Err", err)
		return nil, err
	}
	vectorTable, err := serv.getVectorTable(ctx, fv)
	if err != nil {
		serv.Logger.Errorw("failed to get vector table", "Error", err)
		return nil, err
	}
	searchVector := req.GetVector()
	k := req.GetK()
	if searchVector == nil {
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("no embedding provided"))
	}
	entities, err := vectorTable.Nearest(name, variant, searchVector.Value, k)
	if err != nil {
		serv.Logger.Errorw("nearest search failed", "Error", err)
		return nil, err
	}
	return &pb.NearestResponse{
		Entities: entities,
	}, nil
}

func (serv *FeatureServer) getVectorTable(ctx context.Context, fv *metadata.FeatureVariant) (provider.VectorStoreTable, error) {
	providerEntry, err := fv.FetchProvider(serv.Metadata, ctx)
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
	table, err := store.GetTable(fv.Name(), fv.Variant())
	if err != nil {
		serv.Logger.Errorw("feature not found", "Error", err)
		return nil, err
	}
	vectorTable, ok := table.(provider.VectorStoreTable)
	if !ok {
		serv.Logger.Errorw("failed to use table as vector store table")
		return nil, fferr.NewInternalError(fmt.Errorf("received %T; expected VectorStoreTable", table))
	}
	return vectorTable, nil
}

func (serv *FeatureServer) GetResourceLocation(ctx context.Context, req *pb.ResourceIdRequest) (*pb.ResourceLocation, error) {
	// Pulls the location of a resource from the provider

	name := req.GetName()
	variant := req.GetVariant()
	resourceType := req.GetType()
	serv.Logger.Infow("Getting the Resource Location:", "Name", name, "Variant", variant, "Type", resourceType)

	var location string
	var err error
	if provider.OfflineResourceType(resourceType) == provider.Feature {
		location, err = serv.getOnlineResourceLocation(ctx, name, variant, resourceType)
	} else {
		location, err = serv.getOfflineResourceLocation(ctx, name, variant, resourceType)
		if err != nil {
			return nil, err
		}
	}

	return &pb.ResourceLocation{
		Location: location,
	}, nil
}

func (serv *FeatureServer) getOfflineResourceLocation(ctx context.Context, name, variant string, resourceType int32) (string, error) {
	var providerEntry *metadata.Provider
	switch provider.OfflineResourceType(resourceType) {
	case provider.Primary, provider.Transformation:
		serv.Logger.Infow("Getting Source Variant Provider", "name", name, "variant", variant)
		sv, err := serv.Metadata.GetSourceVariant(ctx, metadata.NameVariant{Name: name, Variant: variant})
		if err != nil {
			return "", err
		}
		providerEntry, err = sv.FetchProvider(serv.Metadata, ctx)
		if err != nil {
			return "", err
		}
	case provider.TrainingSet:
		serv.Logger.Infow("Getting Training Set Provider", "name", name, "variant", variant)
		ts, err := serv.Metadata.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: name, Variant: variant})
		if err != nil {
			return "", err
		}
		providerEntry, err = ts.FetchProvider(serv.Metadata, ctx)
		if err != nil {
			return "", err
		}
	default:
		return "", fferr.NewInvalidResourceTypeError(name, variant, fferr.ResourceType(metadata.ResourceType(resourceType).String()), fmt.Errorf("invalid resource type"))
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return "", err
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		return "", err
	}

	resourceID := provider.ResourceID{Name: name, Variant: variant, Type: provider.OfflineResourceType(resourceType)}
	fileLocation, err := store.ResourceLocation(resourceID)
	if err != nil {
		return "", err
	}
	return fileLocation, nil
}

func (serv *FeatureServer) getOnlineResourceLocation(ctx context.Context, name, variant string, resourceType int32) (string, error) {
	return "", fferr.NewInternalError(fmt.Errorf("online resource location not implemented"))
}
