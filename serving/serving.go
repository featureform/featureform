// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package serving

import (
	"context"
	"fmt"
	"sync"

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

func (serv *FeatureServer) GetTrainingTestSplit(stream pb.Feature_GetTrainingTestSplitServer) error {

	var finalTest, finalTrain provider.TrainingSetIterator
	var isTestFinished = false
	var isTrainFinished = false

	for {
		req, err := stream.Recv()

		serv.Logger.Infow("Getting training test split", "Request", req.String())
		serv.Logger.Infow("request type", "RequestType", req.RequestType.String())
		if err != nil {
			return err
		}

		id := req.GetId()
		reqType := req.GetRequestType()
		name, variant := id.GetName(), id.GetVersion()
		featureObserver := serv.Metrics.BeginObservingTrainingServe(name, variant)
		defer featureObserver.Finish()
		logger := serv.Logger.With("Name", name, "Variant", variant)

		// Handle initialization request
		if req.RequestType == pb.RequestType_INITIALIZE {
			fmt.Println("----------------ATHIS SHOULD HAPPEN FIRST_________________________")
			// Perform initialization based on the request
			// For example, load the dataset, shuffle it, split into training and test sets, etc.
			serv.Logger.Infow("Initializing dataset", "id", req.Id, "shuffle", req.Shuffle, "testSize", req.TestSize)

			//Initialization done, you might want to send a confirmation message back to the client
			//However, if your protocol doesn't require sending a response for initialization, you can skip this
			//Assuming you send some sort of acknowledgment back
			initResponse := &pb.GetTrainingTestSplitResponse{
				RequestType: pb.RequestType_INITIALIZE,
				TrainingTestSplit: &pb.GetTrainingTestSplitResponse_Initialized{
					Initialized: true,
				},
			}

			train, test, dropFunc, err := serv.getTrainingSetTestSplitIterator(name, variant, req.TestSize)
			defer dropFunc()
			fmt.Println("making sure train is not nil: ", train)
			finalTest = test
			finalTrain = train
			if err != nil {
				logger.Errorw("Failed to get training set iterator", "Error", err)
				featureObserver.SetError()
				return err
			}

			if err := stream.Send(initResponse); err != nil {
				serv.Logger.Errorw("Failed to send init response", "error", err)
				featureObserver.SetError()
				return err
			}
		} else {
			fmt.Println("----------------ATHIS SHOULD HAPPEN Second_________________________")
			logger.Info("Serving training split data")
			//if model := req.GetModel(); model != nil {
			//	trainingSets := []metadata.NameVariant{{Name: name, Variant: variant}}
			//	err := serv.Metadata.CreateModel(stream.Context(), metadata.ModelDef{Name: model.GetName(), Trainingsets: trainingSets})
			//	if err != nil {
			//		return err
			//	}
			//}
			fmt.Println("this is the test iterator", finalTest)
			fmt.Println("this is the train iterator", finalTrain)

			var thisIter provider.TrainingSetIterator
			if reqType == pb.RequestType_TEST {
				fmt.Println("this is a test request")
				thisIter = finalTest
			} else if reqType == pb.RequestType_TRAINING {
				fmt.Println("this is a training request")
				thisIter = finalTrain
			} else {
				return fmt.Errorf("invalid request type")
			}

			if thisIter.Next() {
				sRow, err := serializedRow(thisIter.Features(), thisIter.Label())
				logger.Infof("sRow: %v", sRow)
				if err != nil {
					return err
				}
				response := &pb.GetTrainingTestSplitResponse{
					RequestType:       reqType,
					TrainingTestSplit: &pb.GetTrainingTestSplitResponse_Row{Row: sRow},
				}

				if err := stream.Send(response); err != nil {
					logger.Errorw("Failed to write to stream", "Error", err)
					featureObserver.SetError()
					return err
				}
				featureObserver.ServeRow()
			} else {

				logger.Infof("Done with iterator of request type: %v", reqType)

				if reqType == pb.RequestType_TEST {
					isTestFinished = true
				} else if reqType == pb.RequestType_TRAINING {
					isTrainFinished = true
				}

				if isTestFinished && isTrainFinished {
					logger.Infof("Both iterators are finished")
					return nil
				} else {
					logger.Infof("Finished with one iterator, waiting for the other")
					if err := stream.Send(&pb.GetTrainingTestSplitResponse{
						IteratorDone: true,
					}); err != nil {
						logger.Errorw("Failed to write to stream", "Error", err)
					}
				}
			}

			logger.Infof("Done with iterator of request type: %v", reqType)
			if err := thisIter.Err(); err != nil {
				logger.Errorw("Dataset error", "Error", err)
				featureObserver.SetError()
				return err
			}
		}
	}
}

func (serv *FeatureServer) TrainingDataColumns(ctx context.Context, req *pb.TrainingDataColumnsRequest) (*pb.TrainingColumns, error) {
	id := req.GetId()
	name, variant := id.GetName(), id.GetVersion()
	serv.Logger.Infow("Getting training set columns", "Name", name, "Variant", variant)
	ts, err := serv.Metadata.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: name, Variant: variant})
	if err != nil {
		return nil, errors.Wrap(err, "could not get training set variant")
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

func (serv *FeatureServer) getTrainingSetTestSplitIterator(name, variant string, testSize float32) (provider.TrainingSetIterator, provider.TrainingSetIterator, func() error, error) {
	ctx := context.TODO()
	serv.Logger.Infow("Getting Training Set Iterator", "name", name, "variant", variant)
	ts, err := serv.Metadata.GetTrainingSetVariant(ctx, metadata.NameVariant{name, variant})
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "could not get training set variant")
	}
	serv.Logger.Debugw("Fetching Training Set Provider", "name", name, "variant", variant)
	providerEntry, err := ts.FetchProvider(serv.Metadata, ctx)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "could not get fetch provider")
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "could not get provider")
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		// This means that the provider of the training set isn't an offline store.
		// That shouldn't be possible.
		return nil, nil, nil, errors.Wrap(err, "could not open as offline store")
	}
	// assume clickhouse store
	clickhouseStore, ok := store.(*provider.ClickHouseOfflineStore)
	if !ok {
		return nil, nil, nil, errors.New("store is not a clickhouse store")
	}
	return clickhouseStore.GetTrainingSetTestSplit(provider.ResourceID{Name: name, Variant: variant}, testSize)
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
		return nil, errors.Wrap(err, "could not get Feature Variant")
	}
	serv.Logger.Debugw("Fetching Feature Provider from ", "name", ids[0].Name, "variant", ids[0].Variant)
	featureSource, err := feat.FetchSource(serv.Metadata, ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch source")
	}
	providerEntry, err := featureSource.FetchProvider(serv.Metadata, ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch provider")
	}
	providerName := providerEntry.Name()
	err = serv.checkFeatureSources(providerName, ids, ctx)
	if err != nil {
		return nil, err
	}

	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return nil, errors.Wrap(err, "could not get provider")
	}

	store, err := p.AsOfflineStore()
	if err != nil {
		// This means that the provider of the feature isn't an offline store.
		// That shouldn't be possible.
		return nil, errors.Wrap(err, "could not open as offline store")
	}
	return store.GetBatchFeatures(ids)
}

func (serv *FeatureServer) checkFeatureSources(firstProvider string, ids []provider.ResourceID, ctx context.Context) error {
	for id := range ids {
		id_feature, err := serv.Metadata.GetFeatureVariant(ctx, metadata.NameVariant{ids[id].Name, ids[id].Variant})
		if err != nil {
			return errors.Wrap(err, "could not get Feature Variant")
		}
		id_featureSource, err := id_feature.FetchSource(serv.Metadata, ctx)
		if err != nil {
			return errors.Wrap(err, "could not fetch source")
		}
		id_providerEntry, err := id_featureSource.FetchProvider(serv.Metadata, ctx)
		if err != nil {
			return errors.Wrap(err, "could not fetch provider")
		}
		if id_providerEntry.Name() != firstProvider {
			return errors.Wrap(err, "features have different providers")
		}
	}
	return nil
}

// Takes in a list of feature names and returns true if they all have the same entity name
func (serv *FeatureServer) checkEntityOfFeature(ids []provider.ResourceID) (bool, error) {
	ctx := context.TODO()
	entityName := ""
	// if len(features) != len(variants) {
	// 	return false, fmt.Errorf("Feature and Variant lists have different lengths")
	// }
	for _, resourceID := range ids {
		serv.Logger.Infow("Getting Feature Variant Iterator", "name", resourceID.Name, "variant", resourceID.Variant)
		feature, err := serv.Metadata.GetFeatureVariant(ctx, metadata.NameVariant{Name: resourceID.Name, Variant: resourceID.Variant})
		if err != nil {
			return false, errors.Wrap(err, "could not get feature variant")
		}
		correspondingEntity := feature.Entity()
		if entityName == "" {
			entityName = correspondingEntity
		} else if correspondingEntity != entityName {
			return false, fmt.Errorf("Entity names are not the same")
		}
	}
	return true, nil
}

func (serv *FeatureServer) getSourceDataIterator(name, variant string, limit int64) (provider.GenericTableIterator, error) {
	ctx := context.TODO()
	serv.Logger.Infow("Getting Source Variant Iterator", "name", name, "variant", variant)
	sv, err := serv.Metadata.GetSourceVariant(ctx, metadata.NameVariant{Name: name, Variant: variant})
	if err != nil {
		return nil, errors.Wrap(err, "could not get source variant")
	}
	// TODO: Determine if we want to add a backoff here to wait for the source
	if sv.Status() != metadata.READY {
		return nil, fmt.Errorf("source variant is not ready; current status is %v", sv.Status())
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
				providerErr = fmt.Errorf("transformation table is not a primary table")
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
		return nil, errors.Wrap(err, "could not get primary table")
	}
	serv.Logger.Debugw("Getting source data iterator", "name", name, "variant", variant)
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

// TODO: test serving embedding features
func (serv *FeatureServer) FeatureServe(ctx context.Context, req *pb.FeatureServeRequest) (*pb.FeatureRow, error) {
	features := req.GetFeatures()
	entities := req.GetEntities()
	entityMap := make(map[string][]string)

	for _, entity := range entities {
		entityMap[entity.GetName()] = entity.GetValues()
	}

	if model := req.GetModel(); model != nil {
		err := serv.addModel(ctx, model, features)
		if err != nil {
			return nil, err
		}
	}

	rows, err := serv.getFeatureRows(ctx, features, entityMap)
	if err != nil {
		return nil, err
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
			return err
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
		serv.Logger.Errorf("source data iterator is nil", "Name", name, "Variant", variant, "Error", err.Error())
		return nil, fmt.Errorf("could not fetch source data due to error; check the data source registration to ensure it is valid")
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
		return nil, fmt.Errorf("no embedding provided")
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
		serv.Logger.Errorw("failed to use table as vector store table", "Error", err)
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
			return "", errors.Wrap(err, "could not get source variant")
		}
		providerEntry, err = sv.FetchProvider(serv.Metadata, ctx)
		if err != nil {
			return "", errors.Wrap(err, "could not get fetch provider")
		}
	case provider.TrainingSet:
		serv.Logger.Infow("Getting Training Set Provider", "name", name, "variant", variant)
		ts, err := serv.Metadata.GetTrainingSetVariant(ctx, metadata.NameVariant{Name: name, Variant: variant})
		if err != nil {
			return "", errors.Wrap(err, "could not get training set variant")
		}
		providerEntry, err = ts.FetchProvider(serv.Metadata, ctx)
		if err != nil {
			return "", errors.Wrap(err, "could not get fetch provider")
		}
	default:
		return "", fmt.Errorf("invalid resource type")
	}
	p, err := provider.Get(pt.Type(providerEntry.Type()), providerEntry.SerializedConfig())
	if err != nil {
		return "", errors.Wrap(err, "could not get provider")
	}
	store, err := p.AsOfflineStore()
	if err != nil {
		return "", errors.Wrap(err, "could not open as offline store")
	}

	resourceID := provider.ResourceID{Name: name, Variant: variant, Type: provider.OfflineResourceType(resourceType)}
	fileLocation, err := store.ResourceLocation(resourceID)
	if err != nil {
		return "", errors.Wrap(err, "could not get resource location")
	}
	return fileLocation, nil
}

func (serv *FeatureServer) getOnlineResourceLocation(ctx context.Context, name, variant string, resourceType int32) (string, error) {
	return "", fmt.Errorf("online resource location not implemented")
}
