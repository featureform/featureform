package provider

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"

	"github.com/featureform/fferr"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
	"github.com/google/uuid"
	pb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type qdrantOnlineStore struct {
	collections_client pb.CollectionsClient
	points_client      pb.PointsClient
	service_client     pb.QdrantClient
	connection         *grpc.ClientConn
	apiKey             string
	BaseProvider
}

func qdrantOnlineStoreFactory(serialized pc.SerializedConfig) (Provider, error) {
	qdrantConfig := &pc.QdrantConfig{}
	if err := qdrantConfig.Deserialize(serialized); err != nil {
		return nil, err
	}
	return NewQdrantOnlineStore(qdrantConfig)
}

func NewQdrantOnlineStore(options *pc.QdrantConfig) (*qdrantOnlineStore, error) {

	var tlsCredential credentials.TransportCredentials

	if !options.UseTls && options.ApiKey != "" {
		log.Println("Warning: API key is set but TLS is not enabled. The API key will be sent in plaintext.")
		log.Println("May fail when using Qdrant cloud.")
	}

	if options.UseTls {
		tlsCredential = credentials.NewTLS(&tls.Config{})
	} else {
		tlsCredential = insecure.NewCredentials()
	}

	conn, err := grpc.Dial(options.GrpcHost, grpc.WithTransportCredentials(tlsCredential))
	if err != nil {
		return nil, fferr.NewConnectionError(pt.QdrantOnline.String(), err)
	}

	return &qdrantOnlineStore{
		collections_client: pb.NewCollectionsClient(conn),
		points_client:      pb.NewPointsClient(conn),
		service_client:     pb.NewQdrantClient(conn),
		connection:         conn,
		apiKey:             options.ApiKey,
		BaseProvider: BaseProvider{
			ProviderType:   pt.QdrantOnline,
			ProviderConfig: options.Serialize(),
		},
	}, nil
}

func (store *qdrantOnlineStore) AsOnlineStore() (OnlineStore, error) {
	return store, nil
}

func (store *qdrantOnlineStore) Close() error {
	return store.connection.Close()
}

func (store *qdrantOnlineStore) CreateTable(feature, variant string, valueType types.ValueType) (OnlineStoreTable, error) {
	return &qdrantOnlineTable{
		store:          store,
		collectionName: store.getCollectionName(feature, variant),
		valueType:      valueType,
	}, nil
}

func (store *qdrantOnlineStore) DeleteTable(feature, variant string) error {

	ctx := store.appendApiKeyToContext(context.TODO())

	_, err := store.collections_client.Delete(ctx, &pb.DeleteCollection{
		CollectionName: store.getCollectionName(feature, variant),
	})

	return err
}

func (store *qdrantOnlineStore) CheckHealth() (bool, error) {

	ctx := store.appendApiKeyToContext(context.TODO())

	response, err := store.service_client.HealthCheck(ctx, &pb.HealthCheckRequest{})

	if err != nil {
		return false, err
	}
	return response.GetTitle() == "qdrant - vector search engine", nil
}

func (store *qdrantOnlineStore) CreateIndex(feature, variant string, vectorType types.VectorType) (VectorStoreTable, error) {
	collectionName := store.getCollectionName(feature, variant)

	ctx := store.appendApiKeyToContext(context.TODO())

	_, err := store.collections_client.Create(ctx, &pb.CreateCollection{
		CollectionName: collectionName,
		VectorsConfig: &pb.VectorsConfig{
			Config: &pb.VectorsConfig_Params{
				Params: &pb.VectorParams{
					Size:     uint64(vectorType.Dimension),
					Distance: pb.Distance_Cosine,
				},
			},
		},
	})

	if err != nil {
		return nil, fferr.NewInternalError(err)
	}

	return qdrantOnlineTable{
		store:          store,
		collectionName: collectionName,
		valueType: types.VectorType{
			Dimension:   vectorType.Dimension,
			ScalarType:  types.Float32,
			IsEmbedding: true,
		},
	}, nil
}

func (store *qdrantOnlineStore) DeleteIndex(feature, variant string) error {
	collectionName := store.getCollectionName(feature, variant)

	ctx := store.appendApiKeyToContext(context.TODO())

	_, err := store.collections_client.Delete(ctx, &pb.DeleteCollection{
		CollectionName: collectionName,
	})

	return err
}

func (store *qdrantOnlineStore) GetTable(feature, variant string) (OnlineStoreTable, error) {
	collectionName := store.getCollectionName(feature, variant)

	ctx := store.appendApiKeyToContext(context.TODO())

	collection_info, err := store.collections_client.Get(ctx, &pb.GetCollectionInfoRequest{
		CollectionName: collectionName,
	})

	if err != nil {
		return nil, fferr.NewInternalError(err)
	}

	dimension := collection_info.Result.GetConfig().GetParams().GetVectorsConfig().GetParams().GetSize()

	return qdrantOnlineTable{
		store:          store,
		collectionName: collectionName,
		valueType: types.VectorType{
			Dimension:   int32(dimension),
			ScalarType:  types.Float32,
			IsEmbedding: true,
		},
	}, nil
}

func (store *qdrantOnlineStore) getCollectionName(feature, variant string) string {
	uuid := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(fmt.Sprintf("%s-%s", feature, variant)))
	return fmt.Sprintf("ff-%s", uuid.String())
}

func (store *qdrantOnlineStore) appendApiKeyToContext(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "api-key", store.apiKey)
}

type qdrantOnlineTable struct {
	store          *qdrantOnlineStore
	collectionName string
	valueType      types.ValueType
}

func (table qdrantOnlineTable) Set(entity string, value interface{}) error {
	vector, isVector := value.([]float32)
	if !isVector {
		wrapped := fferr.NewInvalidArgumentError(fmt.Errorf("expected value to be of type []float32, got %T", value))
		wrapped.AddDetail("provider", pt.QdrantOnline.String())
		wrapped.AddDetail("entity", entity)
		wrapped.AddDetail("collection_name", table.collectionName)
		return wrapped
	}

	ctx := table.store.appendApiKeyToContext(context.TODO())

	_, err := table.store.points_client.Upsert(ctx, &pb.UpsertPoints{
		CollectionName: table.collectionName,
		Points: []*pb.PointStruct{
			{
				Id: &pb.PointId{
					PointIdOptions: &pb.PointId_Uuid{
						Uuid: generatePointId(entity),
					},
				},
				Payload: map[string]*pb.Value{
					"id": {
						Kind: &pb.Value_StringValue{StringValue: entity},
					},
				},
				Vectors: &pb.Vectors{VectorsOptions: &pb.Vectors_Vector{Vector: &pb.Vector{Data: vector}}},
			},
		},
	})

	if err != nil {
		return fferr.NewInternalError(err)
	}

	return nil
}

func (table qdrantOnlineTable) Get(entity string) (interface{}, error) {
	pointId := generatePointId(entity)

	ctx := table.store.appendApiKeyToContext(context.TODO())

	points, err := table.store.points_client.Get(ctx, &pb.GetPoints{
		CollectionName: table.collectionName,
		Ids:            []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: pointId}}},
		WithVectors: &pb.WithVectorsSelector{
			SelectorOptions: &pb.WithVectorsSelector_Enable{
				Enable: true,
			},
		},
	})

	if err != nil {
		return nil, fferr.NewInternalError(err)
	}

	if len(points.GetResult()) == 0 {
		wrapped := fferr.NewDatasetNotFoundError(pointId, "", fmt.Errorf("point not found"))
		wrapped.AddDetail("id", entity)
		wrapped.AddDetail("collection_name", table.collectionName)
		return nil, wrapped
	}
	return points.GetResult()[0].GetVectors().VectorsOptions.(*pb.Vectors_Vector).Vector.Data, nil
}

func (table qdrantOnlineTable) Nearest(feature, variant string, vector []float32, k int32) ([]string, error) {

	ctx := table.store.appendApiKeyToContext(context.TODO())

	response, err := table.store.points_client.Search(ctx, &pb.SearchPoints{
		CollectionName: table.collectionName,
		Vector:         vector,
		Limit:          uint64(k),
		WithPayload: &pb.WithPayloadSelector{
			SelectorOptions: &pb.WithPayloadSelector_Include{
				Include: &pb.PayloadIncludeSelector{
					Fields: []string{"id"},
				},
			},
		},
	})

	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	results := make([]string, k)
	for i, result := range response.GetResult() {
		results[i] = result.GetPayload()["id"].GetStringValue()
	}
	return results, nil
}

// Generates a deterministic UUID for an arbitrary ID and returns the string representation.
// Qdrant only allows UUIDs and positive integers as point IDs.
func generatePointId(id string) string {
	uuid := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(id))
	return uuid.String()
}
