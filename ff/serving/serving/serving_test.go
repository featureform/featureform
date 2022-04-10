package serving

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/featureform/serving/metadata"
	"github.com/featureform/serving/metrics"
	pb "github.com/featureform/serving/proto"
	"github.com/featureform/serving/provider"
)

const metadataAddr string = ":8989"

var serv *metadata.MetadataServer

func mockOnlineStoreFactory(c provider.SerializedConfig) (provider.Provider, error) {
	store := provider.NewLocalOnlineStore()
	table, err := store.CreateTable("feature", "variant")
	if err != nil {
		panic(err)
	}
	vals := []struct {
		Entity string
		Value  interface{}
	}{
		{"a", 12.5},
		{"b", "def"},
	}
	for _, val := range vals {
		if err := table.Set(val.Entity, val.Value); err != nil {
			panic(err)
		}
	}
	return store, nil
}

func startMetadata() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	config := &metadata.Config{
		Logger:          logger.Sugar(),
		Address:         metadataAddr,
		StorageProvider: metadata.LocalStorageProvider{},
	}
	serv, err = metadata.NewMetadataServer(config)
	if err != nil {
		panic(err)
	}
	go func() {
		if err := serv.Serve(); err != nil {
			panic(err)
		}
	}()
}

func stopMetadata() {
	serv.Stop()
}

func metadataClient(t *testing.T) *metadata.Client {
	logger := zaptest.NewLogger(t).Sugar()
	client, err := metadata.NewClient(metadataAddr, logger)
	if err != nil {
		t.Fatalf("Failed to create client: %s", err)
	}
	return client
}

func TestMain(m *testing.M) {
	startMetadata()
	defer stopMetadata()
	m.Run()
}

func TestFeatureServe(t *testing.T) {
	time.Sleep(time.Second)
	mockOnlineType := provider.Type(uuid.NewString())
	if err := provider.RegisterFactory(mockOnlineType, mockOnlineStoreFactory); err != nil {
		t.Fatalf("Failed to register factory: %s", err)
	}
	client := metadataClient(t)
	defs := []metadata.ResourceDef{
		metadata.UserDef{
			Name: "Featureform",
		},
		metadata.ProviderDef{
			Name: "mockOnline",
			Type: string(mockOnlineType),
		},
		metadata.EntityDef{
			Name: "mockEntity",
		},
		metadata.SourceDef{
			Name:     "mockSource",
			Variant:  "var",
			Owner:    "Featureform",
			Provider: "mockOnline",
		},
		metadata.FeatureDef{
			Name:     "feature",
			Variant:  "variant",
			Provider: "mockOnline",
			Entity:   "mockEntity",
			Source:   metadata.NameVariant{"mockSource", "var"},
			Owner:    "Featureform",
		},
	}
	if err := client.CreateAll(context.Background(), defs); err != nil {
		t.Fatalf("Failed to create metdata entries: %s", err)
	}
	logger := zaptest.NewLogger(t).Sugar()
	serv, err := NewFeatureServer(client, metrics.NewMetrics("abc"), logger)
	if err != nil {
		t.Fatalf("Failed to create feature server: %s", err)
	}
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			&pb.FeatureID{
				Name:    "feature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			&pb.Entity{
				Name:  "mockEntity",
				Value: "a",
			},
		},
	}
	resp, err := serv.FeatureServe(context.Background(), req)
	if err != nil {
		t.Fatalf("Failed to serve feature: %s", err)
	}
	vals := resp.Values
	if len(vals) != len(req.Features) {
		t.Fatalf("Wrong number of values: %d\nExpcted: %d", len(vals), len(req.Features))
	}
	dblVal := vals[0].Value.(*pb.Value_DoubleValue).DoubleValue
	if dblVal != 12.5 {
		t.Fatalf("Wrong feature value: %v\nExpcted: %v", dblVal, 12.5)
	}
}

func TestFeatureNotFound(t *testing.T) {
	time.Sleep(time.Second)
	logger := zaptest.NewLogger(t).Sugar()
	client := metadataClient(t)
	serv, err := NewFeatureServer(client, metrics.NewMetrics("def"), logger)
	if err != nil {
		t.Fatalf("Failed to create feature server: %s", err)
	}
	req := &pb.FeatureServeRequest{
		Features: []*pb.FeatureID{
			&pb.FeatureID{
				Name:    "nonexistantFeature",
				Version: "variant",
			},
		},
		Entities: []*pb.Entity{
			&pb.Entity{
				Name:  "mockEntity",
				Value: "a",
			},
		},
	}
	_, err = serv.FeatureServe(context.Background(), req)
	if err == nil {
		t.Fatalf("Succeeded in serving non-existant feature")
	}
	t.Fatalf("%T %s", err, err)
}
