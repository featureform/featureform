package provider

import (
	"context"
	"fmt"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestVectorStorePinecone(t *testing.T) {
	t.Skip("temporarily disabled")
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	projectID, ok := os.LookupEnv("PINECONE_PROJECT_ID")
	if !ok {
		t.Fatalf("missing PINECONE_PROJECT_ID variable")
	}
	environment, ok := os.LookupEnv("PINECONE_ENVIRONMENT")
	if !ok {
		t.Fatalf("missing PINECONE_ENVIRONMENT variable")
	}
	apiKey, ok := os.LookupEnv("PINECONE_API_KEY")
	if !ok {
		t.Fatalf("missing PINECONE_API_KEY variable")
	}
	pineconeConfig := &pc.PineconeConfig{
		ProjectID:   projectID,
		Environment: environment,
		ApiKey:      apiKey,
	}

	store, err := GetOnlineStore(pt.RedisOnline, pineconeConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := VectorStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
}

func TestPineconeAPI(t *testing.T) {
	t.Skip("Temporarily skipping test")
	err := godotenv.Load("../.env")
	if err != nil {
		t.Fatalf("Error loading .env file: %v", err)
	}

	config := &pc.PineconeConfig{
		ProjectID:   os.Getenv("PINECONE_PROJECT_ID"),
		Environment: os.Getenv("PINECONE_ENVIRONMENT"),
		ApiKey:      os.Getenv("PINECONE_API_KEY"),
	}

	api := NewPineconeAPI(config)
	feature, variant := randomFeatureVariant()
	uuid := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(fmt.Sprintf("%s-%s", feature, variant)))
	indexName := fmt.Sprintf("ff-idx--%s", uuid.String())
	namespace := fmt.Sprintf("ff-namespace--%s-%s", feature, variant)
	var dimension int32 = 768
	vectors := getTestVectorEntities(t)

	//	CREATE INDEX

	createIndexAndWait(t, api, indexName, dimension, 3*time.Minute)

	// UPSERT VECTOR

	for _, vector := range vectors {
		if err := api.upsert(indexName, namespace, vector.entity, vector.vector); err != nil {
			t.Fatalf("Error upserting vector: %v", err)
		}
	}

	// FETCH VECTOR

	expected := vectors[0]
	received, err := api.fetch(indexName, namespace, expected.entity)
	if err != nil {
		t.Fatalf("Error fetching vector: %v", err)
	}
	if !reflect.DeepEqual(expected.vector, received) {
		t.Fatalf("Expected %v, got %v", expected.vector, received)
	}

	// QUERY VECTOR

	searchVector := getSearchVector(t)
	results, err := api.query(indexName, namespace, searchVector, 2)
	if err != nil {
		t.Fatalf("Error querying vector: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	// DELETE INDEX

	if err := api.deleteIndex(indexName); err != nil {
		t.Fatalf("Error deleting index: %v", err)
	}
}

func createIndexAndWait(t *testing.T, api *pineconeAPI, indexName string, dimension int32, duration time.Duration) {
	if err := api.createIndex(indexName, dimension); err != nil {
		t.Fatalf("Error creating index: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Timed out waiting for index to be created")
		case <-ticker.C:
			dim, status, err := api.describeIndex(indexName)
			if err != nil {
				t.Fatalf("Error describing index: %v", err)
			}
			if dim != dimension {
				t.Fatalf("Expected dimension %d, got %d", dimension, dim)
			}
			if status == Ready {
				return
			}
		}
	}
}
