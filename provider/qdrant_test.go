package provider

import (
	"os"
	"strconv"
	"testing"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
)

func TestVectorStoreQdrant(t *testing.T) {
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	grpcHost, ok := os.LookupEnv("QDRANT_GRPC_HOST")
	if !ok {
		t.Fatalf("QDRANT_GRPC_HOST variable not found.")
	}
	apiKey, ok := os.LookupEnv("QDRANT_API_KEY")
	if !ok {
		t.Log("QDRANT_API_KEY variable not found.")
	}

	useTlsEnv, ok := os.LookupEnv("QDRANT_USE_TLS")
	if !ok {
		t.Log("QDRANT_USE_TLS variable not found.")
		useTlsEnv = "false"
	}

	useTls, err := strconv.ParseBool(useTlsEnv)

	if err != nil {
		t.Fatalf("could not parse QDRANT_USE_TLS variable: %s", err)
	}

	qdrantConfig := &pc.QdrantConfig{
		GrpcHost: grpcHost,
		ApiKey:   apiKey,
		UseTls:   useTls,
	}

	store, err := GetOnlineStore(pt.QdrantOnline, qdrantConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := VectorStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
}
