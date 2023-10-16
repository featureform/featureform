package health

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/featureform/metadata"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
	"go.uber.org/zap/zaptest"
)

var providerType = flag.String("provider", "", "provider type under test")

type testMember struct {
	providerDef metadata.ProviderDef
}

func checkEnv(envVar string) string {
	value, has := os.LookupEnv(envVar)
	if !has {
		panic(fmt.Sprintf("Environment variable not found: %s", envVar))
	}
	return value
}

func initProvider(providerType pt.Type) pc.SerializedConfig {
	switch providerType {
	case pt.RedisOnline:
		port := checkEnv("REDIS_INSECURE_PORT")

		redisConfig := pc.RedisConfig{
			Addr: fmt.Sprintf("%s:%s", "0.0.0.0", port),
		}
		return redisConfig.Serialized()
	case pt.PostgresOffline:
		db := checkEnv("POSTGRES_DB")
		user := checkEnv("POSTGRES_USER")
		password := checkEnv("POSTGRES_PASSWORD")

		postgresConfig := pc.PostgresConfig{
			Host:     "0.0.0.0",
			Port:     "5432",
			Database: db,
			Username: user,
			Password: password,
			SSLMode:  "disable",
		}
		return postgresConfig.Serialize()
	default:
		panic(fmt.Sprintf("Unsupported provider type: %s", providerType))
	}

}

func TestHealth_Check(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	err := godotenv.Load("../.env")
	if err != nil {
		t.Fatal(err)
	}

	os.Setenv("TZ", "UTC")

	tests := []testMember{}

	if *providerType == "redis" || *providerType == "" {
		config := initProvider(pt.RedisOnline)
		tests = append(tests, testMember{
			providerDef: metadata.ProviderDef{
				Name:             "redis",
				Type:             string(pt.RedisOnline),
				SerializedConfig: config,
				Software:         "redis",
				Tags:             metadata.Tags{},
				Properties:       metadata.Properties{},
			},
		})
	}

	if *providerType == "postgres" || *providerType == "" {
		config := initProvider(pt.PostgresOffline)
		tests = append(tests, testMember{
			providerDef: metadata.ProviderDef{
				Name:             "postgres",
				Type:             string(pt.PostgresOffline),
				SerializedConfig: config,
				Software:         "postgres",
				Tags:             metadata.Tags{},
				Properties:       metadata.Properties{},
			},
		})
	}

	server, addr := initMetadataServer(t)

	fmt.Printf("Metadata server listening on %s\n", addr)

	client := initClient(t, addr)

	health := NewHealth(client)

	for _, test := range tests {
		if err := client.Create(context.Background(), test.providerDef); err != nil {
			t.Fatalf("Failed to create provider: %s", err)
		}
		t.Run(string(test.providerDef.Type), func(t *testing.T) {
			isHealthy, err := health.Check(test.providerDef.Name)
			if err != nil {
				t.Fatalf("Failed to check provider health: %s", err)
			}
			if !isHealthy {
				t.Fatalf("Provider is not healthy")
			}
		})
	}

	if err := server.Stop(); err != nil {
		t.Fatalf("Failed to stop metadata server: %s", err)
	}
}

func initMetadataServer(t *testing.T) (*metadata.MetadataServer, string) {
	logger := zaptest.NewLogger(t)
	config := &metadata.Config{
		Logger:          logger.Sugar(),
		StorageProvider: metadata.LocalStorageProvider{},
	}
	server, err := metadata.NewMetadataServer(config)
	if err != nil {
		panic(err)
	}
	// listen on a random port
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	go func() {
		if err := server.ServeOnListener(lis); err != nil {
			panic(err)
		}
	}()
	return server, lis.Addr().String()
}

func initClient(t *testing.T, addr string) *metadata.Client {
	logger := zaptest.NewLogger(t).Sugar()
	client, err := metadata.NewClient(addr, logger)
	if err != nil {
		t.Fatalf("Failed to create client: %s", err)
	}
	return client
}
