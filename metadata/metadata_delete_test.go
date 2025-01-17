package metadata

import (
	"context"
	"errors"
	"github.com/featureform/fferr"
	"net"
	"testing"

	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	pb "github.com/featureform/metadata/proto"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/scheduling"
	"github.com/stretchr/testify/assert"
)

func startServPsql(t *testing.T) (*MetadataServer, string) {
	metadataPsqlConfig := help.NewMetadataPSQLConfigFromEnv()
	manager, err := scheduling.NewPSQLTaskMetadataManager(metadataPsqlConfig)
	if err != nil {
		t.Fatalf(err.Error())
	}
	logger := logging.NewTestLogger(t)

	config := &Config{
		Logger:      logger,
		TaskManager: manager,
	}
	serv, err := NewMetadataServer(config)
	if err != nil {
		panic(err)
	}
	// listen on a random port
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	go func() {
		if err := serv.ServeOnListener(lis); err != nil {
			panic(err)
		}
	}()
	return serv, lis.Addr().String()
}

func TestMetadataDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	redisConfig := pc.RedisConfig{
		Addr:     "0.0.0.0",
		Password: "root",
		DB:       0,
	}
	snowflakeConfig := pc.SnowflakeConfig{
		Username:     "featureformer",
		Password:     "password",
		Organization: "featureform",
		Account:      "featureform-test",
		Database:     "transactions_db",
		Schema:       "fraud",
		Warehouse:    "ff_wh_xs",
		Role:         "sysadmin",
	}
	resources := []ResourceDef{
		UserDef{
			Name:       "Featureform",
			Tags:       Tags{},
			Properties: Properties{},
		},
		UserDef{
			Name:       "Other",
			Tags:       Tags{},
			Properties: Properties{},
		},
		ProviderDef{
			Name:             "mockOnline",
			Description:      "A mock online provider",
			Type:             string(pt.RedisOnline),
			Software:         "redis",
			Team:             "fraud",
			SerializedConfig: redisConfig.Serialized(),
			Tags:             Tags{},
			Properties:       Properties{},
		},
		ProviderDef{
			Name:             "mockOffline",
			Description:      "A mock offline provider",
			Type:             string(pt.SnowflakeOffline),
			Software:         "snowflake",
			Team:             "recommendations",
			SerializedConfig: snowflakeConfig.Serialize(),
			Tags:             Tags{},
			Properties:       Properties{},
		},
	}

	serv, addr := startServPsql(t)
	cli := client(t, addr)
	err := cli.CreateAll(context.Background(), resources)
	assert.NoError(t, err)

	// set statuses to ready
	_, err = serv.SetResourceStatus(context.Background(), &pb.SetStatusRequest{
		ResourceId: &pb.ResourceID{
			Resource: &pb.NameVariant{
				Name: "mockOnline",
			},
			ResourceType: pb.ResourceType_PROVIDER,
		},
		Status: &pb.ResourceStatus{
			Status: pb.ResourceStatus_READY,
		},
	})

	t.Run("delete provider with no dependencies", func(t *testing.T) {
		// try to delete the online provider
		_, err := serv.MarkForDeletion(context.Background(), &pb.MarkForDeletionRequest{
			ResourceId: &pb.ResourceID{
				Resource: &pb.NameVariant{
					Name: "mockOnline",
				},
				ResourceType: pb.ResourceType_PROVIDER,
			},
		})
		assert.NoError(t, err)

		res, err := serv.lookup.Lookup(context.Background(), ResourceID{
			Name: "mockOnline",
			Type: PROVIDER,
		})

		assert.Nil(t, res)
		var knfErr *fferr.KeyNotFoundError
		assert.True(t, errors.As(err, &knfErr))
	})
}
