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
	"go.uber.org/zap/zaptest"
)

func startServPsql(t *testing.T) (*MetadataServer, string) {
	metadataPsqlConfig := help.NewMetadataPSQLConfigFromEnv()
	manager, err := scheduling.NewPSQLTaskMetadataManager(metadataPsqlConfig)
	logger := zaptest.NewLogger(t)

	connection, err := help.NewPSQLPoolConnection(metadataPsqlConfig)
	assert.NoError(t, err)

	resourcesRepo := NewSqlResourcesRepository(connection, logging.NewTestLogger(t), DefaultResourcesRepoConfig())

	config := &Config{
		Logger:              logging.WrapZapLogger(logger.Sugar()),
		TaskManager:         manager,
		ResourcesRepository: resourcesRepo,
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
		}, ResourceLookupOpt{IncludeDeleted: false})

		assert.Nil(t, res)
		var knfErr *fferr.KeyNotFoundError
		assert.True(t, errors.As(err, &knfErr))
	})
}
