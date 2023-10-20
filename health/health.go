package health

import (
	"context"
	"errors"
	"fmt"

	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
)

type Health struct {
	metadata *metadata.Client
}

func NewHealth(client *metadata.Client) *Health {
	return &Health{
		metadata: client,
	}
}

func (h *Health) CheckProvider(name string) (bool, error) {
	rec, err := h.metadata.GetProvider(context.Background(), name)
	if err != nil {
		return false, err
	}
	p, err := provider.Get(pt.Type(rec.Type()), pc.SerializedConfig(rec.SerializedConfig()))
	if err != nil {
		return false, errors.New(h.handleError(err))
	}
	isHealthy, err := p.CheckHealth()
	if err != nil {
		return false, errors.New(h.handleError(err))
	}
	return isHealthy, nil
}

func (h *Health) IsSupportedProvider(t pt.Type) bool {
	switch t {
	case pt.RedisOnline, pt.DynamoDBOnline, pt.PostgresOffline, pt.SnowflakeOffline, pt.SparkOffline:
		return true
	default:
		return false
	}
}

func (h *Health) handleError(err error) string {
	switch err.(type) {
	case provider.FileStoreError:
		return fmt.Sprintf("Featureform cannot connect to the file store or doesn't have the correct permissions to read/write due to the following error: %s", err.Error())
	case provider.SparkExecutorError:
		return fmt.Sprintf("Featureform cannot connect to the Spark executor or the Spark cluster cannot connect to the files store due to the following error: %s", err.Error())
	default:
		return err.Error()
	}
}
