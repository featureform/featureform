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
	case provider.ConnectionError:
		return fmt.Sprintf("Featureform cannot connect to the provider during health check: %s", err.Error())
	case provider.RuntimeError:
		return fmt.Sprintf("Featureform encountered a runtime error during health check: %s", err.Error())
	case provider.InternalError:
		return fmt.Sprintf("Featureform encountered an internal error during health check: %s", err.Error())
	default:
		return err.Error()
	}
}
