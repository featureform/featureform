package health

import (
	"context"

	"github.com/featureform/fferr"
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
		h.handleError(err)
		return false, err
	}
	isHealthy, err := p.CheckHealth()
	if err != nil {
		h.handleError(err)
		return false, err
	}
	return isHealthy, nil
}

func (h *Health) IsSupportedProvider(t pt.Type) bool {
	switch t {
	case pt.RedisOnline, pt.DynamoDBOnline, pt.PostgresOffline, pt.SnowflakeOffline, pt.ClickHouseOffline, pt.SparkOffline, pt.RedshiftOffline:
		return true
	default:
		return false
	}
}

func (h *Health) handleError(err error) {
	switch errType := err.(type) {
	case *fferr.ConnectionError:
		errType.SetMessage("Featureform cannot connect to the provider during health check")
	case *fferr.ExecutionError:
		errType.SetMessage("Featureform encountered a runtime error during health check")
	case *fferr.InternalError:
		errType.SetMessage("Featureform encountered an internal error during health check")
	}
}
