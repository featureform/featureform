package health

import (
	"context"
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

func (h *Health) Check(providerName string) (bool, error) {
	rec, err := h.metadata.GetProvider(context.Background(), providerName)
	if err != nil {
		return false, err
	}
	if !h.isSupportedProvider(pt.Type(rec.Type())) {
		return false, fmt.Errorf("unsupported provider type: %s", rec.Type())
	}
	p, err := provider.Get(pt.Type(rec.Type()), pc.SerializedConfig(rec.SerializedConfig()))
	if err != nil {
		return false, err
	}
	return p.Check()
}

func (h *Health) isSupportedProvider(t pt.Type) bool {
	switch t {
	case pt.RedisOnline, pt.DynamoDBOnline, pt.PostgresOffline, pt.SnowflakeOffline, pt.SparkOffline:
		return true
	default:
		return false
	}
}
