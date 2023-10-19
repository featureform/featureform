package health

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
)

const baseInitializationErrorMessage = "Featureform failed to connect to provider with the supplied credentials due to the following error:"

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
		return false, errors.New(h.handleError(err))
	}
	isHealthy, err := p.Check()
	if err != nil {
		return false, errors.New(h.handleError(err))
	}
	return isHealthy, nil
}

func (h *Health) isSupportedProvider(t pt.Type) bool {
	switch t {
	case pt.RedisOnline, pt.DynamoDBOnline, pt.PostgresOffline, pt.SnowflakeOffline, pt.SparkOffline:
		return true
	default:
		return false
	}
}

func (h *Health) handleError(err error) string {
	// GENERAL ERRORS
	if strings.Contains(err.Error(), "no such host") {
		return fmt.Sprintf("%s '%s' Check that the host name is correct and the server is running.", baseInitializationErrorMessage, err.Error())
	}
	if strings.Contains(err.Error(), "connection refused") {
		return fmt.Sprintf("%s '%s' Check that the server is running and/or the port is correct.", baseInitializationErrorMessage, err.Error())
	}
	if strings.Contains(err.Error(), "operation timed out") {
		return fmt.Sprintf("%s '%s' Check that the server is running and able to accept requests.", baseInitializationErrorMessage, err.Error())
	}
	// REDIS ERRORS
	if strings.Contains(err.Error(), "WRONGPASS") {
		return fmt.Sprintf("%s '%s' Check that the password is correct.", baseInitializationErrorMessage, err.Error())
	}
	if strings.Contains(err.Error(), "NOAUTH") {
		return fmt.Sprintf("%s '%s' The server has been configured with the 'requirepass' directive, which requires a password for authentication.", baseInitializationErrorMessage, "NOAUTH HELLO")
	}
	// POSTGRES ERRORS
	if match, matchErr := regexp.MatchString("pq: database .* does not exist", err.Error()); match && matchErr == nil {
		return fmt.Sprintf("%s '%s' Check that the database name is correct.", baseInitializationErrorMessage, err.Error())
	}
	if match, matchErr := regexp.MatchString("pq: password authentication failed for user .*", err.Error()); match && matchErr == nil {
		return fmt.Sprintf("%s '%s' Check that the password and username combination is correct.", baseInitializationErrorMessage, err.Error())
	}
	return err.Error()
}
