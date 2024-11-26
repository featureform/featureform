// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package health

import (
	"context"

	"github.com/featureform/fferr"
	"github.com/featureform/metadata"
	"github.com/featureform/provider"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/secrets"
	"golang.org/x/exp/slices"
)

var supportedProviders = []pt.Type{
	pt.RedisOnline,
	pt.DynamoDBOnline,
	pt.PostgresOffline,
	pt.SnowflakeOffline,
	pt.ClickHouseOffline,
	pt.SparkOffline,
	pt.RedshiftOffline,
}

type Health struct {
	metadata *metadata.Client
}

func NewHealth(client *metadata.Client) *Health {
	return &Health{
		metadata: client,
	}
}

func (h *Health) CheckProvider(name string, secretsManager secrets.Manager) (bool, error) {
	rec, err := h.metadata.GetProvider(context.Background(), name)
	if err != nil {
		return false, err
	}

	p, err := provider.GetWithSecretsManager(pt.Type(rec.Type()), rec.SerializedConfig(), secretsManager)
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
	return slices.Contains(supportedProviders, t)
}

func (h *Health) handleError(err error) {
	switch errType := err.(type) {
	case *fferr.ConnectionError:
		errType.SetMessage("Featureform could not connect to the provider during health check")
	case *fferr.ExecutionError:
		errType.SetMessage("Featureform encountered a runtime error during health check")
	case *fferr.InternalError:
		errType.SetMessage("Featureform encountered an internal error during health check")
	}
}
