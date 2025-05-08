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
)

type Health struct {
	metadata *metadata.Client
}

func NewHealth(client *metadata.Client) *Health {
	return &Health{
		metadata: client,
	}
}

func (h *Health) CheckProvider(ctx context.Context, name string) (bool, error) {
	rec, err := h.metadata.GetProvider(ctx, name)
	if err != nil {
		return false, err
	}
	p, err := provider.Get(pt.Type(rec.Type()), rec.SerializedConfig())
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
	case
		pt.RedisOnline,
		pt.DynamoDBOnline,
		pt.PostgresOffline,
		pt.SnowflakeOffline,
		pt.ClickHouseOffline,
		pt.SparkOffline,
		pt.RedshiftOffline,
		pt.FirestoreOnline:
		return true
	default:
		return false
	}
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
