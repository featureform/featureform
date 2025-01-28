// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package bootstrap

import (
	"context"
	"fmt"
	"sync"

	"github.com/featureform/config"
	"github.com/featureform/fferr"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"
	"github.com/featureform/scheduling"
)

type Initializer struct {
	config *config.FeatureformApp
	pg     *postgres.Pool
	pgErr  error
	initPg sync.Once
	tm     scheduling.TaskMetadataManager
	tmErr  error
	initTm sync.Once
}

// TODO(simba) I want to make the loger to be initialized by this object later
func NewInitializer(cfg *config.FeatureformApp) (*Initializer, error) {
	if cfg == nil {
		return nil, fferr.NewInternalErrorf("Config not set")
	}
	return &Initializer{
		config: cfg,
	}, nil
}

// GetOrCreatePostgresPool will get a postgres pool as defined by config
func (i *Initializer) GetOrCreatePostgresPool(ctx context.Context) (*postgres.Pool, error) {
	logger := logging.GetLoggerFromContext(ctx)
	i.initPg.Do(func() {
		logger.Info("Initializing postgres from app config")
		if i.config.Postgres == nil {
			i.pgErr = fferr.NewInvalidConfigf("PSQL config not set but state provider is psql")
			logger.Errorw("Failed to created postgres pool", "err", i.pgErr)
			return
		}
		i.pg, i.pgErr = postgres.NewPool(ctx, *i.config.Postgres)
		if i.pgErr != nil {
			logger.Errorw("Failed to initialize postgres from app config", "err", i.pgErr)
			return
		} else {
			logger.Info("Successfully initialized postgres from app config")
			return
		}
	})
	return i.pg, i.pgErr
}

// GetOrCreateTaskMetadataManager creates a TaskMetadataManager if one doesn't exist. It'll
// re-use or initialize state providers like Postgres based on the config.
func (i *Initializer) GetOrCreateTaskMetadataManager(ctx context.Context) (scheduling.TaskMetadataManager, error) {
	logger := logging.GetLoggerFromContext(ctx)
	i.initTm.Do(func() {
		logger.Debug("Initializing TaskManager from app config")
		switch i.config.StateProviderType {
		case config.NoStateProvider:
			logger.Debug("Initializing memory task manager")
			i.tm, i.tmErr = scheduling.NewMemoryTaskMetadataManager(ctx)
		case config.PostgresStateProvider:
			logger.Debug("Initializing postgres task manager")
			pool, poolErr := i.GetOrCreatePostgresPool(ctx)
			if poolErr != nil {
				logger.Errorw("Failed to get Postgres connection pool", "err", poolErr)
				i.tmErr = poolErr
				return
			}
			logger.Debug("Got postgres connection pool")
			i.tm, i.tmErr = scheduling.NewPSQLTaskMetadataManager(ctx, pool)
		case config.EtcdStateProvider:
			logger.Debug("Initializing etcd task manager")
			if i.config.Etcd == nil {
				i.tmErr = fferr.NewInvalidConfigf("ETCD config not set but state provider is etcd")
				logger.Errorw("Failed to created etcd task manager", "err", i.tmErr)
				return
			}
			i.tm, i.tmErr = scheduling.NewETCDTaskMetadataManager(ctx, *i.config.Etcd)
		default:
			errMsg := fmt.Sprintf(
				"Unable to initialize task manager, unknown state provider type: %s",
				i.config.StateProviderType,
			)
			logger.Errorw(errMsg)
			i.tmErr = fferr.NewInternalErrorf(errMsg)
		}
	})
	if i.tmErr != nil {
		logger.Errorw("TaskManager failed to be created", "err", i.tmErr)
		return i.tm, i.tmErr
	}
	logger.Debug("Returning task manager")
	return i.tm, i.tmErr
}

func (i *Initializer) Close() error {
	// This isn't super thread safe, but given its just attempting to
	// close things, it's acceptable
	if i.pg != nil {
		i.pg.Close()
	}
	return nil
}
