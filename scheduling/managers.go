// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package scheduling

import (
	"context"

	cfg "github.com/featureform/config"
	"github.com/featureform/ffsync"
	"github.com/featureform/helpers/notifications"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"
	ss "github.com/featureform/storage"
)

func NewMemoryTaskMetadataManager(ctx context.Context) (TaskMetadataManager, error) {
	logger := logging.GetLoggerFromContext(ctx)
	logger.Debug("Building in-memory task metadata manager")
	memoryLocker, err := ffsync.NewMemoryLocker()
	if err != nil {
		logger.Errorw("Failed to build memory locker", "err", err)
		return TaskMetadataManager{}, err
	}

	logger.Debug("Building in-memory storage impl")
	memoryStorage, err := ss.NewMemoryStorageImplementation()
	if err != nil {
		logger.Errorw("Failed to build memory storage impl", "err", err)
		return TaskMetadataManager{}, err
	}

	storage := ss.MetadataStorage{
		Locker:  &memoryLocker,
		Storage: &memoryStorage,
		Logger:  logger,
	}

	logger.Debug("Building in-memory ordered ID generator")
	generator, err := ffsync.NewMemoryOrderedIdGenerator()
	if err != nil {
		logger.Errorw("Failed to build memory orderedId generator", "err", err)
		return TaskMetadataManager{}, err
	}

	logger.Debug("Building slack notifier")
	slackChannel := cfg.GetSlackChannelId()
	slackNotif := notifications.NewSlackNotifier(slackChannel, logger)

	logger.Info("Successfully created in-memory TaskMetadataManager")
	return TaskMetadataManager{
		Storage:     storage,
		idGenerator: generator,
		notifier:    slackNotif,
	}, nil
}

func NewPSQLTaskMetadataManager(ctx context.Context, pool *postgres.Pool) (TaskMetadataManager, error) {
	logger := logging.GetLoggerFromContext(ctx)
	logger.Debug("Building PSQL task metadata manager")
	psqlLocker, err := ffsync.NewPSQLLocker(ctx, pool)
	if err != nil {
		logger.Errorw("Failed to initialize PSQL locker", "err", err)
		return TaskMetadataManager{}, err
	}

	logger.Debug("Building PSQL storage impl")
	psqlStorage, err := ss.NewPSQLStorageImplementation(ctx, pool, "ff_task_metadata")
	if err != nil {
		logger.Errorw("Failed to initialize PSQL storage", "err", err)
		return TaskMetadataManager{}, err
	}

	psqlMetadataStorage := ss.MetadataStorage{
		Locker:          psqlLocker,
		Storage:         psqlStorage,
		Logger:          logger,
		SkipListLocking: true,
	}

	logger.Debug("Building PSQL ordered ID generator")
	idGenerator, err := ffsync.NewPSQLOrderedIdGenerator(ctx, pool)
	if err != nil {
		logger.Errorw("Failed to initialize PSQL ordered-id-generator", "err", err)
		return TaskMetadataManager{}, err
	}

	logger.Debug("Building slack notifier")
	slackChannel := cfg.GetSlackChannelId()
	slackNotif := notifications.NewSlackNotifier(slackChannel, logger)

	logger.Info("TaskMetadataManager successfully created.")
	return TaskMetadataManager{
		Storage:     psqlMetadataStorage,
		idGenerator: idGenerator,
		notifier:    slackNotif,
	}, nil
}
