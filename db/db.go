package db

import (
	"context"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"
)

func RunMigrations(ctx context.Context, pgPool *postgres.Pool, migrationPath string) error {
	logger := logging.GetLoggerFromContext(ctx)

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		logger.Debugw("no deadline set on context")
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}

	if migrationPath == "" {
		logger.Info("No migration path provided")
		return nil
	}

	if pgPool == nil {
		logger.Info("No postgres config provided")
		return nil
	}

	// Convert pgxpool.Pool to *sql.DB for goose
	sqlDb, err := pgPool.ToSqlDB()
	if err != nil {
		logger.Errorw("error converting pgxpool.Pool to *sql.DB", "err", err)
		return fferr.NewInternalErrorf("error converting pgxpool.Pool to *sql.DB: %v", err)
	}
	defer sqlDb.Close()

	logger.Debugw("pinging database before running migrations")
	if err := sqlDb.PingContext(ctx); err != nil {
		logger.Errorw("error pinging database", "err", err)
		return fferr.NewInternalErrorf("error pinging database: %v", err)
	}

	if err := goose.SetDialect(string(goose.DialectPostgres)); err != nil {
		logger.Errorw("error setting dialect", "err", err)
		return fferr.NewInternalErrorf("error setting dialect: %v", err)
	}

	logger.Infow("starting migrations", "directory", migrationPath)
	if err := goose.UpContext(ctx, sqlDb, migrationPath); err != nil {
		logger.Errorw("error running migrations", "err", err)
		return fferr.NewInternalErrorf("error running migrations: %v", err)
	}
	logger.Infow("migrations completed successfully")

	return nil
}
