package db

import (
	"context"
	"database/sql"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"
)

func RunMigrations(ctx context.Context, pgConfig *postgres.Config, migrationPath string) error {
	logger := logging.GetLoggerFromContext(ctx)

	if migrationPath == "" {
		logger.Info("No migration path provided")
		return nil
	}

	if pgConfig == nil {
		logger.Info("No postgres config provided")
		return nil
	}

	// creating a connection using sqlDb connection as goose expects sql.DB, no easy way to convert pgxpool to sql.DB
	db, err := sql.Open("pgx", pgConfig.ConnectionString())
	if err != nil {
		logger.Errorw("error opening database", "err", err)
		return fferr.NewInternalErrorf("error opening database: %v", err)
	}
	defer db.Close()

	logger.Debugw("pinging database before running migrations")
	if err := db.PingContext(ctx); err != nil {
		logger.Errorw("error pinging database", "err", err)
		return fferr.NewInternalErrorf("error pinging database: %v", err)
	}

	if err := goose.SetDialect(string(goose.DialectPostgres)); err != nil {
		logger.Errorw("error setting dialect", "err", err)
		return fferr.NewInternalErrorf("error setting dialect: %v", err)
	}

	logger.Infow("starting migrations", "directory", migrationPath)
	if err := goose.UpContext(ctx, db, migrationPath); err != nil {
		logger.Errorw("error running migrations", "err", err)
		return fferr.NewInternalErrorf("error running migrations: %v", err)
	}
	logger.Infow("migrations completed successfully")

	return nil
}
