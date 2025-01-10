package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/featureform/fferr"
	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata/common"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type ResourcesRepositoryType int

const (
	ResourcesRepositoryTypeUnknown ResourcesRepositoryType = iota
	ResourcesRepositoryTypeMemory
	ResourcesRepositoryTypePsql
)

func (r ResourcesRepositoryType) String() string {
	switch r {
	case ResourcesRepositoryTypeMemory:
		return "memory"
	case ResourcesRepositoryTypePsql:
		return "psql"
	default:
		return "unknown"
	}
}

const (
	sqlCountDependencies = `-- name: CountDependencies :one
		SELECT COUNT(*)
		FROM edges
		WHERE from_resource_type = $1::integer
			AND from_resource_name = $2::text
			AND from_resource_variant = $3::text;`

	sqlMarkAsDeleted = `-- name: MarkAsDeleted :exec
		UPDATE ff_task_metadata
		SET delete = NOW()
		WHERE key = $1
		RETURNING key;`

	sqlDeleteFromEdges = `-- name: DeleteFromEdges :exec
		DELETE FROM edges
		WHERE to_resource_type = $1::integer
			AND to_resource_name = $2::text
			AND to_resource_variant = $3::text;`
)

type ResourcesRepository interface {
	Type() ResourcesRepositoryType
	MarkForDeletion(ctx context.Context, resourceID common.ResourceID) error
}

type SqlRepositoryConfig struct {
	MaxRetryDuration   time.Duration
	InitialBackoff     time.Duration
	TransactionTimeout time.Duration
}

func DefaultResourcesRepoConfig() SqlRepositoryConfig {
	return SqlRepositoryConfig{
		MaxRetryDuration:   10 * time.Second,
		InitialBackoff:     100 * time.Millisecond,
		TransactionTimeout: 30 * time.Second,
	}
}

func NewResourcesRepositoryFromEnv(managerType string) (ResourcesRepository, error) {
	return NewResourcesRepositoryFromEnvWithConfig(managerType, DefaultResourcesRepoConfig())
}

func NewResourcesRepositoryFromEnvWithConfig(managerType string, config SqlRepositoryConfig) (ResourcesRepository, error) {
	switch managerType {
	case "memory":
		return NewInMemoryResourcesRepository(), nil
	case "psql":
		metadataPsqlConfig := help.NewMetadataPSQLConfigFromEnv()
		connection, err := help.NewPSQLPoolConnection(metadataPsqlConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create PSQL connection: %w", err)
		}
		return NewSqlResourcesRepository(connection, logging.NewLogger("metadata"), config), nil
	default:
		return nil, fferr.NewInvalidArgumentErrorf("Metadata Manager must be one of type: memory, psql")
	}
}

type sqlResourcesRepository struct {
	db     *pgxpool.Pool
	logger logging.Logger
	config SqlRepositoryConfig
}

func NewSqlResourcesRepository(db *pgxpool.Pool, logger logging.Logger, config SqlRepositoryConfig) ResourcesRepository {
	return &sqlResourcesRepository{
		db:     db,
		logger: logger,
		config: config,
	}
}

// transactionError to combine transaction error and rollback error
type transactionError struct {
	txErr       error
	rollbackErr error
}

func (e *transactionError) Error() string {
	if e.rollbackErr != nil {
		return fmt.Sprintf("transaction error: %v, rollback error: %v", e.txErr, e.rollbackErr)
	}
	return fmt.Sprintf("transaction error: %v", e.txErr)
}

func (r *sqlResourcesRepository) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, r.config.TransactionTimeout)
	defer cancel()

	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}

	txErr := fn(tx)
	if txErr != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return &transactionError{
				txErr:       txErr,
				rollbackErr: rbErr,
			}
		}
		return txErr
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// an Error that happens at serializable iso level so that we can retry
func isSerializationError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40001"
}

func (r *sqlResourcesRepository) Type() ResourcesRepositoryType {
	return ResourcesRepositoryTypePsql
}

func (r *sqlResourcesRepository) MarkForDeletion(ctx context.Context, resourceID common.ResourceID) error {
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = r.config.MaxRetryDuration
	b.InitialInterval = r.config.InitialBackoff

	operation := func() error {
		return r.withTx(ctx, func(tx pgx.Tx) error {
			if err := r.checkDependencies(ctx, tx, resourceID); err != nil {
				return err
			}

			if err := r.markDeleted(ctx, tx, resourceID); err != nil {
				return err
			}

			return r.deleteEdges(ctx, tx, resourceID)
		})
	}

	return backoff.RetryNotify(
		operation,
		backoff.WithContext(b, ctx),
		func(err error, duration time.Duration) {
			if isSerializationError(err) {
				r.logger.Debugw("retrying after serialization failure",
					"error", err,
					"backoff_duration", duration,
					"resource_id", resourceID)
			}
		},
	)
}

func (r *sqlResourcesRepository) checkDependencies(ctx context.Context, tx pgx.Tx, resourceID common.ResourceID) error {
	var dependencyCount int
	if err := tx.QueryRow(ctx, sqlCountDependencies,
		resourceID.Type, resourceID.Name, resourceID.Variant).Scan(&dependencyCount); err != nil {
		return fmt.Errorf("count dependencies: %w", err)
	}

	if dependencyCount > 0 {
		return backoff.Permanent(fferr.NewInternalErrorf(
			"cannot delete resource %s %s (%s) because it has %d dependencies",
			resourceID.Type, resourceID.Name, resourceID.Variant, dependencyCount,
		))
	}
	return nil
}

func (r *sqlResourcesRepository) markDeleted(ctx context.Context, tx pgx.Tx, resourceID common.ResourceID) error {
	var deletedKey string
	if err := tx.QueryRow(ctx, sqlMarkAsDeleted, resourceID.ToKey()).Scan(&deletedKey); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return backoff.Permanent(fferr.NewInternalErrorf("resource with key %s does not exist in ff_task_metadata", resourceID.ToKey()))
		}
		return fmt.Errorf("mark as deleted: %w", err)
	}
	return nil
}

func (r *sqlResourcesRepository) deleteEdges(ctx context.Context, tx pgx.Tx, resourceID common.ResourceID) error {
	if _, err := tx.Exec(ctx, sqlDeleteFromEdges,
		resourceID.Type, resourceID.Name, resourceID.Variant); err != nil {
		return fmt.Errorf("delete edges: %w", err)
	}
	return nil
}

type inMemoryResourcesRepository struct{}

func NewInMemoryResourcesRepository() ResourcesRepository {
	return &inMemoryResourcesRepository{}
}

func (r *inMemoryResourcesRepository) MarkForDeletion(ctx context.Context, resourceID common.ResourceID) error {
	return nil
}

func (r *inMemoryResourcesRepository) Type() ResourcesRepositoryType {
	return ResourcesRepositoryTypeMemory
}
