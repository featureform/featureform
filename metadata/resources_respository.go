package metadata

import (
	"context"
	"errors"
	"fmt"
	"github.com/avast/retry-go/v4"
	"github.com/featureform/scheduling"
	"github.com/featureform/storage"
	"golang.org/x/exp/slices"
	"math"
	"time"

	"github.com/featureform/fferr"
	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	"github.com/featureform/metadata/common"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type ResourcesRepositoryType string

const (
	ResourcesRepositoryTypeMemory ResourcesRepositoryType = "memory"
	ResourcesRepositoryTypePsql   ResourcesRepositoryType = "psql"
)

const (
	sqlCountDirectDependencies = `-- name: CountDirectDependencies :one
		SELECT COUNT(*)
		FROM edges
		WHERE from_resource_type = $1::integer
			AND from_resource_name = $2::text
			AND from_resource_variant = $3::text;`

	sqlMarkAsDeleted = `-- name: MarkAsDeleted :exec
		UPDATE ff_task_metadata
		SET marked_for_deletion_at = NOW()
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
	ResourceLookup
}

type SqlRepositoryConfig struct {
	MaxDelay           time.Duration
	InitialBackoff     time.Duration
	TransactionTimeout time.Duration
	Attempts           uint
}

func DefaultResourcesRepoConfig() SqlRepositoryConfig {
	return SqlRepositoryConfig{
		MaxDelay:           5 * time.Second,       // Total time we're willing to retry
		InitialBackoff:     50 * time.Millisecond, // Start with short delay
		TransactionTimeout: 10 * time.Second,      // Max time for a single transaction
		Attempts:           5,                     // Number of total attempts
	}
}

func NewResourcesRepositoryFromLookup(resourceLookup ResourceLookup) (ResourcesRepository, error) {
	if resourceLookup == nil {
		return nil, fferr.NewInternalErrorf("resource lookup is nil")
	}

	switch lookup := resourceLookup.(type) {
	case *EtcdResourceLookup, *LocalResourceLookup:
		return NewInMemoryResourcesRepository(lookup), nil

	case *MemoryResourceLookup:
		if lookup.Connection.Storage == nil {
			return nil, fferr.NewInternalErrorf("MemoryResourceLookup.Storage is nil")
		}
		switch lookup.Connection.Storage.Type() {
		case storage.MemoryMetadataStorage, storage.ETCDMetadataStorage:
			return NewInMemoryResourcesRepository(lookup), nil

		case storage.PSQLMetadataStorage:
			conn, err := help.NewPSQLPoolConnection(help.NewMetadataPSQLConfigFromEnv())
			if err != nil {
				return nil, fmt.Errorf("failed to create PSQL connection: %w", err)
			}
			return NewSqlResourcesRepository(conn, logging.NewLogger("metadata"), lookup, DefaultResourcesRepoConfig()), nil

		default:
			return nil, fmt.Errorf("unsupported storage type: %T", lookup.Connection.Storage.Type())
		}

	default:
		return nil, fmt.Errorf("unsupported resource lookup type: %T", resourceLookup)
	}
}

type sqlResourcesRepository struct {
	db     *pgxpool.Pool
	logger logging.Logger
	config SqlRepositoryConfig
	ResourceLookup
}

func NewSqlResourcesRepository(db *pgxpool.Pool, logger logging.Logger, lookup ResourceLookup, config SqlRepositoryConfig) ResourcesRepository {
	return &sqlResourcesRepository{
		db:             db,
		logger:         logger,
		config:         config,
		ResourceLookup: lookup,
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

func isRetryableError(err error) bool {
	var retryablePgErrors = []string{
		"40001", // Serialization failure (error while trying to run everything in a single transaction)
	}

	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && slices.Contains(retryablePgErrors, pgErr.Code)
}

func (r *sqlResourcesRepository) Type() ResourcesRepositoryType {
	return ResourcesRepositoryTypePsql
}

func (r *sqlResourcesRepository) MarkForDeletion(ctx context.Context, resourceID common.ResourceID) error {
	operation := func() error {
		return r.withTx(ctx, func(tx pgx.Tx) error {
			if err := r.checkDependencies(ctx, tx, resourceID); err != nil {
				return err
			}

			// get status
			status, err := r.getStatus(ctx, tx, resourceID)
			if err != nil {
				return err
			}

			if status != scheduling.READY && status != scheduling.CREATED {
				return retry.Unrecoverable(fferr.NewInternalErrorf(
					"cannot delete resource %s %s (%s) because it is not in READY or CREATED status",
					resourceID.Type, resourceID.Name, resourceID.Variant,
				))
			}

			if err := r.markDeleted(ctx, tx, resourceID); err != nil {
				return err
			}

			return r.deleteEdges(ctx, tx, resourceID)
		})
	}

	return retry.Do(
		operation,
		retry.Attempts(r.config.Attempts),    // Number of attempts (5)
		retry.Delay(r.config.InitialBackoff), // Initial delay (50ms)
		retry.MaxDelay(r.config.MaxDelay),    // Max total time (5s)
		retry.DelayType(retry.BackOffDelay),  // Exponential backoff
		retry.RetryIf(isRetryableError),      // Only retry on specific errors
		retry.OnRetry(func(n uint, err error) {
			r.logger.Debugw("retrying after error",
				"attempt", n+1,
				"error", err,
				"backoff_duration", time.Duration(float64(r.config.InitialBackoff)*math.Pow(2, float64(n))),
				"resource_id", resourceID,
			)
		}),
		retry.Context(ctx),
	)
}

func (r *sqlResourcesRepository) checkDependencies(ctx context.Context, tx pgx.Tx, resourceID common.ResourceID) error {
	var dependencyCount int
	if err := tx.QueryRow(ctx, sqlCountDirectDependencies,
		resourceID.Type, resourceID.Name, resourceID.Variant).Scan(&dependencyCount); err != nil {
		return fmt.Errorf("count dependencies: %w", err)
	}

	if dependencyCount > 0 {
		return retry.Unrecoverable(fferr.NewInternalErrorf(
			"cannot delete resource %s %s (%s) because it has %d dependencies",
			resourceID.Type, resourceID.Name, resourceID.Variant, dependencyCount,
		))
	}
	return nil
}

func (r *sqlResourcesRepository) getStatus(ctx context.Context, tx pgx.Tx, resourceID common.ResourceID) (scheduling.Status, error) {
	const query = `
		-- Step 1: Extract the first taskId from the taskIdList
		WITH extracted_task_id AS (
			SELECT (jsonb_array_elements_text(((ff.value::jsonb ->> 'Message')::jsonb -> 'taskIdList'))::INTEGER) AS task_id
			FROM ff_task_metadata ff
			WHERE ff.key = $1
			LIMIT 1
		),

		-- Step 2: Get the task run data for the extracted taskId
		task_runs AS (
			SELECT tr.value::jsonb AS runs_data, et.task_id
			FROM extracted_task_id et
			JOIN ff_task_metadata tr
			  ON tr.key = '/tasks/runs/task_id=' || et.task_id
		),

		-- Step 3: Extract the latest runId based on dateCreated
		latest_run AS (
			SELECT run_obj->>'runID' AS run_id,
				   run_obj->>'dateCreated' AS date_created,
				   tr.task_id
			FROM task_runs tr,
				 jsonb_array_elements(tr.runs_data -> 'runs') AS run_obj
			ORDER BY run_obj->>'dateCreated' DESC
			LIMIT 1
		)

		-- Step 4: Retrieve the run metadata and parse the status
		SELECT (run_metadata.value::jsonb ->> 'status')::INTEGER AS status
		FROM latest_run lr
		JOIN ff_task_metadata run_metadata
		  ON run_metadata.key = '/tasks/runs/metadata/' ||
								to_char((lr.date_created)::timestamptz, 'YYYY/MM/DD/HH24/MI') ||
								'/task_id=' || lr.task_id ||
								'/run_id=' || lr.run_id
	`

	var status int

	// Execute the query
	err := tx.QueryRow(ctx, query, resourceID.ToKey()).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return scheduling.NO_STATUS, nil
		}
		return scheduling.NO_STATUS, fmt.Errorf("failed to get status for resource %s: %w", resourceID.ToKey(), err)
	}

	return scheduling.Status(status), nil
}

func (r *sqlResourcesRepository) markDeleted(ctx context.Context, tx pgx.Tx, resourceID common.ResourceID) error {
	var deletedKey string
	if err := tx.QueryRow(ctx, sqlMarkAsDeleted, resourceID.ToKey()).Scan(&deletedKey); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return retry.Unrecoverable(fferr.NewInternalErrorf(
				"resource with key %s does not exist in ff_task_metadata",
				resourceID.ToKey(),
			))
		}
		return fferr.NewInternalErrorf("mark as deleted %s: %v", resourceID.ToKey(), err)
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

type inMemoryResourcesRepository struct {
	ResourceLookup
}

func NewInMemoryResourcesRepository(lookup ResourceLookup) ResourcesRepository {
	return &inMemoryResourcesRepository{ResourceLookup: lookup}
}

func (r *inMemoryResourcesRepository) MarkForDeletion(ctx context.Context, resourceID common.ResourceID) error {
	return nil
}

func (r *inMemoryResourcesRepository) Type() ResourcesRepositoryType {
	return ResourcesRepositoryTypeMemory
}