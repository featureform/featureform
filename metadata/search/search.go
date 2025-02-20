// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package search

import (
	"context"
	"fmt"

	"github.com/featureform/fferr"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"

	"github.com/lib/pq"
)

type Searcher interface {
	RunSearch(ctx context.Context, q string) ([]ResourceDoc, error)
	DeleteAll(context.Context) error
}

type NewPostgresFunc func(ctx context.Context, pool *postgres.Pool) (Searcher, error)

type PostgresSearch struct {
	pool *postgres.Pool
}

type ResourceDoc struct {
	Name    string
	Variant string
	Type    string
	Tags    []string
}

func NewPostgres(ctx context.Context, pool *postgres.Pool) (Searcher, error) {
	logger := logging.GetLoggerFromContext(ctx)
	logger.Debugw("Initializing Postgres search", "pool", pool)
	search := &PostgresSearch{pool: pool}
	return search, nil
}

func (s *PostgresSearch) RunSearch(ctx context.Context, q string) ([]ResourceDoc, error) {
	logger := logging.GetLoggerFromContext(ctx)
	logger.Infow("Searching for resources", "query", q)
	query := `
	SELECT name, type, variant, tags
	FROM search_resources
	WHERE search_vector @@ plainto_tsquery('english', $1)
	ORDER BY ts_rank(search_vector, plainto_tsquery('english', $1)) DESC
	`

	rows, err := s.pool.Query(ctx, query, q)
	if err != nil {
		logger.Errorw("failed to execute search", "err", err, "query", query)
		return nil, fferr.NewExecutionError("Postgres", fmt.Errorf("failed to execute search: %v", err))
	}
	defer rows.Close()

	var results []ResourceDoc
	for rows.Next() {
		var doc ResourceDoc
		if err := rows.Scan(&doc.Name, &doc.Type, &doc.Variant, pq.Array(&doc.Tags)); err != nil {
			logger.Errorw("failed to scan row", "err", err)
			return nil, fferr.NewInternalErrorf("failed to scan row: %v", err)
		}
		results = append(results, doc)
	}
	if err := rows.Err(); err != nil {
		logger.Errorw("failed to iterate over rows", "err", err)
		return nil, fferr.NewInternalErrorf("failed to iterate over rows: %v", err)
	}

	return results, nil
}

func (s *PostgresSearch) DeleteAll(ctx context.Context) error {
	logger := logging.GetLoggerFromContext(ctx)
	logger.Info("deleting all resources")
	_, err := s.pool.Exec(ctx, "TRUNCATE TABLE search_resources")
	if err != nil {
		logger.Errorw("failed to delete all tables", "err", err)
		return err
	}
	return nil
}

type SearchMock struct {
}

func (s SearchMock) DeleteAll(ctx context.Context) error {
	return nil
}

func (s SearchMock) RunSearch(ctx context.Context, q string) ([]ResourceDoc, error) {
	return nil, nil
}
