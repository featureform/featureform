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
	"testing"

	"github.com/featureform/config"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/helpers/tests"
	"github.com/featureform/logging"
)

func insertIntoSearchResources(ctx context.Context, pool *postgres.Pool) error {
	resources := []ResourceDoc{
		{
			Name:    "hero-typesense-film",
			Variant: "default_variant",
			Type:    "string-normal",
			Tags:    []string{},
		}, {
			Name:    "wine_sonoma_county",
			Variant: "second_variant",
			Type:    "general",
			Tags:    []string{},
		}, {
			Name:    "hero",
			Variant: "default-shirt",
			Type:    "string",
			Tags:    []string{},
		},
		{
			Name:    "test_name",
			Variant: "test_variant",
			Type:    "test_type",
			Tags:    []string{"tag1", "tag2"},
		},
	}
	for _, res := range resources {
		_, err := pool.Exec(ctx, `
        INSERT INTO search_resources (id, name, variant, type, tags)
        VALUES ($1, $2, $3, $4, $5)`,
			fmt.Sprintf("%s__%s__%s", res.Type, res.Name, res.Variant), // Construct ID
			res.Name,
			res.Variant,
			res.Type,
			res.Tags,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func TestPSQLSearch(t *testing.T) {
	ctx := logging.NewTestContext(t)
	testDbName, dbCleanup := tests.CreateTestDatabase(ctx, t, config.GetMigrationPath())
	defer dbCleanup()
	postgresParams := tests.GetTestPostgresParams(testDbName)

	var err error
	pool, err := postgres.NewPool(ctx, postgresParams)
	if err != nil {
		t.Fatalf("Failed to initialize Postgres Pool: %v", err)
	}
	defer pool.Close()
	err = insertIntoSearchResources(ctx, pool)
	if err != nil {
		t.Fatalf("Failed to Insert into Search Resources: %s", err)
	}
	searcher, err := NewPostgres(ctx, pool)
	if err != nil {
		t.Fatalf("Failed to Initialize Postgres Search: %s", err)
	}
	psqlTest := PSQLSearchTest{
		ctx:      ctx,
		t:        t,
		searcher: searcher,
	}
	psqlTest.Run()
}

type PSQLSearchTest struct {
	ctx      context.Context
	t        *testing.T
	searcher Searcher
}

func (test *PSQLSearchTest) Run() {
	t := test.t
	searcher := test.searcher
	ctx := test.ctx
	testFns := map[string]func(*testing.T, context.Context, Searcher){
		"TestPostgresFullSearch": testPostgresFullSearch,
		"TestPostgresCharacters": testPostgresCharacters,
	}

	for name, fn := range testFns {
		t.Run(name, func(t *testing.T) {
			fn(t, ctx, searcher)
		})
	}

	if err := searcher.DeleteAll(ctx); err != nil {
		t.Fatalf("Failed to Delete: %s", err)
	}
}

func testPostgresFullSearch(t *testing.T, ctx context.Context, searcher Searcher) {

	// Test searching by name
	results, err := searcher.RunSearch(ctx, "test_name")
	if err != nil {
		t.Fatalf("Failed to search by name: %s", err)
	}
	if len(results) != 1 || results[0].Name != "test_name" {
		t.Fatal("Failed to find document by name")
	}

	// Test searching by tag
	results, err = searcher.RunSearch(ctx, "tag1")
	if err != nil {
		t.Fatalf("Failed to search by tag: %s", err)
	}
	if len(results) != 1 || results[0].Name != "test_name" {
		t.Fatal("Failed to find document by tag")
	}
}

func testPostgresCharacters(t *testing.T, ctx context.Context, searcher Searcher) {
	// Test partial word matching
	results, err := searcher.RunSearch(ctx, "film")
	if err != nil {
		t.Fatalf("Failed to search: %s", err)
	}
	if len(results) != 1 || results[0].Name != "hero-typesense-film" {
		t.Fatal("Failed to find document with partial word match")
	}

	// Test multiple word matching
	results, err = searcher.RunSearch(ctx, "sonoma county")
	if err != nil {
		t.Fatalf("Failed to search: %s", err)
	}
	if len(results) != 1 || results[0].Name != "wine_sonoma_county" {
		t.Fatal("Failed to find document with multiple word match")
	}
}
