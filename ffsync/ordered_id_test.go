// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package ffsync

import (
	"context"
	"fmt"
	"testing"

	"github.com/featureform/config"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/helpers/tests"
	"github.com/featureform/logging"

	_ "github.com/lib/pq"
)

func TestUint64OrderedId(t *testing.T) {
	id1 := Uint64OrderedId(1)
	id2 := Uint64OrderedId(2)
	id3 := Uint64OrderedId(1)

	if !id1.Equals(id3) {
		t.Errorf("Expected id1 to equal id3")
	}

	if id1.Equals(id2) {
		t.Errorf("Expected id1 to not equal id2")
	}

	if !id1.Less(id2) {
		t.Errorf("Expected id1 to be less than id2")
	}

	if id2.Less(id1) {
		t.Errorf("Expected id2 to not be less than id1")
	}

	if id1.String() != "1" {
		t.Errorf("Expected id1 to be '1'")
	}

	if id2.String() != "2" {
		t.Errorf("Expected id2 to be '2'")
	}
}

func TestOrderedIdGenerator(t *testing.T) {
	testCases := []struct {
		name      string
		shortTest bool
		createGen func(t *testing.T, testDbName string) (OrderedIdGenerator, error)
		deferFunc func(generator OrderedIdGenerator, t *testing.T)
	}{
		{
			name:      "Memory",
			shortTest: true,
			createGen: createMemoryIdGenerator,
			deferFunc: func(generator OrderedIdGenerator, t *testing.T) {},
		},
		{
			name:      "Postgres",
			shortTest: false,
			createGen: createPSQLIdGenerator,
			deferFunc: func(generator OrderedIdGenerator, t *testing.T) {
				// Clean up the RDS table
				pg := generator.(*pgIdGenerator)
				_, err := pg.connPool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", pg.tableName))
				if err != nil {
					t.Errorf("failed to drop table %s: %v", pg.tableName, err)
				}
				pg.Close()
			},
		},
	}
	ctx := logging.NewTestContext(t)
	testDbName, dbCleanup := tests.CreateTestDatabase(ctx, t, config.GetMigrationPath())
	defer dbCleanup()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.shortTest && testing.Short() {
				t.Skip()
			}
			generator, err := tc.createGen(t, testDbName)
			if err != nil {
				t.Fatalf("failed to create %s ID generator: %v", tc.name, err)
			}
			defer tc.deferFunc(generator, t)

			prevId, err := generator.NextId(ctx, "testNamespace")
			if err != nil {
				t.Errorf("failed to get next id: %v", err)
			}

			diffNamespaceId, err := generator.NextId(ctx, "diffNamespace")
			if err != nil {
				t.Errorf("failed to get next id: %v", err)
			}

			if !prevId.Equals(diffNamespaceId) {
				t.Errorf("expected id: '%s' Received Id: '%s'", diffNamespaceId, prevId)
			}

			for i := 0; i < 10; i++ {
				id, err := generator.NextId(ctx, "testNamespace")
				if err != nil {
					t.Errorf("failed to get next id: %v", err)
				}

				if !prevId.Less(id) {
					t.Errorf("expected id '%s' to be greater than previous id '%s'", id, prevId)
				}
				prevId = id
			}
		})
	}
}

func createMemoryIdGenerator(t *testing.T, testDbName string) (OrderedIdGenerator, error) {
	return NewMemoryOrderedIdGenerator()
}

func createPSQLIdGenerator(t *testing.T, testDbName string) (OrderedIdGenerator, error) {
	cfg := tests.GetTestPostgresParams(testDbName)
	ctx := logging.NewTestContext(t)
	pool, err := postgres.NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("Failed to create postgres pool with config: %v . Err: %v", cfg, err)
	}
	return NewPSQLOrderedIdGenerator(ctx, pool)
}
