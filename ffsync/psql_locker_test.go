// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package ffsync

import (
	"fmt"
	"testing"

	"github.com/featureform/config"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/helpers/tests"
	"github.com/featureform/logging"
	"github.com/jonboulle/clockwork"
)

func TestPSQLLocker(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := logging.NewTestContext(t)
	testDbName, dbCleanup := tests.CreateTestDatabase(ctx, t, config.GetMigrationPath())
	defer dbCleanup()
	cfg := tests.GetTestPostgresParams(testDbName)

	pool, err := postgres.NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("Failed to create postgres pool with config: %v . Err: %v", cfg, err)
	}

	locker, err := NewPSQLLocker(ctx, pool)
	if err != nil {
		t.Fatalf("Failed to create PSQL locker: %v", err)
	}

	clock := clockwork.NewFakeClock()
	locker.(*psqlLocker).clock = clock

	// clean up
	defer func() {
		rLocker := locker.(*psqlLocker)

		_, err := rLocker.connPool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", rLocker.tableName))
		if err != nil {
			t.Fatalf("Failed to drop table: %v", err)
		}

		locker.Close()
	}()

	test := LockerTest{
		t:          t,
		locker:     locker,
		lockerType: "psql",
	}
	test.Run(clock)
}
