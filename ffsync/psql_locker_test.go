// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package ffsync

import (
	"fmt"
	"github.com/jonboulle/clockwork"
	"testing"

	"github.com/featureform/helpers"
	"github.com/featureform/helpers/postgres"
	"github.com/featureform/logging"
)

func TestPSQLLocker(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	var host, username, password, port, dbName, sslMode string

	if *useEnv {
		host = helpers.GetEnv("POSTGRES_HOST", "localhost")
		username = helpers.GetEnv("POSTGRES_USER", "postgres")
		password = helpers.GetEnv("POSTGRES_PASSWORD", "mysecretpassword")
		port = helpers.GetEnv("POSTGRES_PORT", "5432")
		dbName = helpers.GetEnv("POSTGRES_DB", "postgres")
		sslMode = helpers.GetEnv("POSTGRES_SSL_MODE", "disable")
	} else {
		host = "127.0.0.1"
		port = pgPort
		username = "postgres"
		password = "mysecretpassword"
		dbName = "postgres"
		sslMode = "disable"
	}

	cfg := postgres.Config{
		Host:     host,
		Port:     port,
		User:     username,
		Password: password,
		DBName:   dbName,
		SSLMode:  sslMode,
	}
	ctx := logging.NewTestContext(t)
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
