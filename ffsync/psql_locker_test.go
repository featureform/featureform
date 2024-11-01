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

	"github.com/featureform/helpers"
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

	config := helpers.PSQLConfig{
		Host:     host,
		Port:     port,
		User:     username,
		Password: password,
		DBName:   dbName,
		SSLMode:  sslMode,
	}

	locker, err := NewPSQLLocker(config)
	if err != nil {
		t.Fatalf("Failed to create PSQL locker: %v", err)
	}

	// clean up
	defer func() {
		rLocker := locker.(*psqlLocker)

		_, err := rLocker.db.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", rLocker.tableName))
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
	test.Run()
}
