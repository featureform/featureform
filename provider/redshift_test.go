// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
)

func TestOfflineStoreRedshift(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}

	endpoint, ok := os.LookupEnv("REDSHIFT_HOST")
	if !ok {
		t.Fatalf("missing REDSHIFT_HOST variable")
	}
	port, ok := os.LookupEnv("REDSHIFT_PORT")
	if !ok {
		t.Fatalf("missing REDSHIFT_PORT variable")
	}
	username, ok := os.LookupEnv("REDSHIFT_USERNAME")
	if !ok {
		t.Fatalf("missing REDSHIFT_USERNAME variable")
	}
	password, ok := os.LookupEnv("REDSHIFT_PASSWORD")
	if !ok {
		t.Fatalf("missing REDSHIFT_PASSWORD variable")
	}

	redshiftDatabase := fmt.Sprintf("ff%s", strings.ToLower(uuid.NewString()))

	redshiftConfig := pc.RedshiftConfig{
		Host:     endpoint,
		Port:     port,
		Database: redshiftDatabase,
		Username: username,
		Password: password,
	}
	serialRSConfig := redshiftConfig.Serialize()
	if err := createRedshiftDatabase(redshiftConfig); err != nil {
		t.Fatalf("%v", err)
	}

	t.Cleanup(func() {
		err := destroyRedshiftDatabase(redshiftConfig)
		if err != nil {
			t.Logf("failed to cleanup database: %s\n", err)
		}
	})

	_, err = GetOfflineStore(pt.RedshiftOffline, serialRSConfig)
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	// TODO: (kamal) re-enable tests after refactor
	//test := OfflineStoreTest{
	//	t:     t,
	//	store: store,
	//}
	//test.Run()
	//test.RunSQL()
}

func createRedshiftDatabase(c pc.RedshiftConfig) error {
	url := fmt.Sprintf("sslmode=require user=%v password=%s host=%v port=%v dbname=%v", c.Username, c.Password, c.Host, c.Port, "dev")
	db, err := sql.Open("postgres", url)
	if err != nil {
		return err
	}
	databaseQuery := fmt.Sprintf("CREATE DATABASE %s", sanitize(c.Database))
	if _, err := db.Exec(databaseQuery); err != nil {
		return err
	}
	fmt.Printf("Created Redshift Database %s\n", c.Database)
	return nil
}

func destroyRedshiftDatabase(c pc.RedshiftConfig) error {
	url := fmt.Sprintf("sslmode=require user=%v password=%s host=%v port=%v dbname=%v", c.Username, c.Password, c.Host, c.Port, "dev")
	db, err := sql.Open("postgres", url)
	if err != nil {
		fmt.Errorf(err.Error())
		return err
	}
	disconnectQuery := fmt.Sprintf("SELECT pg_terminate_backend(pg_stat_activity.procpid) FROM pg_stat_activity WHERE datid=(SELECT oid from pg_database where datname = '%s');", c.Database)
	if _, err := db.Exec(disconnectQuery); err != nil {
		fmt.Errorf(err.Error())
		return err
	}
	var deleteErr error
	retries := 5
	databaseQuery := fmt.Sprintf("DROP DATABASE %s", sanitize(c.Database))
	for {
		if _, err := db.Exec(databaseQuery); err != nil {
			deleteErr = err
			time.Sleep(time.Second)
			retries--
			if retries == 0 {
				fmt.Errorf(err.Error())
				return deleteErr
			}
		} else {
			continue
		}
	}
}

func TestRedshiftCastTableItemType(t *testing.T) {
	q := redshiftSQLQueries{}

	// Prepare a fixed time value for testing.
	testTime := time.Date(2025, time.February, 13, 12, 0, 0, 0, time.UTC)

	// Table-driven test cases.
	testCases := []struct {
		name     string
		input    interface{}
		typeSpec interface{}
		expected interface{}
	}{
		{
			name:     "Nil input returns nil",
			input:    nil,
			typeSpec: rsInt,
			expected: nil,
		},
		{
			name:     "rsInt conversion",
			input:    int64(42),
			typeSpec: rsInt,
			expected: int32(42),
		},
		{
			name:     "rsBigInt conversion",
			input:    int64(42),
			typeSpec: rsBigInt,
			expected: 42,
		},
		{
			name:     "rsFloat conversion",
			input:    3.14,
			typeSpec: rsFloat,
			expected: 3.14,
		},
		{
			name:     "rsString conversion",
			input:    "hello",
			typeSpec: rsString,
			expected: "hello",
		},
		{
			name:     "rsBool conversion",
			input:    true,
			typeSpec: rsBool,
			expected: true,
		},
		{
			name:     "rsTimestamp conversion",
			input:    testTime,
			typeSpec: rsTimestamp,
			expected: testTime,
		},
		{
			name:     "Default case returns input unchanged",
			input:    "unchanged",
			typeSpec: "unknown", // unrecognized type specifier
			expected: "unchanged",
		},
	}

	// Execute each test case.
	for _, tc := range testCases {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			result := q.castTableItemType(tc.input, tc.typeSpec)
			assert.Equal(t, tc.expected, result)
		})
	}
}
