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
	"strconv"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"

	"github.com/featureform/helpers"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
)

func TestOfflineStoreClickhouse(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}

	clickHouseDb := ""
	ok := true
	if clickHouseDb, ok = os.LookupEnv("CLICKHOUSE_DB"); !ok {
		clickHouseDb = fmt.Sprintf("feature_form_%d", time.Now().UnixMilli())
	}

	username, ok := os.LookupEnv("CLICKHOUSE_USER")
	if !ok {
		t.Fatalf("missing CLICKHOUSE_USER variable")
	}
	password, ok := os.LookupEnv("CLICKHOUSE_PASSWORD")
	if !ok {
		t.Fatalf("missing CLICKHOUSE_PASSWORD variable")
	}
	host, ok := os.LookupEnv("CLICKHOUSE_HOST")
	if !ok {
		t.Fatalf("missing CLICKHOUSE_HOST variable")
	}
	portStr, ok := os.LookupEnv("CLICKHOUSE_PORT")
	if !ok {
		t.Fatalf("missing CLICKHOUSE_PORT variable")
	}
	ssl := helpers.GetEnvBool("CLICKHOUSE_SSL", false)

	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("Failed to parse port to numeric: %v", portStr)
	}

	var clickHouseConfig = pc.ClickHouseConfig{
		Host:     host,
		Port:     uint16(port),
		Username: username,
		Password: password,
		Database: clickHouseDb,
		SSL:      ssl,
	}

	if err := createClickHouseDatabase(clickHouseConfig); err != nil {
		t.Fatalf("%v", err)
	}

	store, err := GetOfflineStore(pt.ClickHouseOffline, clickHouseConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OfflineStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
	// test.RunSQL()
}

func createClickHouseDatabase(c pc.ClickHouseConfig) error {
	conn, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s&secure=%t", c.Host, c.Port, c.Username, c.Password, c.SSL))
	if err != nil {
		return err
	}
	return createDatabases(c, conn)
}

func createDatabases(c pc.ClickHouseConfig, conn *sql.DB) error {
	if _, err := conn.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", SanitizeClickHouseIdentifier(c.Database))); err != nil {
		return err
	}
	if _, err := conn.Exec(fmt.Sprintf("CREATE DATABASE %s", SanitizeClickHouseIdentifier(c.Database))); err != nil {
		return err
	}
	return nil
}

func TestTrainingSet(t *testing.T) {
	t.Skip()
	var clickHouseConfig = pc.ClickHouseConfig{
		Host:     "127.0.0.1",
		Port:     uint16(9000),
		Username: "default",
		Password: "",
		Database: "ff",
		SSL:      false,
	}

	store, err := NewClickHouseOfflineStore(clickHouseConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	tsDef := TrainingSetDef{
		ID: ResourceID{
			Name:    "ts_alice",
			Variant: "v5",
			Type:    TrainingSet,
		},
		Label: ResourceID{
			Name:    "fraudulent",
			Variant: "2024-02-16t11-11-50",
			Type:    Label,
		},
		Features: []ResourceID{
			{
				Name:    "avg_transactions",
				Variant: "2024-02-16t11-11-50",
				Type:    Feature,
			},
		},
		LagFeatures: nil,
	}
	err = store.CreateTrainingSet(tsDef)
	if err != nil {
		t.Fatalf("could not create training set: %s\n", err)
	}
	set, err := store.GetTrainingSet(tsDef.ID)
	if err != nil {
		return
	}
	for set.Next() {
		t.Logf("features %v and labels %v\n", set.Features(), set.Label())
	}
}

func TestSplit(t *testing.T) {
	t.Skip()
	var clickHouseConfig = pc.ClickHouseConfig{
		Host:     "127.0.0.1",
		Port:     uint16(9000),
		Username: "default",
		Password: "",
		Database: "ff",
		SSL:      false,
	}

	store, err := NewClickHouseOfflineStore(clickHouseConfig.Serialize())
	if err != nil {
		fmt.Printf("could not initialize store: %s\n", err)
	}

	resourceId := ResourceID{
		Name:    "ts_alice",
		Variant: "v5",
		Type:    TrainingSet,
	}

	trainTestSplitDef := TrainTestSplitDef{
		TrainingSetName:    resourceId.Name,
		TrainingSetVariant: resourceId.Variant,
		TestSize:           .5,
		Shuffle:            true,
		RandomState:        1,
	}

	closeFunc, err := store.CreateTrainTestSplit(trainTestSplitDef)
	if err != nil {
		t.Fatalf("could not create split: %s\n", err)
	}
	defer closeFunc()
	train, test, err := store.GetTrainTestSplit(trainTestSplitDef)
	if err != nil {
		t.Fatalf("could not get split: %s\n", err)
	}

	if err != nil {
		t.Fatalf("could not get split: %s\n", err)
	}
	// loop through train
	var trainCount int
	for train.Next() {
		// print features and labels
		t.Logf("features %v and labels %v\n", train.Features(), train.Label())
		trainCount++
	}
	t.Logf("train count: %d", trainCount)

	var testCount int
	for test.Next() {
		t.Logf("features %v and labels %v\n", test.Features(), test.Label())
		testCount++
	}
	t.Logf("test count: %d", testCount)

	health, err := store.CheckHealth()
	if err != nil {
		t.Fatalf("health check failed: %s", err)
	}
	if !health {
		t.Fatalf("health check failed")
	}
}

func TestClickhouseCastTableItemType(t *testing.T) {
	q := clickhouseSQLQueries{}

	var (
		maxInt = int(^uint(0) >> 1)
	)

	testTime := time.Date(2025, time.February, 13, 12, 0, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		input    interface{}
		typeSpec interface{}
		expected interface{}
	}{
		{
			name:     "Nil input returns nil",
			input:    nil,
			typeSpec: chInt,
			expected: nil,
		},
		{
			name:     "chInt simple conversion",
			input:    42,
			typeSpec: chInt,
			expected: 42,
		},
		{
			name:     "chInt with Nullable wrapper",
			input:    42,
			typeSpec: "Nullable(" + chInt + ")",
			expected: 42,
		},
		{
			name:     "chInt32 conversion",
			input:    int32(42),
			typeSpec: chInt32,
			expected: 42,
		},
		{
			name:     "chUInt32 in range conversion",
			input:    uint32(100),
			typeSpec: chUInt32,
			expected: 100, // converted to int
		},
		{
			name:     "chUInt32 out of range returns original",
			input:    uint32(3000000000), // 3e9 > 2^31-1 (2147483647)
			typeSpec: chUInt32,
			expected: uint32(3000000000),
		},
		{
			name:     "chInt64 in range conversion",
			input:    int64(42),
			typeSpec: chInt64,
			expected: 42,
		},
		{
			name:     "chUInt64 in range conversion",
			input:    uint64(100),
			typeSpec: chUInt64,
			expected: 100, // converted to int
		},
		{
			name:     "chUInt64 out of range returns original",
			input:    uint64(maxInt) + 1,
			typeSpec: chUInt64,
			expected: uint64(maxInt) + 1,
		},
		{
			name:     "chDateTime valid time",
			input:    testTime,
			typeSpec: chDateTime,
			expected: testTime,
		},
		{
			name:     "chDateTime invalid type returns default UnixMilli",
			input:    "not a time",
			typeSpec: chDateTime,
			expected: time.UnixMilli(0).UTC(),
		},
	}

	for _, tc := range testCases {
		tc := tc // capture loop variable
		t.Run(tc.name, func(t *testing.T) {
			result := q.castTableItemType(tc.input, tc.typeSpec)
			assert.Equal(t, tc.expected, result)
		})
	}
}
