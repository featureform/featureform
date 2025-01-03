// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/joho/godotenv"
)

func TestOfflineStorePostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}

	db, ok := os.LookupEnv("POSTGRES_DB")
	if !ok {
		t.Fatalf("missing POSTGRES_DB variable")
	}
	user, ok := os.LookupEnv("POSTGRES_USER")
	if !ok {
		t.Fatalf("missing POSTGRES_USER variable")
	}
	password, ok := os.LookupEnv("POSTGRES_PASSWORD")
	if !ok {
		t.Fatalf("missing POSTGRES_PASSWORD variable")
	}

	postgresConfig := pc.PostgresConfig{
		Host:     "localhost",
		Port:     "5432",
		Database: db,
		Username: user,
		Password: password,
		SSLMode:  "disable",
	}

	serialize, err := postgresConfig.Serialize()
	assert.NoError(t, err)
	store, err := GetOfflineStore(pt.PostgresOffline, serialize)
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OfflineStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
	test.RunSQL()
}
