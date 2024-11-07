// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package tests

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/ory/dockertest/v3"
)

// InitPG initializes an postgres instance for testing. It runs the Postgres container on a random port that can be
// fetched with resource.GetPort("5432/tcp"). This instance uses username=postgres, password=mysecretpassword, database=postgres.
// This instance will be torn down when the test completes.
func InitPG(pool *dockertest.Pool) *dockertest.Resource {
	// pulls an image, creates a container based on it and runs it
	resource, err := pool.Run("postgres", "16.1", []string{"POSTGRES_PASSWORD=mysecretpassword", "POSTGRES_DB=postgres"})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	port := resource.GetPort("5432/tcp")
	databaseUrl := fmt.Sprintf("postgres://postgres:mysecretpassword@localhost:%s/postgres?sslmode=disable", port)

	log.Println("Connecting to postgres on port: ", databaseUrl)

	if err = pool.Retry(func() error {
		db, err := sql.Open("postgres", databaseUrl)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to postgres: %s", err)
	}
	return resource
}
