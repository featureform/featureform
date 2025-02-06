// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package ffsync

import (
	"flag"
	"log"
	"os"
	"testing"

	"github.com/featureform/helpers/tests"
	"github.com/ory/dockertest/v3"
)

var pgPort string

// Flag to enable whether to use env vars or built-in values for dockertest
var useEnv = flag.Bool("use-env", false, "Use environment variables for ETCD configuration")

func initTestDocker() (*dockertest.Pool, []*dockertest.Resource) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	// uses pool to try to connect to Docker
	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	var resources []*dockertest.Resource
	res := tests.InitPG(pool)
	pgPort = res.GetPort("5432/tcp")
	res.Expire(120)
	resources = append(resources, res)

	log.Printf("pg conection: %s", res.GetHostPort("5432/tcp"))
	log.Printf("pg bound ip: %s", res.GetBoundIP("5432/tcp"))
	return pool, resources
}

func TestMain(m *testing.M) {
	flag.Parse()
	// Skip when using remote provider
	if *useEnv {
		return
	}
	var resources []*dockertest.Resource
	var pool *dockertest.Pool
	if !testing.Short() {
		pool, resources = initTestDocker()
	}
	code := m.Run()
	if !testing.Short() {
		// You can't defer this because os.Exit doesn't care for defer
		for _, resource := range resources {
			if err := pool.Purge(resource); err != nil {
				log.Fatalf("Could not purge resource: %s", err)
			}
		}
	}
	os.Exit(code)
}
