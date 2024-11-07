// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package tests

import (
	"log"

	"github.com/ory/dockertest/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// InitETCD initializes an etcd instance for testing. It runs the ETCD container on a random port that can be
// fetched with resource.GetPort("2379/tcp"). This instance does not need a username or password and will be torn down
// when the test completes.
func InitETCD(pool *dockertest.Pool) *dockertest.Resource {
	// pulls an image, creates a container based on it and runs it
	resource, err := pool.Run("bitnami/etcd", "latest", []string{"ALLOW_NONE_AUTHENTICATION=yes"})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	port := resource.GetPort("2379/tcp")

	log.Println("Connecting to etcd on port: ", port)

	if err = pool.Retry(func() error {
		_, err := clientv3.New(clientv3.Config{
			Endpoints: []string{"http://localhost:" + port},
		})
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatalf("Could not connect to etcd: %s", err)
	}
	return resource
}
