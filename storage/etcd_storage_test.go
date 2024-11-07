// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package storage

import (
	"testing"

	"github.com/featureform/helpers"
)

func TestETCDMetadataStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	etcdHost := helpers.GetEnv("ETCD_HOST", "localhost")
	etcdPort := helpers.GetEnv("ETCD_PORT", "2379")

	etcdConfig := helpers.ETCDConfig{
		Host:     etcdHost,
		Port:     etcdPort,
		Username: "root",
		Password: "secretpassword",
	}

	etcdStorage, err := NewETCDStorageImplementation(etcdConfig)
	if err != nil {
		t.Fatalf("Failed to create ETCD storage: %v", err)
	}

	test := MetadataStorageTest{
		t:       t,
		storage: etcdStorage,
	}
	test.Run()
}
