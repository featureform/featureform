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
	"time"

	"github.com/featureform/helpers"
	_ "github.com/lib/pq"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestUint64OrderedId(t *testing.T) {
	id1 := Uint64OrderedId(1)
	id2 := Uint64OrderedId(2)
	id3 := Uint64OrderedId(1)

	if !id1.Equals(id3) {
		t.Errorf("Expected id1 to equal id3")
	}

	if id1.Equals(id2) {
		t.Errorf("Expected id1 to not equal id2")
	}

	if !id1.Less(id2) {
		t.Errorf("Expected id1 to be less than id2")
	}

	if id2.Less(id1) {
		t.Errorf("Expected id2 to not be less than id1")
	}

	if id1.String() != "1" {
		t.Errorf("Expected id1 to be '1'")
	}

	if id2.String() != "2" {
		t.Errorf("Expected id2 to be '2'")
	}
}

func TestOrderedIdGenerator(t *testing.T) {
	testCases := []struct {
		name      string
		shortTest bool
		createGen func() (OrderedIdGenerator, error)
		deferFunc func(generator OrderedIdGenerator, t *testing.T)
	}{
		{
			name:      "Memory",
			shortTest: true,
			createGen: createMemoryIdGenerator,
			deferFunc: func(generator OrderedIdGenerator, t *testing.T) {},
		},
		{
			name:      "ETCD",
			shortTest: false,
			createGen: createETCDIdGenerator,
			deferFunc: func(generator OrderedIdGenerator, t *testing.T) {
				// Clean up the ETCD keys
				etcd := generator.(*etcdIdGenerator)
				_, err := etcd.client.Delete(context.Background(), "", clientv3.WithPrefix())
				if err != nil {
					t.Errorf("failed to delete keys with prefix %s: %v", "", err)
				}
				etcd.Close()
			},
		},
		{
			name:      "Postgres",
			shortTest: false,
			createGen: createPSQLIdGenerator,
			deferFunc: func(generator OrderedIdGenerator, t *testing.T) {
				// Clean up the RDS table
				pg := generator.(*pgIdGenerator)
				_, err := pg.db.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", pg.tableName))
				if err != nil {
					t.Errorf("failed to drop table %s: %v", pg.tableName, err)
				}
				pg.Close()
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.shortTest && testing.Short() {
				t.Skip()
			}
			generator, err := tc.createGen()
			if err != nil {
				t.Fatalf("failed to create %s ID generator: %v", tc.name, err)
			}
			defer tc.deferFunc(generator, t)

			prevId, err := generator.NextId("testNamespace")
			if err != nil {
				t.Errorf("failed to get next id: %v", err)
			}

			diffNamespaceId, err := generator.NextId("diffNamespace")
			if err != nil {
				t.Errorf("failed to get next id: %v", err)
			}

			if !prevId.Equals(diffNamespaceId) {
				t.Errorf("expected id: '%s' Received Id: '%s'", diffNamespaceId, prevId)
			}

			for i := 0; i < 10; i++ {
				id, err := generator.NextId("testNamespace")
				if err != nil {
					t.Errorf("failed to get next id: %v", err)
				}

				if !prevId.Less(id) {
					t.Errorf("expected id '%s' to be greater than previous id '%s'", id, prevId)
				}
				prevId = id
			}
		})
	}
}

func createMemoryIdGenerator() (OrderedIdGenerator, error) {
	return NewMemoryOrderedIdGenerator()
}

func createETCDIdGenerator() (OrderedIdGenerator, error) {
	etcdHost := helpers.GetEnv("ETCD_HOST", "localhost")

	etcdConfig := helpers.ETCDConfig{
		Host:        etcdHost,
		Port:        etcdPort,
		DialTimeout: time.Second * 5,
	}

	return NewETCDOrderedIdGenerator(etcdConfig)
}

func createPSQLIdGenerator() (OrderedIdGenerator, error) {
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

	return NewPSQLOrderedIdGenerator(config)
}
