package ffsync

import (
	"context"
	"fmt"
	"testing"

	"github.com/featureform/helpers"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestUint64OrderedId(t *testing.T) {
	id1 := Uint64OrderedId(1)
	id2 := Uint64OrderedId(2)
	id3 := Uint64OrderedId(1)

	if !id1.Equals(&id3) {
		t.Errorf("Expected id1 to equal id3")
	}

	if id1.Equals(&id2) {
		t.Errorf("Expected id1 to not equal id2")
	}

	if !id1.Less(&id2) {
		t.Errorf("Expected id1 to be less than id2")
	}

	if id2.Less(&id1) {
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
		createGen func() (OrderedIdGenerator, error)
		deferFunc func(generator OrderedIdGenerator, t *testing.T)
	}{
		{
			name:      "Memory",
			createGen: createMemoryGenerator,
			deferFunc: func(generator OrderedIdGenerator, t *testing.T) {},
		},
		{
			name:      "ETCD",
			createGen: createETCDGenerator,
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
			name:      "RDS",
			createGen: createRDSGenerator,
			deferFunc: func(generator OrderedIdGenerator, t *testing.T) {
				// Clean up the RDS table
				rds := generator.(*rdsIdGenerator)
				_, err := rds.db.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", rds.tableName))
				if err != nil {
					t.Errorf("failed to drop table %s: %v", rds.tableName, err)
				}
				rds.Close()
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if testing.Short() && (tc.name == "ETCD" || tc.name == "RDS") {
				t.Skip("skipping integration test")
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

func createMemoryGenerator() (OrderedIdGenerator, error) {
	return NewMemoryOrderedIdGenerator()
}

func createETCDGenerator() (OrderedIdGenerator, error) {
	etcdHost := helpers.GetEnv("ETCD_HOST", "localhost")
	etcdPort := helpers.GetEnv("ETCD_PORT", "2379")

	etcdConfig := helpers.ETCDConfig{
		Host: etcdHost,
		Port: etcdPort,
	}

	return NewETCDOrderedIdGenerator(etcdConfig)
}

func createRDSGenerator() (OrderedIdGenerator, error) {
	host := helpers.GetEnv("POSTGRES_HOST", "localhost")
	port := helpers.GetEnv("POSTGRES_PORT", "5432")
	username := helpers.GetEnv("POSTGRES_USER", "postgres")
	password := helpers.GetEnv("POSTGRES_PASSWORD", "mysecretpassword")
	dbName := helpers.GetEnv("POSTGRES_DB", "postgres")
	sslMode := helpers.GetEnv("POSTGRES_SSL_MODE", "disable")

	config := helpers.RDSConfig{
		Host:     host,
		Port:     port,
		User:     username,
		Password: password,
		DBName:   dbName,
		SSLMode:  sslMode,
	}

	return NewRDSOrderedIdGenerator(config)
}
