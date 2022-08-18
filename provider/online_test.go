//go:build online
// +build online

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
package provider

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/google/uuid"
)

func mockRedis() *miniredis.Miniredis {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	return s
}

type OnlineResource struct {
	Entity string
	Value  interface{}
	Type   ValueType
}

var provider = flag.String("provider", "all", "provider to perform test on")

func TestOnlineStores(t *testing.T) {
	testFns := map[string]func(*testing.T, OnlineStore){
		"CreateGetTable":     testCreateGetTable,
		"TableAlreadyExists": testTableAlreadyExists,
		"TableNotFound":      testTableNotFound,
		"SetGetEntity":       testSetGetEntity,
		"EntityNotFound":     testEntityNotFound,
		"TypeCasting":        testTypeCasting,
	}

	// Redis (Mock)
	redisMockInit := func() RedisConfig {
		mockRedisAddr := miniRedis.Addr()
		redisMockConfig := &RedisConfig{
			Addr: mockRedisAddr,
		}
		return *redisMockConfig
	}

	//Redis (Live)
	redisInsecureInit := func() RedisConfig {
		redisInsecurePort := os.Getenv("REDIS_INSECURE_PORT")
		insecureAddr := fmt.Sprintf("%s:%s", "localhost", redisInsecurePort)
		redisInsecureConfig := &RedisConfig{
			Addr: insecureAddr,
		}
		return *redisInsecureConfig
	}

	redisSecureInit := func() RedisConfig {
		redisSecurePort := os.Getenv("REDIS_SECURE_PORT")
		redisPassword := os.Getenv("REDIS_PASSWORD")
		secureAddr := fmt.Sprintf("%s:%s", "localhost", redisSecurePort)
		redisSecureConfig := &RedisConfig{
			Addr:     secureAddr,
			Password: redisPassword,
		}
		return *redisSecureConfig
	}

	//Cassandra
	cassandraInit := func() CassandraConfig {
		cassandraAddr := "localhost:9042"
		cassandraUsername := os.Getenv("CASSANDRA_USER")
		cassandraPassword := os.Getenv("CASSANDRA_PASSWORD")
		cassandraConfig := &CassandraConfig{
			Addr:        cassandraAddr,
			Username:    cassandraUsername,
			Consistency: "ONE",
			Password:    cassandraPassword,
			Replication: 3,
		}
		return *cassandraConfig
	}

	//Firestore
	firestoreInit := func() FirestoreConfig {
		projectID := os.Getenv("FIRESTORE_PROJECT")
		firestoreCredentials := os.Getenv("FIRESTORE_CRED")
		JSONCredentials, err := ioutil.ReadFile(firestoreCredentials)
		if err != nil {
			panic(fmt.Sprintf("Could not open firestore credentials: %v", err))
		}
		firestoreConfig := &FirestoreConfig{
			ProjectID:   projectID,
			Credentials: JSONCredentials,
		}
		return *firestoreConfig
	}

	dynamoInit := func() DynamodbConfig {
		dynamoAccessKey := os.Getenv("DYNAMO_ACCESS_KEY")
		dynamoSecretKey := os.Getenv("DYNAMO_SECRET_KEY")
		dynamoConfig := &DynamodbConfig{
			Region:    "us-east-1",
			AccessKey: dynamoAccessKey,
			SecretKey: dynamoSecretKey,
		}
		return *dynamoConfig
	}

	type testMember struct {
		t               Type
		subType         string
		c               SerializedConfig
		integrationTest bool
	}

	testList := []testMember{}

	if *provider == "memory" || *provider == "" {
		testList = append(testList, testMember{LocalOnline, "", []byte{}, false})
	}
	if *provider == "redis_mock" || *provider == "" {
		miniRedis := mockRedis()
		defer miniRedis.Close()
		testList = append(testList, testMember{RedisOnline, "_MOCK", redisMockInit().Serialized(), false})
	}
	if *provider == "redis_insecure" || *provider == "" {
		testList = append(testList, testMember{RedisOnline, "_INSECURE", redisInsecureInit().Serialized(), true})
	}
	if *provider == "redis_secure" || *provider == "" {
		testList = append(testList, testMember{RedisOnline, "_SECURE", redisSecureInit().Serialized(), true})
	}
	if *provider == "cassandra" || *provider == "" {
		testList = append(testList, testMember{CassandraOnline, "", cassandraInit().Serialized(), true})
	}
	if *provider == "firestore" || *provider == "" {
		testList = append(testList, testMember{FirestoreOnline, "", firestoreInit().Serialized(), true})
	}
	if *provider == "dynamo" || *provider == "" {
		testList = append(testList, testMember{DynamoDBOnline, "", dynamoInit().Serialized(), true})
	}

	for _, testItem := range testList {
		if testing.Short() && testItem.integrationTest {
			t.Logf("Skipping %s, because it is an integration test", testItem.t)
			continue
		}
		for name, fn := range testFns {
			provider, err := Get(testItem.t, testItem.c)
			if err != nil {
				t.Fatalf("Failed to get provider %s: %s", testItem.t, err)
			}
			store, err := provider.AsOnlineStore()
			if err != nil {
				t.Fatalf("Failed to use provider %s as OnlineStore: %s", testItem.t, err)
			}
			var prefix string
			if testItem.integrationTest {
				prefix = "INTEGRATION"
			} else {
				prefix = "UNIT"
			}
			testName := fmt.Sprintf("%s%s_%s_%s", testItem.t, testItem.subType, prefix, name)
			t.Run(testName, func(t *testing.T) {
				fn(t, store)
			})
		}
	}
}

func randomFeatureVariant() (string, string) {
	return uuid.NewString(), uuid.NewString()
}

func testCreateGetTable(t *testing.T, store OnlineStore) {
	mockFeature, mockVariant := randomFeatureVariant()
	defer store.DeleteTable(mockFeature, mockVariant)
	if tab, err := store.CreateTable(mockFeature, mockVariant, String); tab == nil || err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if tab, err := store.GetTable(mockFeature, mockVariant); tab == nil || err != nil {
		t.Fatalf("Failed to get table: %s", err)
	}
}

func testTableAlreadyExists(t *testing.T, store OnlineStore) {
	mockFeature, mockVariant := randomFeatureVariant()
	defer store.DeleteTable(mockFeature, mockVariant)
	if _, err := store.CreateTable(mockFeature, mockVariant, String); err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if _, err := store.CreateTable(mockFeature, mockVariant, String); err == nil {
		t.Fatalf("Succeeded in creating table twice")
	} else if casted, valid := err.(*TableAlreadyExists); !valid {
		t.Fatalf("Wrong error for table already exists: %T", err)
	} else if casted.Error() == "" {
		t.Fatalf("TableAlreadyExists has empty error message")
	}
}

func testTableNotFound(t *testing.T, store OnlineStore) {
	mockFeature, mockVariant := randomFeatureVariant()
	if _, err := store.GetTable(mockFeature, mockVariant); err == nil {
		t.Fatalf("Succeeded in getting non-existent table")
	} else if casted, valid := err.(*TableNotFound); !valid {
		t.Fatalf("Wrong error for table not found: %s,%T", err, err)
	} else if casted.Error() == "" {
		t.Fatalf("TableNotFound has empty error message")
	}
}

func testSetGetEntity(t *testing.T, store OnlineStore) {
	mockFeature, mockVariant := randomFeatureVariant()
	defer store.DeleteTable(mockFeature, mockVariant)
	entity, val := "e", "val"
	defer store.DeleteTable(mockFeature, mockVariant)
	tab, err := store.CreateTable(mockFeature, mockVariant, String)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if err := tab.Set(entity, val); err != nil {
		t.Fatalf("Failed to set entity: %s", err)
	}
	gotVal, err := tab.Get(entity)
	if err != nil {
		t.Fatalf("Failed to get entity: %s", err)
	}
	if !reflect.DeepEqual(val, gotVal) {
		t.Fatalf("Values are not the same %v %v", val, gotVal)
	}
}

func testEntityNotFound(t *testing.T, store OnlineStore) {
	mockFeature, mockVariant := uuid.NewString(), "v"
	entity := "e"
	defer store.DeleteTable(mockFeature, mockVariant)
	tab, err := store.CreateTable(mockFeature, mockVariant, String)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if _, err := tab.Get(entity); err == nil {
		t.Fatalf("succeeded in getting non-existent entity")
	} else if casted, valid := err.(*EntityNotFound); !valid {
		t.Fatalf("Wrong error for entity not found: %T", err)
	} else if casted.Error() == "" {
		t.Fatalf("EntityNotFound has empty error message")
	}
}

func testTypeCasting(t *testing.T, store OnlineStore) {
	onlineResources := []OnlineResource{
		{
			Entity: "a",
			Value:  int(1),
			Type:   Int,
		},
		{
			Entity: "b",
			Value:  int64(1),
			Type:   Int64,
		},
		{
			Entity: "c",
			Value:  float32(1.0),
			Type:   Float32,
		},
		{
			Entity: "d",
			Value:  float64(1.0),
			Type:   Float64,
		},
		{
			Entity: "e",
			Value:  "1.0",
			Type:   String,
		},
		{
			Entity: "f",
			Value:  false,
			Type:   Bool,
		},
	}
	for _, resource := range onlineResources {
		featureName := uuid.New().String()
		tab, err := store.CreateTable(featureName, "", resource.Type)
		if err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}
		if err := tab.Set(resource.Entity, resource.Value); err != nil {
			t.Fatalf("Failed to set entity: %s", err)
		}
		gotVal, err := tab.Get(resource.Entity)
		if err != nil {
			t.Fatalf("Failed to get entity: %s", err)
		}
		if !reflect.DeepEqual(resource.Value, gotVal) {
			t.Fatalf("Values are not the same %v, type %T. %v, type %T", resource.Value, resource.Value, gotVal, gotVal)
		}
		store.DeleteTable(featureName, "")
	}
}
