//go:build online
// +build online

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
package provider

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/featureform/helpers"

	"github.com/alicebob/miniredis"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
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
	err := godotenv.Load("../.env")
	if err != nil {
		fmt.Println(err)
	}

	testFns := map[string]func(*testing.T, OnlineStore){
		"CreateGetTable":      testCreateGetTable,
		"TableAlreadyExists":  testTableAlreadyExists,
		"TableNotFound":       testTableNotFound,
		"SetGetEntity":        testSetGetEntity,
		"EntityNotFound":      testEntityNotFound,
		"MassTableWrite":      testMassTableWrite,
		"SimpleTypeCasting":   testSimpleTypeCasting,
		"InvalidTypes":        testInvalidTypes,
		"TypeCastingOverride": testTypeCastingOverride,
	}

	// Redis (Mock)
	redisMockInit := func(mRedis *miniredis.Miniredis) RedisConfig {
		mockRedisAddr := mRedis.Addr()
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
		fmt.Println(os.Getwd())
		projectID := os.Getenv("FIRESTORE_PROJECT")
		firestoreCredentials := os.Getenv("FIRESTORE_CRED")
		JSONCredentials, err := ioutil.ReadFile(firestoreCredentials)
		if err != nil {
			panic(fmt.Sprintf("Could not open firestore credentials: %v", err))
		}

		var credentialsDict map[string]interface{}
		err = json.Unmarshal(JSONCredentials, &credentialsDict)
		if err != nil {
			panic(fmt.Errorf("cannot unmarshal firestore credentials: %v", err))
		}

		firestoreConfig := &FirestoreConfig{
			Collection:  "featureform_test",
			ProjectID:   projectID,
			Credentials: credentialsDict,
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

	blobAzureInit := func() OnlineBlobConfig {
		azureConfig := AzureBlobStoreConfig{
			AccountName:   helpers.GetEnv("AZURE_ACCOUNT_NAME", ""),
			AccountKey:    helpers.GetEnv("AZURE_ACCOUNT_KEY", ""),
			ContainerName: helpers.GetEnv("AZURE_CONTAINER_NAME", "newcontainer"),
			Path:          "featureform/onlinetesting",
		}
		blobConfig := &OnlineBlobConfig{
			Type:   Azure,
			Config: azureConfig,
		}
		return *blobConfig
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
		testList = append(testList, testMember{RedisOnline, "_MOCK", redisMockInit(miniRedis).Serialized(), false})
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
		testList = append(testList, testMember{FirestoreOnline, "", firestoreInit().Serialize(), true})
	}
	if *provider == "dynamo" || *provider == "" {
		testList = append(testList, testMember{DynamoDBOnline, "", dynamoInit().Serialized(), true})
	}
	if *provider == "azure_blob" || *provider == "" {
		testList = append(testList, testMember{BlobOnline, "_AZURE", blobAzureInit().Serialized(), true})
	}
	for _, testItem := range testList {
		if testing.Short() && testItem.integrationTest {
			t.Logf("Skipping %s, because it is an integration test", testItem.t)
			continue
		}
		for name, fn := range testFns {
			provider, err := Get(testItem.t, testItem.c)
			if err != nil {
				t.Errorf("Failed to get provider %s: %s", testItem.t, err)
				continue
			}
			store, err := provider.AsOnlineStore()
			if err != nil {
				t.Errorf("Failed to use provider %s as OnlineStore: %s", testItem.t, err)
				continue
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
			if err := store.Close(); err != nil {
				t.Errorf("Failed to close online store %s: %v", testItem.t, err)
				continue
			}

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

func testMassTableWrite(t *testing.T, store OnlineStore) {
	mockFeature, mockVariant := randomFeatureVariant()
	table := ResourceID{mockFeature, mockVariant, Feature}
	entityList := make([]string, 1000)
	for i := range entityList {
		entityList[i] = uuid.New().String()
	}
	tab, err := store.CreateTable(table.Name, table.Variant, ValueType("int"))
	if err != nil {
		t.Fatalf("could not create table %v in online store: %v", table, err)
	}
	defer store.DeleteTable(table.Name, table.Variant)
	for i := range entityList {
		if err := tab.Set(entityList[i], 1); err != nil {
			t.Fatalf("could not set entity %v in table %v: %v", entityList[i], table, err)
		}
	}
	tab, err = store.GetTable(table.Name, table.Variant)
	if err != nil {
		t.Fatalf("could not get table %v in online store: %v", table, err)
	}
	for j := range entityList {
		val, err := tab.Get(entityList[j])
		if err != nil {
			t.Fatalf("could not get entity %v in table %v: %v", entityList[j], table, err)
		}
		if val != 1 {
			t.Fatalf("could not get correct value from entity list. Wanted %v, got %v", 1, val)
		}
	}
}

func testSimpleTypeCasting(t *testing.T, store OnlineStore) {
	testCases := []struct {
		Resource     OnlineResource
		ExpectedType reflect.Type
	}{
		{
			Resource: OnlineResource{
				Entity: "int",
				Value:  int(1),
				Type:   Int,
			},
			ExpectedType: reflect.TypeOf(int(1)),
		},
		{
			Resource: OnlineResource{
				Entity: "int32",
				Value:  int32(1),
				Type:   Int32,
			},
			ExpectedType: reflect.TypeOf(int32(1)),
		},
		{
			Resource: OnlineResource{
				Entity: "int64",
				Value:  int64(1),
				Type:   Int64,
			},
			ExpectedType: reflect.TypeOf(int64(1)),
		},
		{
			Resource: OnlineResource{
				Entity: "float32",
				Value:  float32(1.0),
				Type:   Float32,
			},
			ExpectedType: reflect.TypeOf(float32(1)),
		},
		{
			Resource: OnlineResource{
				Entity: "float64",
				Value:  float64(1.0),
				Type:   Float64,
			},
			ExpectedType: reflect.TypeOf(float64(1)),
		},
		{
			Resource: OnlineResource{
				Entity: "string",
				Value:  "1",
				Type:   String,
			},
			ExpectedType: reflect.TypeOf(""),
		},
		{
			Resource: OnlineResource{
				Entity: "bool",
				Value:  true,
				Type:   Bool,
			},
			ExpectedType: reflect.TypeOf(true),
		},
		{
			Resource: OnlineResource{
				Entity: "timestamp",
				Value:  time.Unix(100000000, 0).UTC(),
				Type:   Timestamp,
			},
			ExpectedType: reflect.TypeOf(time.Unix(100000000, 0).UTC()),
		},
		{
			Resource: OnlineResource{
				Entity: "datetime",
				Value:  time.Unix(100000000, 0).UTC(),
				Type:   Datetime,
			},
			ExpectedType: reflect.TypeOf(time.Unix(100000000, 0).UTC()),
		},
	}
	for _, c := range testCases {
		featureName := uuid.New().String()
		tab, err := store.CreateTable(featureName, "", c.Resource.Type)
		if err != nil {
			t.Errorf("Failed to create table: %s", err)
			continue
		}
		if err := tab.Set(c.Resource.Entity, c.Resource.Value); err != nil {
			t.Errorf("Failed to set entity: %s", err)
			store.DeleteTable(featureName, "")
			continue
		}
		gotVal, err := tab.Get(c.Resource.Entity)
		if err != nil {
			t.Errorf("Failed to get entity: %s", err)
			store.DeleteTable(featureName, "")
			continue
		}
		if !reflect.DeepEqual(c.Resource.Value, gotVal) {
			t.Errorf("Values are not the same Entity: %s Values: %v (%T) != %v (%T)", c.Resource.Entity, c.Resource.Value, c.Resource.Value, gotVal, gotVal)
		}
		store.DeleteTable(featureName, "")
	}
}

func testTypeCastingOverride(t *testing.T, store OnlineStore) {
	testCases := []struct {
		Resource      OnlineResource
		ExpectedType  reflect.Type
		ExpectedValue interface{}
		ShouldError   bool
	}{
		{
			Resource: OnlineResource{
				Entity: "int",
				Value:  int(1),
				Type:   Int,
			},
			ExpectedType:  reflect.TypeOf(int(1)),
			ExpectedValue: 1,
			ShouldError:   false,
		},
		{
			Resource: OnlineResource{
				Entity: "int",
				Value:  int(1),
				Type:   String,
			},
			ExpectedType:  reflect.TypeOf(int(1)),
			ExpectedValue: "1",
			ShouldError:   false,
		},
		{
			Resource: OnlineResource{
				Entity: "int32",
				Value:  int32(1),
				Type:   Int,
			},
			ExpectedType:  reflect.TypeOf(int32(1)),
			ExpectedValue: 1,
			ShouldError:   false,
		},
		{
			Resource: OnlineResource{
				Entity: "int64",
				Value:  int64(1),
				Type:   Int,
			},
			ExpectedType:  reflect.TypeOf(int64(1)),
			ExpectedValue: 1,
			ShouldError:   false,
		},
		{
			Resource: OnlineResource{
				Entity: "float32",
				Value:  float32(1.2),
				Type:   Int,
			},
			ExpectedType:  reflect.TypeOf(float32(1)),
			ExpectedValue: 1,
			ShouldError:   true,
		},
		{
			Resource: OnlineResource{
				Entity: "float64",
				Value:  float64(1.2),
				Type:   Int,
			},
			ExpectedType:  reflect.TypeOf(float64(1)),
			ExpectedValue: 1,
			ShouldError:   true,
		},
		{
			Resource: OnlineResource{
				Entity: "string",
				Value:  "somestring",
				Type:   Int,
			},
			ExpectedType: reflect.TypeOf(""),
			ShouldError:  true,
		},
		{
			Resource: OnlineResource{
				Entity: "bool",
				Value:  true,
				Type:   Int,
			},
			ExpectedType:  reflect.TypeOf(true),
			ExpectedValue: 1,
			ShouldError:   true,
		},
		{
			Resource: OnlineResource{
				Entity: "time",
				Value:  time.Now().UTC(),
				Type:   Int,
			},
			ExpectedType: reflect.TypeOf(time.Now().UTC()),
			ShouldError:  true,
		},
	}
	for _, c := range testCases {
		featureName := uuid.New().String()
		tab, err := store.CreateTable(featureName, "", c.Resource.Type)
		if err != nil {
			t.Errorf("Failed to create table: %s", err)
		}
		err = tab.Set(c.Resource.Entity, c.Resource.Value)
		if err != nil {
			t.Errorf("Unable to set resource with entity: %s, value: %v, type: %s: %s", c.Resource.Entity, c.Resource.Value, c.Resource.Type, err.Error())
			continue
		}
		gotVal, err := tab.Get(c.Resource.Entity)
		if err != nil && !c.ShouldError {
			t.Errorf("Unable to get resource value for entity %s: %v, Type: %v: %s", c.Resource.Entity, c.Resource.Value, c.Resource.Type, err.Error())
			store.DeleteTable(featureName, "")
			continue
		} else if err == nil && c.ShouldError {
			t.Errorf("Invalid value get created with Value Type: %v, Value: %v, Table Type: %v", reflect.TypeOf(c.Resource.Value), c.Resource.Value, c.Resource.Type)
			store.DeleteTable(featureName, "")
			continue
		} else if err != nil && c.ShouldError {
			store.DeleteTable(featureName, "")
			continue
		}
		if !reflect.DeepEqual(c.ExpectedValue, gotVal) {
			t.Errorf("Values are not the same entity:%s, %v, type %T. %v, type %T", c.Resource.Entity, c.ExpectedValue, c.ExpectedValue, gotVal, gotVal)
		}
		store.DeleteTable(featureName, "")
	}
}

func testInvalidTypes(t *testing.T, store OnlineStore) {
	testCases := []struct {
		Resource    OnlineResource
		ShouldError bool
	}{
		{

			Resource: OnlineResource{
				Entity: "a",
				Value:  1,
				Type:   Int,
			},
			ShouldError: false,
		},
		{

			Resource: OnlineResource{
				Entity: "b",
				Value:  1,
				Type:   Int32,
			},
			ShouldError: false,
		},
		{

			Resource: OnlineResource{
				Entity: "c",
				Value:  1,
				Type:   Int64,
			},
			ShouldError: false,
		},
		{

			Resource: OnlineResource{
				Entity: "d",
				Value:  1,
				Type:   Float32,
			},
			ShouldError: false,
		},
		{

			Resource: OnlineResource{
				Entity: "e",
				Value:  1,
				Type:   Float64,
			},
			ShouldError: false,
		},
		{

			Resource: OnlineResource{
				Entity: "f",
				Value:  true,
				Type:   Bool,
			},
			ShouldError: false,
		},
		{

			Resource: OnlineResource{
				Entity: "g",
				Value:  time.Now(),
				Type:   Timestamp,
			},
			ShouldError: false,
		},
		{

			Resource: OnlineResource{
				Entity: "h",
				Value:  1,
				Type:   "str",
			},
			ShouldError: true,
		},
		{
			// Will default to string if left blank
			Resource: OnlineResource{
				Entity: "i",
				Value:  1,
				Type:   "",
			},
			ShouldError: false,
		},
		{

			Resource: OnlineResource{
				Entity: "j",
				Value:  1,
				Type:   "somenontype",
			},
			ShouldError: true,
		},
	}
	for _, c := range testCases {
		featureName := uuid.New().String()
		_, err := store.CreateTable(featureName, "", c.Resource.Type)
		if err != nil && !c.ShouldError {
			t.Errorf("Unable to create resource value: %v, Type: %v", c.Resource.Value, c.Resource.Type)
		} else if err == nil && c.ShouldError {
			t.Errorf("Invalid table created with Type: %v", c.Resource.Type)
		}
	}
}

func TestFirestoreConfig_Deserialize(t *testing.T) {
	content, err := ioutil.ReadFile("connection/connection_configs.json")
	if err != nil {
		t.Fatalf(err.Error())
	}
	var payload map[string]interface{}
	err = json.Unmarshal(content, &payload)
	if err != nil {
		t.Fatalf(err.Error())
	}
	testConfig := payload["Firestore"].(map[string]interface{})

	fsconfig := FirestoreConfig{
		ProjectID:   testConfig["ProjectID"].(string),
		Collection:  testConfig["Collection"].(string),
		Credentials: testConfig["Credentials"].(map[string]interface{}),
	}

	serialized := fsconfig.Serialize()

	type fields struct {
		Collection  string
		ProjectID   string
		Credentials map[string]interface{}
	}
	type args struct {
		config SerializedConfig
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "TestCredentials",
			fields: fields{
				ProjectID:   testConfig["ProjectID"].(string),
				Collection:  testConfig["Collection"].(string),
				Credentials: testConfig["Credentials"].(map[string]interface{}),
			},
			args: args{
				config: serialized,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &FirestoreConfig{
				Collection:  tt.fields.Collection,
				ProjectID:   tt.fields.ProjectID,
				Credentials: tt.fields.Credentials,
			}
			if err := r.Deserialize(tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("Deserialize() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
