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
		"CreateGetTable":     testCreateGetTable,
		"TableAlreadyExists": testTableAlreadyExists,
		"TableNotFound":      testTableNotFound,
		"SetGetEntity":       testSetGetEntity,
		"EntityNotFound":     testEntityNotFound,
		"MassTableWrite":     testMassTableWrite,
		"TypeCasting":        testTypeCasting,
		"InvalidTypes":       testInvalidTypes,
		"IncorrectTypes":     testIncorrectTypes,
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
	fmt.Println("Test List: ", testList)
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
	tableList := make([]ResourceID, 10)
	for i := range tableList {
		mockFeature, mockVariant := randomFeatureVariant()
		tableList[i] = ResourceID{mockFeature, mockVariant, Feature}
	}
	entityList := make([]string, 10)
	for i := range entityList {
		entityList[i] = uuid.New().String()
	}
	for i := range tableList {
		tab, err := store.CreateTable(tableList[i].Name, tableList[i].Variant, ValueType("int"))
		if err != nil {
			t.Fatalf("could not create table %v in online store: %v", tableList[i], err)
		}
		for j := range entityList {
			if err := tab.Set(entityList[j], 1); err != nil {
				t.Fatalf("could not set entity %v in table %v: %v", entityList[j], tableList[i], err)
			}
		}
	}
	for i := range tableList {
		tab, err := store.GetTable(tableList[i].Name, tableList[i].Variant)
		if err != nil {
			t.Fatalf("could not get table %v in online store: %v", tableList[i], err)
		}
		for j := range entityList {
			val, err := tab.Get(entityList[j])
			if err != nil {
				t.Fatalf("could not get entity %v in table %v: %v", entityList[j], tableList[i], err)
			}
			if val != 1 {
				t.Fatalf("could not get correct value from entity list. Wanted %v, got %v", 1, val)
			}
		}
	}
}

func testSimpleTypeCasting(t *testing.T, store OnlineStore) {
	testCases := []struct {
		OnlineResource
		ExpectedType reflect.Type
	}{
		{
			OnlineResource{
				Entity: "int",
				Value:  int(1),
			},
			ExpectedType: reflect.Int,
		},
		{
			OnlineResource{
				Entity: "int8",
				Value:  int8(1),
			},
			ExpectedType: reflect.Int8,
		},
		{
			OnlineResource{
				Entity: "int16",
				Value:  int16(1),
			},
			ExpectedType: reflect.Int16,
		},
		{
			OnlineResource{
				Entity: "int32",
				Value:  int32(1),
			},
			ExpectedType: reflect.Int32,
		},
		{
			OnlineResource{
				Entity: "int64",
				Value:  int64(1),
			},
			ExpectedType: reflect.Int64,
		},
		{
			OnlineResource{
				Entity: "float32",
				Value:  float32(1.0),
			},
			ExpectedType: reflect.Float32,
		},
		{
			OnlineResource{
				Entity: "float64",
				Value:  float64(1.0),
			},
			ExpectedType: reflect.Float64,
		},
		{
			OnlineResource{
				Entity: "string",
				Value:  "1",
			},
			ExpectedType: reflect.String,
		},
		{
			OnlineResource{
				Entity: "bool",
				Value:  true,
			},
			ExpectedType: reflect.Bool,
		},
		{
			OnlineResource{
				Entity: "complex64",
				Value:  complex(float32(23), float32(31)),
			},
			ExpectedType: reflect.Complex64,
		},
		{
			OnlineResource{
				Entity: "complex128",
				Value:  complex(float64(23), float64(31)),
			},
			ExpectedType: reflect.Complex128,
		},
		{
			OnlineResource{
				Entity: "time",
				Value:  time.Now(),
			},
			ExpectedType: reflect.TypeOf(time.Now()),
		},
		{
			OnlineResource{
				Entity: "time",
				Value:  time.Now(),
			},
			ExpectedType: reflect.TypeOf(time.Now()),
		},
	}
	for _, c := range testCases {
		featureName := uuid.New().String()
		tab, err := store.CreateTable(featureName, "", c.Resource.Type)
		if err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}
		if err := tab.Set(c.Resource.Entity, c.Resource.Value); err != nil {
			t.Fatalf("Failed to set entity: %s", err)
		}
		gotVal, err := tab.Get(c.Resource.Entity)
		if err != nil {
			t.Fatalf("Failed to get entity: %s", err)
		}
		if reflect.TypeOf(c.ExpectedValue) reflect.TypeOf(gotVal) {
			t.Fatalf("Types are not the same %T != %T", c.ExpectedValue, gotVal)
		}
		store.DeleteTable(featureName, "")
	}
}

func testForcedTypeCasting(t *testing.T, store OnlineStore) {
	dummy := struct {
		Something string
		Else      string
	}{"some", "field"}

	testCases := []struct {
		Resource      OnlineResource
		ExpectedType  reflect.Type
		ExpectedValue interface{}
		ShouldError   bool
	}{
		{
			OnlineResource{
				Entity: "int",
				Value:  int(1),
				Type:   Int,
			},
			ExpectedType:  reflect.Int,
			ExpectedValue: 1,
			ShouldError:   false,
		},
		{
			OnlineResource{
				Entity: "int",
				Value:  int(1),
				Type:   String,
			},
			ExpectedType:  reflect.Int,
			ExpectedValue: "1",
			ShouldError:   false,
		},
		{
			OnlineResource{
				Entity: "int8",
				Value:  int8(1),
				Type:   Int,
			},
			ExpectedType:  reflect.Int,
			ExpectedValue: 1,
			ShouldError:   false,
		},
		{
			OnlineResource{
				Entity: "int16",
				Value:  int16(1),
				Type:   Int,
			},
			ExpectedType:  reflect.Int,
			ExpectedValue: 1,
			ShouldError:   false,
		},
		{
			OnlineResource{
				Entity: "int32",
				Value:  int32(1),
				Type:   Int,
			},
			ExpectedType:  reflect.Int,
			ExpectedValue: 1,
			ShouldError:   false,
		},
		{
			OnlineResource{
				Entity: "int64",
				Value:  int64(1),
				Type:   Int,
			},
			ExpectedType:  reflect.Int,
			ExpectedValue: 1,
			ShouldError:   false,
		},
		{
			OnlineResource{
				Entity: "float32",
				Value:  float32(1.2),
				Type:   Int,
			},
			ExpectedType:  reflect.Int,
			ExpectedValue: 1,
			ShouldError:   false,
		},
		{
			OnlineResource{
				Entity: "float64",
				Value:  float64(1.2),
				Type:   Int,
			},
			ExpectedType:  reflect.Int,
			ExpectedValue: 1,
			ShouldError:   false,
		},
		{
			OnlineResource{
				Entity: "string",
				Value:  "somestring",
				Type:   Int,
			},
			ExpectedType: reflect.String,
			ShouldError:  true,
		},
		{
			OnlineResource{
				Entity: "bool",
				Value:  true,
				Type:   Int,
			},
			ExpectedType:  reflect.Bool,
			ExpectedValue: 1,
			ShouldError:   false,
		},
		{
			OnlineResource{
				Entity: "complex64",
				Value:  complex(float32(23), float32(31)),
				Type:   Int,
			},
			ExpectedType: reflect.Complex64,
			ShouldError:  true,
		},
		{
			OnlineResource{
				Entity: "complex128",
				Value:  complex(float64(23), float64(31)),
				Type:   Int,
			},
			ExpectedType: reflect.Complex128,
			ShouldError:  false,
		},
		{
			OnlineResource{
				Entity: "time",
				Value:  time.Now(),
				Type:   Int,
			},
			ExpectedType: reflect.TypeOf(time.Now()),
			ShouldError:  true,
		},
		{
			OnlineResource{
				Entity: "time",
				Value:  time.UnixMicro(0),
				Type:   String,
			},
			ExpectedType:  reflect.TypeOf(time.Now()),
			ExpectedValue: string(time.UnixMicro(0)),
			ShouldError:   false,
		},
		{
			// Test unknown type
			OnlineResource{
				Entity: "time",
				Value:  dummy,
				Type:   String,
			},
			ExpectedType:  reflect.string,
			ExpectedValue: fmt.Sprintf("%v", dummy),
			ShouldError:   false,
		},
	}
	for _, c := range testCases {
		featureName := uuid.New().String()
		tab, err := store.CreateTable(featureName, "", c.Resource.Type)
		if err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}
		err := tab.Set(c.Resource.Entity, c.Resource.Value)
		if err != nil && !c.ShouldError {
			t.Errorf("Unable to set resource value: %v, Type: %v", c.Resource.Value, c.Resource.Type)
		} else if err == nil && c.ShouldError {
			t.Errorf("Invalid value set created with Value Type: %v, Value: %v, Table Type: %v", reflect.TypeOf(c.Resource.Value), c.Resource.Value, c.Resource.Type)
		}
		gotVal, err := tab.Get(c.Resource.Entity)
		if err != nil {
			t.Fatalf("Failed to get entity: %s", err)
		}
		if !reflect.DeepEqual(c.ExpectedValue, gotVal) {
			t.Fatalf("Values are not the same %v, type %T. %v, type %T", c.ExpectedValue, c.ExpectedValue, gotVal, gotVal)
		}
		store.DeleteTable(featureName, "")
	}
}

func testTypeOverride(t *testing.T, store OnlineStore) {
	testCases := []struct {
		Resource    OnlineResource
		ShouldError bool
	}{
		{
			// Test unknown type
			OnlineResource{
				Entity: "a",
				Value:  1,
				Type:   Int,
			},
			ShouldError: false,
		},
		{
			// Test unknown type
			OnlineResource{
				Entity: "b",
				Value:  1,
				Type:   Int32,
			},
			ShouldError: false,
		},
		{
			// Test unknown type
			OnlineResource{
				Entity: "c",
				Value:  1,
				Type:   Int64,
			},
			ShouldError: false,
		},
		{
			// Test unknown type
			OnlineResource{
				Entity: "d",
				Value:  1,
				Type:   Float32,
			},
			ShouldError: false,
		},
		{
			// Test unknown type
			OnlineResource{
				Entity: "e",
				Value:  1,
				Type:   Float64,
			},
			ShouldError: false,
		},
		{
			// Test unknown type
			OnlineResource{
				Entity: "f",
				Value:  true,
				Type:   Bool,
			},
			ShouldError: false,
		},
		{
			// Test unknown type
			OnlineResource{
				Entity: "g",
				Value:  time.Now(),
				Type:   Timestamp,
			},
			ShouldError: false,
		},
		{
			// Test unknown type
			OnlineResource{
				Entity: "h",
				Value:  1,
				Type:   "str",
			},
			ShouldError: true,
		},
		{
			// Test unknown type
			OnlineResource{
				Entity: "i",
				Value:  1,
				Type:   "",
			},
			ShouldError: true,
		},
		{
			// Test unknown type
			OnlineResource{
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
