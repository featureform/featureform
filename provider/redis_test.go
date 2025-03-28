// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/joho/godotenv"

	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"

	"github.com/redis/rueidis"
)

func mockRedis() *miniredis.Miniredis {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	return s
}

func TestOnlineStoreRedisMock(t *testing.T) {
	mRedis := mockRedis()
	defer mRedis.Close()
	mockRedisAddr := mRedis.Addr()
	redisMockConfig := &pc.RedisConfig{
		Addr: mockRedisAddr,
	}

	store, err := GetOnlineStore(pt.RedisOnline, redisMockConfig.Serialized())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OnlineStoreTest{
		t:     t,
		store: store,
		// TODO(simba) make this work.
		testNil: false,
	}
	test.Run()
}

func TestOnlineStoreRedisInsecure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	redisInsecurePort, ok := os.LookupEnv("REDIS_INSECURE_PORT")
	if !ok {
		t.Fatalf("missing REDIS_INSECURE_PORT variable")
	}
	insecureAddr := fmt.Sprintf("%s:%s", "localhost", redisInsecurePort)
	redisInsecureConfig := &pc.RedisConfig{
		Addr: insecureAddr,
	}

	store, err := GetOnlineStore(pt.RedisOnline, redisInsecureConfig.Serialized())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OnlineStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
}

func TestOnlineStoreRedisSecure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	redisSecurePort, ok := os.LookupEnv("REDIS_SECURE_PORT")
	if !ok {
		t.Fatalf("missing REDIS_SECURE_PORT variable")
	}
	redisPassword, ok := os.LookupEnv("REDIS_PASSWORD")
	if !ok {
		t.Fatalf("missing REDIS_PASSWORD variable")
	}

	secureAddr := fmt.Sprintf("%s:%s", "localhost", redisSecurePort)
	redisSecureConfig := &pc.RedisConfig{
		Addr:     secureAddr,
		Password: redisPassword,
	}

	store, err := GetOnlineStore(pt.RedisOnline, redisSecureConfig.Serialized())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OnlineStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
}

func TestVectorStoreRedis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}
	redisInsecurePort, ok := os.LookupEnv("REDIS_INSECURE_PORT")
	if !ok {
		t.Fatalf("missing REDIS_INSECURE_PORT variable")
	}
	insecureAddr := fmt.Sprintf("%s:%s", "localhost", redisInsecurePort)
	redisInsecureConfig := &pc.RedisConfig{
		Addr: insecureAddr,
	}

	store, err := GetOnlineStore(pt.RedisOnline, redisInsecureConfig.Serialized())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := VectorStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
}

func Test_redisOnlineTable_Get(t *testing.T) {
	miniRedis := mockRedis()
	redisClient, err := instantiateMockRedisClient(miniRedis.Addr())
	if err != nil {
		t.Fatalf("Failed to create redis client: %v", err)
	}
	type fields struct {
		client    rueidis.Client
		key       redisTableKey
		valueType types.ValueType
	}
	type args struct {
		entity string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		given   interface{}
		want    interface{}
		wantErr bool
	}{
		{"String Success", fields{redisClient, redisTableKey{}, types.String}, args{"entity1"}, "somestring", "somestring", false},
		{"Int Success", fields{redisClient, redisTableKey{}, types.Int}, args{"entity2"}, 1, 1, false},
		{"Int32 Success", fields{redisClient, redisTableKey{}, types.Int32}, args{"entity3"}, 1, int32(1), false},
		{"Int64 Success", fields{redisClient, redisTableKey{}, types.Int64}, args{"entity4"}, 1, int64(1), false},
		{"Float32 Success", fields{redisClient, redisTableKey{}, types.Float32}, args{"entity5"}, 1, float32(1), false},
		{"Float64 Success", fields{redisClient, redisTableKey{}, types.Float64}, args{"entity6"}, 1, float64(1), false},
		{"Bool Success", fields{redisClient, redisTableKey{}, types.Bool}, args{"entity7"}, true, true, false},
		{"Timestamp Success", fields{redisClient, redisTableKey{}, types.Timestamp}, args{"entity8"}, time.UnixMilli(0), time.UnixMilli(0).Local(), false},
		{
			"Vector32 Success",
			fields{
				redisClient,
				redisTableKey{},
				types.VectorType{
					ScalarType: types.Float32,
					Dimension:  5,
				},
			},
			args{"entity9"},
			[]float32{0.08067775, 0.0012904393, 0.14408082, -0.028135499, 0.076197624},
			[]float32{0.08067775, 0.0012904393, 0.14408082, -0.028135499, 0.076197624},
			false,
		},
		// These will allow any previously created tables with incorrect valueTypes to be called as a string
		// if the valueType is not recognized
		{"String Default", fields{redisClient, redisTableKey{}, types.ScalarType("Invalid")}, args{"entity9"}, "somestring", "somestring", false},
		{"Int Default", fields{redisClient, redisTableKey{}, types.ScalarType("Invalid")}, args{"entity10"}, 1, fmt.Sprintf("%d", 1), false},
		{"Int32 Default", fields{redisClient, redisTableKey{}, types.ScalarType("Invalid")}, args{"entity11"}, 1, fmt.Sprintf("%d", 1), false},
		{"Int64 Default", fields{redisClient, redisTableKey{}, types.ScalarType("Invalid")}, args{"entity12"}, 1, fmt.Sprintf("%d", 1), false},
		{"Float32 Default", fields{redisClient, redisTableKey{}, types.ScalarType("Invalid")}, args{"entity13"}, 1, fmt.Sprintf("%d", 1), false},
		{"Float64 Default", fields{redisClient, redisTableKey{}, types.ScalarType("Invalid")}, args{"entity14"}, 1, fmt.Sprintf("%d", 1), false},
		{"Bool Default", fields{redisClient, redisTableKey{}, types.ScalarType("Invalid")}, args{"entity15"}, true, fmt.Sprintf("%d", 1), false},
		{"Timestamp Default", fields{redisClient, redisTableKey{}, types.ScalarType("Invalid")}, args{"entity16"}, time.UnixMilli(0), time.UnixMilli(0).Format(time.RFC3339), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := redisOnlineTable{
				client:    tt.fields.client,
				key:       tt.fields.key,
				valueType: tt.fields.valueType,
			}
			err := table.Set(tt.args.entity, tt.given)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got, err := table.Get(tt.args.entity)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Comparing time.Time using reflect results in comparing the underlying time.Time struct
			// which won't be equal if the timestamps are created on different machines; instead, we
			// compare the string representations of the timestamps
			if reflect.TypeOf(got) == reflect.TypeOf(time.Time{}) {
				if !(tt.want).(time.Time).Equal(got.(time.Time)) {
					t.Errorf("Get() got = %v (%T), want %v (%T)", got, got, tt.want, tt.want)
				}
			} else {
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("Get() got = %v (%T), want %v (%T)", got, got, tt.want, tt.want)
				}
			}
		})
	}
}

func TestGetTableBackwardsCompatibility(t *testing.T) {
	miniRedis := mockRedis()
	redisClient, err := instantiateMockRedisClient(miniRedis.Addr())
	if err != nil {
		t.Fatalf("Failed to create redis client: %v", err)
	}
	redisConfig := pc.RedisConfig{
		Addr:     miniRedis.Addr(),
		Password: "",
		DB:       0,
	}
	prefix := "Featureform_table__"
	redisOnlineStore := redisOnlineStore{
		redisClient,
		prefix,
		BaseProvider{ProviderType: pt.RedisOnline, ProviderConfig: redisConfig.Serialized()},
	}
	if err != nil {
		t.Fatalf("Failed to create redis online store: %v", err)
	}
	// Arrange - Create "Featureform_table____tables" hash to simulate
	// in user's Redis instance existing "metadata" table
	scalarTypes := []types.ScalarType{types.String, types.Int, types.Int32, types.Int64, types.Float32, types.Float64, types.Bool, types.Timestamp}
	for _, scalarType := range scalarTypes {
		// The below represents the implementation of CreateTable prior to introducing the
		// JSON serialized value type as the field value of the tables hash
		key := redisTableKey{prefix, fmt.Sprintf("feature_%s", string(scalarType)), "v"}
		cmd := redisClient.B().
			Hset().
			Key(fmt.Sprintf("%s__tables", prefix)).
			FieldValue().
			FieldValue(key.String(), string(scalarType)).
			Build()
		resp := redisClient.Do(context.TODO(), cmd)
		if resp.Error() != nil {
			t.Fatalf("Failed to create table: %v", resp.Error())
		}
	}
	// Act - Get table
	for _, scalarType := range scalarTypes {
		onlineStoreTable, err := redisOnlineStore.GetTable(fmt.Sprintf("feature_%s", string(scalarType)), "v")
		if err != nil {
			t.Fatalf("Failed to get table: %v", err)
		}
		if reflect.TypeOf(onlineStoreTable) != reflect.TypeOf(&redisOnlineTable{}) {
			t.Fatalf("Expected onlineStoreTable to be redisOnlineTable but received: %T", onlineStoreTable)
		}
		tbl := onlineStoreTable.(*redisOnlineTable)
		if tbl.valueType != scalarType {
			t.Fatalf("Expected valueType to be %s but received: %s", scalarType, tbl.valueType)
		}
		if tbl.valueType.IsVector() {
			t.Fatalf("Expected valueType to be scalar but received: %s", tbl.valueType)
		}
	}
}

func TestCreateGetTable(t *testing.T) {
	miniRedis := mockRedis()
	redisClient, err := instantiateMockRedisClient(miniRedis.Addr())
	if err != nil {
		t.Fatalf("Failed to create redis client: %v", err)
	}
	redisConfig := pc.RedisConfig{
		Addr:     miniRedis.Addr(),
		Password: "",
		DB:       0,
	}
	prefix := "Featureform_table__"
	redisOnlineStore := redisOnlineStore{
		redisClient,
		prefix,
		BaseProvider{ProviderType: pt.RedisOnline, ProviderConfig: redisConfig.Serialized()},
	}
	if err != nil {
		t.Fatalf("Failed to create redis online store: %v", err)
	}
	// Arrange - Create tables
	scalarTypes := []types.ScalarType{types.String, types.Int, types.Int32, types.Int64, types.Float32, types.Float64, types.Bool, types.Timestamp}
	for _, scalarType := range scalarTypes {
		var valueType types.ValueType
		if scalarType == types.Float32 {
			valueType = types.VectorType{ScalarType: scalarType, Dimension: 384, IsEmbedding: true}
		} else {
			valueType = scalarType
		}
		_, err := redisOnlineStore.CreateTable(fmt.Sprintf("feature_%s", string(scalarType)), "v", valueType)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}
	}
	// Act - Get table
	for _, scalarType := range scalarTypes {
		onlineStoreTable, err := redisOnlineStore.GetTable(fmt.Sprintf("feature_%s", string(scalarType)), "v")
		if err != nil {
			t.Fatalf("Failed to get table: %v", err)
		}
		if scalarType == types.Float32 {
			if reflect.TypeOf(onlineStoreTable) != reflect.TypeOf(&redisOnlineIndex{}) {
				t.Fatalf("Expected onlineStoreTable to be redisOnlineIndex but received: %T", onlineStoreTable)
			}
			tbl := onlineStoreTable.(*redisOnlineIndex)
			if !tbl.valueType.IsVector() {
				t.Fatalf("Expected onlineStoreTable to be embedding but received: %v", tbl.valueType.IsVector())
			}
			if reflect.TypeOf(tbl.valueType) != reflect.TypeOf(types.VectorType{}) {
				t.Fatalf("Expected onlineStoreTable to be VectorType but received: %T", tbl.valueType)
			}
			if !tbl.valueType.(types.VectorType).IsEmbedding {
				t.Fatalf("Expected onlineStoreTable to be embedding but received: %v", tbl.valueType.(types.VectorType).IsEmbedding)
			}
		} else {
			if reflect.TypeOf(onlineStoreTable) != reflect.TypeOf(&redisOnlineTable{}) {
				t.Fatalf("Expected onlineStoreTable to be redisOnlineTable but received: %T", onlineStoreTable)
			}
			tbl := onlineStoreTable.(*redisOnlineTable)
			if tbl.valueType.IsVector() {
				t.Fatalf("Expected onlineStoreTable to not be embedding but received: %v", tbl.valueType.IsVector())
			}
			if reflect.TypeOf(tbl.valueType) != reflect.TypeOf(scalarType) {
				t.Fatalf("Expected onlineStoreTable to be %v but received: %T", scalarType, tbl.valueType)
			}
		}
	}
}

func instantiateMockRedisClient(addr string) (rueidis.Client, error) {
	return rueidis.NewClient(
		rueidis.ClientOption{
			InitAddress: []string{addr},
			Password:    "",
			SelectDB:    0,
			// Miniredis does not support certain commands used by RediSearch, and given
			// rueidis supports these, there are certain configurations that need to be be
			// set to avoid errors, such as the following:
			// ````
			// unknown command `CLIENT`, with args beginning with: `TRACKING`, `ON`, `OPTIN`, :
			// ClientOption.DisableCache must be true for redis not supporting client-side caching or not supporting RESP3
			// ```
			DisableCache: true,
		},
	)
}
