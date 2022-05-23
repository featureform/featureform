package provider

import (
	"fmt"
	"github.com/alicebob/miniredis"
	"github.com/google/uuid"
	"os"
	"reflect"
	"testing"
)

func mockRedis() *miniredis.Miniredis {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	return s
}
func TestOnlineStores(t *testing.T) {
	testFns := map[string]func(*testing.T, OnlineStore){
		"CreateGetTable":     testCreateGetTable,
		"TableAlreadyExists": testTableAlreadyExists,
		"TableNotFound":      testTableNotFound,
		"SetGetEntity":       testSetGetEntity,
		"EntityNotFound":     testEntityNotFound,
	}

	miniRedis := mockRedis()
	defer miniRedis.Close()
	mockRedisAddr := miniRedis.Addr()
	redisMockConfig := &RedisConfig{
		Addr: mockRedisAddr,
	}
	redisPort := os.Getenv("REDIS_PORT")
	liveAddr := fmt.Sprintf("%s:%s", "localhost", redisPort)
	redisLiveConfig := &RedisConfig{
		Addr: liveAddr,
	}
	testList := []struct {
		t               Type
		c               SerializedConfig
		integrationTest bool
	}{
		{LocalOnline, []byte{}, false},
		{RedisOnline, redisMockConfig.Serialized(), false},
		{RedisOnline, redisLiveConfig.Serialized(), true},
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
				t.Fatalf("Failed to use provider %s as OfflineStore: %s", testItem.t, err)
			}
			var prefix string
			if testItem.integrationTest {
				prefix = "INTEGRATION"
			} else {
				prefix = "UNIT"
			}
			testName := fmt.Sprintf("%s_%s_%s", testItem.t, prefix, name)
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
	if tab, err := store.CreateTable(mockFeature, mockVariant); tab == nil || err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if tab, err := store.GetTable(mockFeature, mockVariant); tab == nil || err != nil {
		t.Fatalf("Failed to get table: %s", err)
	}
}

func testTableAlreadyExists(t *testing.T, store OnlineStore) {
	mockFeature, mockVariant := randomFeatureVariant()
	if _, err := store.CreateTable(mockFeature, mockVariant); err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if _, err := store.CreateTable(mockFeature, mockVariant); err == nil {
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
		t.Fatalf("Succeeded in getting non-existant table")
	} else if casted, valid := err.(*TableNotFound); !valid {
		t.Fatalf("Wrong error for table not found: %T", err)
	} else if casted.Error() == "" {
		t.Fatalf("TableNotFound has empty error message")
	}
}

func testSetGetEntity(t *testing.T, store OnlineStore) {
	mockFeature, mockVariant := randomFeatureVariant()
	entity, val := "e", "val"
	tab, err := store.CreateTable(mockFeature, mockVariant)
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
	tab, err := store.CreateTable(mockFeature, mockVariant)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if _, err := tab.Get(entity); err == nil {
		t.Fatalf("succeeded in getting non-existant entity")
	} else if casted, valid := err.(*EntityNotFound); !valid {
		t.Fatalf("Wrong error for entity not found: %T", err)
	} else if casted.Error() == "" {
		t.Fatalf("EntityNotFound has empty error message")
	}
}
