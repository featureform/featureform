// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
package provider

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"

	pc "github.com/featureform/provider/provider_config"
	"github.com/google/uuid"
)

type OnlineResource struct {
	Entity string
	Value  interface{}
	Type   ValueType
}

type OnlineStoreTest struct {
	t     *testing.T
	store OnlineStore
}

func (test *OnlineStoreTest) Run() {
	t := test.t

	testFns := map[string]func(*testing.T, OnlineStore){
		"CreateGetTable":     testCreateGetTable,
		"TableAlreadyExists": testTableAlreadyExists,
		"TableNotFound":      testTableNotFound,
		"SetGetEntity":       testSetGetEntity,
		"EntityNotFound":     testEntityNotFound,
		"MassTableWrite":     testMassTableWrite,
		"TypeCasting":        testTypeCasting,
	}

	store := test.store
	for name, fn := range testFns {
		testName := fmt.Sprintf("%s_%s", name, store.Type())
		t.Run(testName, func(t *testing.T) {
			fn(t, store)
		})
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Failed to close online store %s: %v", store.Type(), err)
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
		t.Fatalf("Wrong error for table not found: %s, %T", err, err)
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
		tab, err := store.CreateTable(tableList[i].Name, tableList[i].Variant, ScalarType("int"))
		if err != nil {
			t.Fatalf("could not create table %v in online store: %v", tableList[i], err)
		}
		defer store.DeleteTable(tableList[i].Name, tableList[i].Variant)
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
	testConfig := payload["FirestoreConfig"].(map[string]interface{})

	fsconfig := pc.FirestoreConfig{
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
		config pc.SerializedConfig
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
			r := &pc.FirestoreConfig{
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
