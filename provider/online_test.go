// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.

package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/featureform/fferr"
	pc "github.com/featureform/provider/provider_config"
	"github.com/featureform/provider/types"
	"github.com/google/uuid"
)

type OnlineResource struct {
	Entity string
	Value  interface{}
	Type   types.ValueType
}

type OnlineStoreTest struct {
	t     *testing.T
	store OnlineStore
	// TODO(simba) remove once we implement for all providers
	testNil      bool
	testFloatVec bool
	testBatch    bool
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

	if test.testNil {
		testFns["NilValues"] = testNilValues
	}

	if test.testFloatVec {
		testFns["FloatVecValues"] = testFloatVecValues
	}

	if test.testBatch {
		testFns["BatchSetGetEntity"] = testBatchSetGetEntity
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
	// This is done before creating it as sometimes creation partially fails and leaves some resources behind,
	// so we attempt to delete it anyway and ignore the error.
	defer store.DeleteTable(mockFeature, mockVariant)
	if tab, err := store.CreateTable(mockFeature, mockVariant, types.String); tab == nil || err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if tab, err := store.GetTable(mockFeature, mockVariant); tab == nil || err != nil {
		t.Fatalf("Failed to get table: %s", err)
	}
}

func testTableAlreadyExists(t *testing.T, store OnlineStore) {
	mockFeature, mockVariant := randomFeatureVariant()
	defer store.DeleteTable(mockFeature, mockVariant)
	if _, err := store.CreateTable(mockFeature, mockVariant, types.String); err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if _, err := store.CreateTable(mockFeature, mockVariant, types.String); err == nil {
		t.Fatalf("Succeeded in creating table twice")
	} else if casted, valid := err.(*fferr.DatasetAlreadyExistsError); !valid {
		t.Fatalf("Wrong error for table already exists: %T", err)
	} else if casted.Error() == "" {
		t.Fatalf("TableAlreadyExists has empty error message")
	}
}

func testTableNotFound(t *testing.T, store OnlineStore) {
	mockFeature, mockVariant := randomFeatureVariant()
	if _, err := store.GetTable(mockFeature, mockVariant); err == nil {
		t.Fatalf("Succeeded in getting non-existent table")
	} else if casted, valid := err.(*fferr.DatasetNotFoundError); !valid {
		t.Fatalf("Wrong error for table not found: %s, %T", err, err)
	} else if casted.Error() == "" {
		t.Fatalf("TableNotFound has empty error message")
	}
}

func testSetGetEntity(t *testing.T, store OnlineStore) {
	mockFeature, mockVariant := randomFeatureVariant()
	defer store.DeleteTable(mockFeature, mockVariant)
	entity, val := "e", "val"
	tab, err := store.CreateTable(mockFeature, mockVariant, types.String)
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

func testBatchSetGetEntity(t *testing.T, store OnlineStore) {
	mockFeature, mockVariant := randomFeatureVariant()
	defer store.DeleteTable(mockFeature, mockVariant)
	tab, err := store.CreateTable(mockFeature, mockVariant, types.String)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	batchTable, ok := tab.(BatchOnlineTable)
	if !ok {
		t.Fatalf("Table does not implement batch interface.")
	}
	maxNum, err := batchTable.MaxBatchSize()
	if err != nil {
		t.Fatalf("Failed to get max batch size")
	}
	singleEnt := "e"
	singleVal := "val"
	singleSet := []SetItem{{singleEnt, singleVal}}
	if err := batchTable.BatchSet(context.Background(), singleSet); err != nil {
		t.Fatalf("Failed to set single entity: %s", err)
	}
	gotVal, err := tab.Get(singleEnt)
	if err != nil {
		t.Fatalf("Failed to get entity: %s", err)
	}
	if !reflect.DeepEqual(singleVal, gotVal) {
		t.Fatalf("Values are not the same %v %v", singleVal, gotVal)
	}
	maxSet := make([]SetItem, maxNum)
	for i := 0; i < maxNum; i++ {
		entity := fmt.Sprintf("entity_%d", i)
		value := fmt.Sprintf("value_%d", i)
		maxSet[i] = SetItem{entity, value}
	}
	if err := batchTable.BatchSet(context.Background(), maxSet); err != nil {
		t.Fatalf("Failed to set multi entity: %s", err)
	}
	for _, item := range maxSet {
		entity, val := item.Entity, item.Value
		gotVal, err = tab.Get(entity)
		if err != nil {
			t.Fatalf("Failed to get entity: %s", err)
		}
		if !reflect.DeepEqual(val, gotVal) {
			t.Fatalf("Values are not the same %v %v", val, gotVal)
		}
	}
	overSizedSet := append(maxSet, SetItem{"a", "b"})
	if err := batchTable.BatchSet(context.Background(), overSizedSet); err == nil {
		t.Fatalf("Succeeded to batch set over max size")
	}
}

func testEntityNotFound(t *testing.T, store OnlineStore) {
	mockFeature, mockVariant := uuid.NewString(), "v"
	entity := "e"
	defer store.DeleteTable(mockFeature, mockVariant)
	tab, err := store.CreateTable(mockFeature, mockVariant, types.String)
	if err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}
	if _, err := tab.Get(entity); err == nil {
		t.Fatalf("succeeded in getting non-existent entity")
	} else if casted, valid := err.(*fferr.EntityNotFoundError); !valid {
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
		tab, err := store.CreateTable(tableList[i].Name, tableList[i].Variant, types.Int)
		if err != nil {
			t.Fatalf("could not create table %v in online store: %v\n", tableList[i], err)
		}
		defer store.DeleteTable(tableList[i].Name, tableList[i].Variant)
		for j := range entityList {
			if err := tab.Set(entityList[j], 1); err != nil {
				t.Fatalf("could not set entity %v in table %v: %v\n", entityList[j], tableList[i], err)
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

func testNilValues(t *testing.T, store OnlineStore) {
	onlineResources := []OnlineResource{
		{
			Entity: "a",
			Value:  nil,
			Type:   types.Int,
		},
		{
			Entity: "b",
			Value:  nil,
			Type:   types.Int64,
		},
		{
			Entity: "c",
			Value:  nil,
			Type:   types.Float32,
		},
		{
			Entity: "d",
			Value:  nil,
			Type:   types.Float64,
		},
		{
			Entity: "e",
			Value:  nil,
			Type:   types.String,
		},
		{
			Entity: "f",
			Value:  nil,
			Type:   types.Bool,
		},
	}
	for _, resource := range onlineResources {
		featureName, variantName := randomFeatureVariant()
		defer store.DeleteTable(featureName, variantName)
		tab, err := store.CreateTable(featureName, variantName, resource.Type)
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
	}
}

func testFloatVecValues(t *testing.T, store OnlineStore) {
	onlineResources := []OnlineResource{
		{
			Entity: "a",
			Value:  nil,
			Type:   types.VectorType{ScalarType: types.Float32, Dimension: 3, IsEmbedding: true},
		},
		{
			Entity: "b",
			Value:  nil,
			Type:   types.VectorType{ScalarType: types.Float32, Dimension: 3, IsEmbedding: false},
		},
		{
			Entity: "c",
			Value:  []float32{1, 2, 3},
			Type:   types.VectorType{ScalarType: types.Float32, Dimension: 3, IsEmbedding: true},
		},
		{
			Entity: "d",
			Value:  []float32{4, 5, 6},
			Type:   types.VectorType{ScalarType: types.Float32, Dimension: 3, IsEmbedding: false},
		},
	}
	for _, resource := range onlineResources {
		featureName, variantName := randomFeatureVariant()
		defer store.DeleteTable(featureName, variantName)
		tab, err := store.CreateTable(featureName, variantName, resource.Type)
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
	}
}

func testTypeCasting(t *testing.T, store OnlineStore) {
	onlineResources := []OnlineResource{
		{
			Entity: "a",
			Value:  int(1),
			Type:   types.Int,
		},
		{
			Entity: "b",
			Value:  int64(1),
			Type:   types.Int64,
		},
		{
			Entity: "c",
			Value:  float32(1.0),
			Type:   types.Float32,
		},
		{
			Entity: "d",
			Value:  float64(1.0),
			Type:   types.Float64,
		},
		{
			Entity: "e",
			Value:  "1.0",
			Type:   types.String,
		},
		{
			Entity: "f",
			Value:  false,
			Type:   types.Bool,
		},
	}
	for _, resource := range onlineResources {
		featureName, variantName := randomFeatureVariant()
		store.DeleteTable(featureName, variantName)
		tab, err := store.CreateTable(featureName, variantName, resource.Type)
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
