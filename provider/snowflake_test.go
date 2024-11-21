// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/featureform/metadata"
	"github.com/featureform/provider/location"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/types"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
)

const (
	floatRoundingDecimals   = 2
	transactionsEntityIdx   = 0
	transactionsValueIdx    = 1
	transactionTimestampIdx = 2
	transactionLabelIdx     = 3
	// SHOW DYNAMIC TABLES returns 21 columns; however, we're only concerned with the 11th column which is the warehouse.
	showDynamicTableColCount        = 21
	showDynamicTableWarehouseColIdx = 11
)

type snowflakeOfflineStoreTester struct {
	*snowflakeOfflineStore
}

func (s *snowflakeOfflineStoreTester) CreateDatabase(name string) error {
	db, err := s.sqlOfflineStore.getDb("", "")
	if err != nil {
		return err
	}
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + sanitize(name))
	return err
}

func (s *snowflakeOfflineStoreTester) DropDatabase(name string) error {
	db, err := s.sqlOfflineStore.getDb("", "")
	if err != nil {
		return err
	}
	query := "DROP DATABASE IF EXISTS " + sanitize(name)
	_, err = db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func (s *snowflakeOfflineStoreTester) CreateSchema(database, schema string) error {
	db, err := s.sqlOfflineStore.getDb(database, "")
	if err != nil {
		return err
	}
	query := "CREATE SCHEMA IF NOT EXISTS " + sanitize(schema)
	_, err = db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func (s *snowflakeOfflineStoreTester) CreateTable(loc location.Location, schema TableSchema) (PrimaryTable, error) {
	sqlLocation, ok := loc.(*location.SQLLocation)
	if !ok {
		return nil, fmt.Errorf("invalid location type")
	}

	db, err := s.sqlOfflineStore.getDb(sqlLocation.GetDatabase(), sqlLocation.GetSchema())
	if err != nil {
		return nil, err
	}

	// don't need string builder here
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", sqlLocation.TableLocation().String()))

	// do strings .join after
	for i, column := range schema.Columns {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}
		columnType, err := s.sqlOfflineStore.query.determineColumnType(column.ValueType)
		if err != nil {
			return nil, err
		}
		queryBuilder.WriteString(fmt.Sprintf("%s %s", column.Name, columnType))
	}
	queryBuilder.WriteString(")")

	query := queryBuilder.String()
	_, err = db.Exec(query)
	if err != nil {
		return nil, err
	}

	return &snowflakePrimaryTable{
		sqlPrimaryTable{
			db:           db,
			name:         sqlLocation.Location(),
			sqlLocation:  sqlLocation,
			query:        s.sqlOfflineStore.query,
			schema:       schema,
			providerType: s.sqlOfflineStore.ProviderType,
		},
	}, nil
}

func (s *snowflakeOfflineStoreTester) PollTableRefresh(id ResourceID, retryInterval time.Duration, timeout time.Duration) error {
	if err := id.check(Feature); err != nil {
		return err
	}
	tableName, err := ps.ResourceToTableName(FeatureMaterialization.String(), id.Name, id.Variant)
	if err != nil {
		return err
	}
	db, err := s.sqlOfflineStore.getDb("", "")
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`SELECT count(*) FROM  TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(NAME => '%s')) WHERE REFRESH_TRIGGER != 'CREATION' AND REFRESH_ACTION != 'NO_DATA' AND STATE = 'SUCCEEDED' ORDER BY REFRESH_END_TIME DESC;`, sanitize(tableName))
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("polling timed out after %v", timeout)
		case <-ticker.C:
			count := int64(0)
			r := db.QueryRow(query)
			if err := r.Scan(&count); err != nil {
				return err
			}
			if count == 1 {
				return nil
			}
		}
	}
}

func (s *snowflakeOfflineStoreTester) CheckWarehouse(id ResourceID, expectedWh string) (bool, error) {
	db, err := s.sqlOfflineStore.getDb("", "")
	if err != nil {
		return false, err
	}

	tableName, err := ps.ResourceToTableName(FeatureMaterialization.String(), id.Name, id.Variant)
	if err != nil {
		return false, err
	}

	query := fmt.Sprintf("SHOW DYNAMIC TABLES LIKE '%s'", tableName)

	r := db.QueryRow(query)

	var warehouse string
	// SHOW DYNAMIC TABLES returns 21 columns; however, we're only concerned with the 11th column which is the warehouse.
	// To satisfy the scan function, we need to create a slice of 21 interfaces with the 11th element being the warehouse.
	var placeholders [showDynamicTableColCount]interface{}

	// Assign the warehouse to the 11th element of the slice
	placeholders[showDynamicTableWarehouseColIdx] = &warehouse

	for i := 0; i < len(placeholders); i++ {
		// Skip the 11th element since we've already assigned it
		if i == showDynamicTableWarehouseColIdx {
			continue
		}
		placeholders[i] = new(interface{})
	}

	if err := r.Scan(placeholders[:]...); err != nil {
		if err == sql.ErrNoRows {
			// Handle the case where no rows were returned
			fmt.Println("No dynamic tables found with the specified name.")
			return false, nil
		}
		return false, err
	}

	return warehouse == expectedWh, nil
}

// TESTS

func TestSnowflakeTransformations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	tester := getConfiguredTester(t, false)

	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		"RegisterTransformationOnPrimaryInDifferentDatabaseTest": RegisterTransformationOnPrimaryInDifferentDatabaseTest,
		"RegisterChainedTransformationsTest":                     RegisterChainedTransformationsTest,
	}

	for name, testCase := range testCases {
		constName := name
		constTestCase := testCase
		t.Run(constName, func(t *testing.T) {
			t.Parallel()
			constTestCase(t, tester)
		})
	}
}

func TestSnowflakeMaterializations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	tester := getConfiguredTester(t, false)

	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		"RegisterMaterializationNoDuplicatesWithTimestampTest":    RegisterMaterializationNoDuplicatesWithTimestampTest,
		"RegisterMaterializationWithDefaultTargetLagTest":         RegisterMaterializationWithDefaultTargetLagTest,
		"RegisterMaterializationDuplicateEntitiesNoTimestampTest": RegisterMaterializationDuplicateEntitiesNoTimestampTest,
		"RegisterMaterializationDuplicateEntitiesTimestampTest":   RegisterMaterializationDuplicateEntitiesTimestampTest,
		"RegisterMaterializationWithDifferentWarehouseTest":       RegisterMaterializationWithDifferentWarehouseTest,
	}

	for name, testCase := range testCases {
		constName := name
		constTestCase := testCase
		t.Run(constName, func(t *testing.T) {
			t.Parallel()
			constTestCase(t, tester)
		})
	}
}

func TestSnowflakeTrainingSets(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	tester := getConfiguredTester(t, false)

	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		"RegisterTrainingSetOnMaterializationTest":    RegisterTrainingSetOnMaterializationTest,
		"RegisterTrainingSetWithDefaultTargetLagTest": RegisterTrainingSetWithDefaultTargetLagTest,
	}

	for name, testCase := range testCases {
		constName := name
		constTestCase := testCase
		t.Run(constName, func(t *testing.T) {
			t.Parallel()
			constTestCase(t, tester)
		})
	}
}

func TestSnowflakeSchemas(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	tester := getConfiguredTester(t, true)

	testCases := map[string]func(t *testing.T, storeTester offlineSqlTest){
		"RegisterTableInDifferentDatabaseTest":           RegisterTableInDifferentDatabaseTest,
		"RegisterTableInSameDatabaseDifferentSchemaTest": RegisterTableInSameDatabaseDifferentSchemaTest,
		"RegisterTwoTablesInSameSchemaTest":              RegisterTwoTablesInSameSchemaTest,
		"CrossDatabaseJoinTest":                          CrossDatabaseJoinTest,
	}

	for name, testCase := range testCases {
		constName := name
		constTestCase := testCase
		t.Run(constName, func(t *testing.T) {
			t.Parallel()
			constTestCase(t, tester)
		})
	}
}

func TestSnowflakeConfigHasLegacyCredentials(t *testing.T) {
	type fields struct {
		Username       string
		Password       string
		AccountLocator string
		Organization   string
		Account        string
		Database       string
		Schema         string
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{"Empty", fields{}, false},
		{"Has Legacy", fields{AccountLocator: "abcdefg"}, true},
		{"Has Current", fields{Account: "account", Organization: "organization"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sf := &pc.SnowflakeConfig{
				Username:       tt.fields.Username,
				Password:       tt.fields.Password,
				AccountLocator: tt.fields.AccountLocator,
				Organization:   tt.fields.Organization,
				Account:        tt.fields.Account,
				Database:       tt.fields.Database,
				Schema:         tt.fields.Schema,
			}
			if got := sf.HasLegacyCredentials(); got != tt.want {
				t.Errorf("HasLegacyCredentials() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSnowflakeConfigHasCurrentCredentials(t *testing.T) {
	type fields struct {
		Username       string
		Password       string
		AccountLocator string
		Organization   string
		Account        string
		Database       string
		Schema         string
	}
	tests := []struct {
		name    string
		fields  fields
		want    bool
		wantErr bool
	}{
		{"Empty", fields{}, false, false},
		{"Has Legacy", fields{AccountLocator: "abcdefg"}, false, false},
		{"Has Current", fields{Account: "account", Organization: "organization"}, true, false},
		{"Only Account", fields{Account: "account"}, false, true},
		{"Only Organization", fields{Organization: "organization"}, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sf := &pc.SnowflakeConfig{
				Username:       tt.fields.Username,
				Password:       tt.fields.Password,
				AccountLocator: tt.fields.AccountLocator,
				Organization:   tt.fields.Organization,
				Account:        tt.fields.Account,
				Database:       tt.fields.Database,
				Schema:         tt.fields.Schema,
			}
			got, err := sf.HasCurrentCredentials()
			if (err != nil) != tt.wantErr {
				t.Errorf("HasCurrentCredentials() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("HasCurrentCredentials() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSnowflakeConfigConnectionString(t *testing.T) {
	type fields struct {
		Username       string
		Password       string
		AccountLocator string
		Organization   string
		Account        string
		Database       string
		Schema         string
		Role           string
		Warehouse      string
		SessionParams  map[string]string
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		{"Empty", fields{}, "", true},
		{
			"Has Legacy",
			fields{Username: "u", Password: "p", AccountLocator: "accountlocator", Database: "d", Schema: "s"},
			"u:p@accountlocator/d/s",
			false,
		},
		{
			"Has Current",
			fields{Username: "u", Password: "p", Account: "account", Organization: "org", Database: "d", Schema: "s"},
			"u:p@org-account/d/s",
			false,
		},
		{
			"Has Role Parameter",
			fields{Username: "u", Password: "p", Account: "account", Organization: "org", Database: "d", Schema: "s", Role: "myrole"},
			"u:p@org-account/d/s?role=myrole",
			false,
		},
		{
			"Has Warehouse Parameter",
			fields{Username: "u", Password: "p", Account: "account", Organization: "org", Database: "d", Schema: "s", Warehouse: "wh"},
			"u:p@org-account/d/s?warehouse=wh",
			false,
		},
		{
			"Has Warehouse and Role Parameter",
			fields{Username: "u", Password: "p", Account: "account", Organization: "org", Database: "d", Schema: "s", Warehouse: "wh", Role: "myrole"},
			"u:p@org-account/d/s?warehouse=wh&role=myrole",
			false,
		},
		{
			"Only Account",
			fields{Username: "u", Password: "p", Account: "account", Database: "d", Schema: "s"},
			"",
			true,
		},
		{
			"Only Organization",
			fields{Username: "u", Password: "p", Organization: "org", Database: "d", Schema: "s"},
			"",
			true,
		},
		{
			"Both Current And Legacy",
			fields{Username: "u", Password: "p", Account: "account", Organization: "org", AccountLocator: "accountlocator", Database: "d", Schema: "s"},
			"",
			true,
		},
		{
			"Neither Current Nor Legacy",
			fields{Username: "u", Password: "p", Database: "d", Schema: "s"},
			"",
			true,
		},
		{
			"Has Session Params",
			fields{Username: "u", Password: "p", Account: "account", Organization: "org", Database: "d", Schema: "s", SessionParams: map[string]string{"query_tag": "t"}},
			"u:p@org-account/d/s?query_tag=t",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sf := &pc.SnowflakeConfig{
				Username:       tt.fields.Username,
				Password:       tt.fields.Password,
				AccountLocator: tt.fields.AccountLocator,
				Organization:   tt.fields.Organization,
				Account:        tt.fields.Account,
				Database:       tt.fields.Database,
				Schema:         tt.fields.Schema,
				Role:           tt.fields.Role,
				Warehouse:      tt.fields.Warehouse,
				SessionParams:  tt.fields.SessionParams,
			}
			got, err := sf.ConnectionString(tt.fields.Database, tt.fields.Schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConnectionString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ConnectionString() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSnowflakeDeserializeCurrentCredentials(t *testing.T) {
	expected := pc.SnowflakeConfig{
		Username:      "username",
		Password:      "password",
		Organization:  "org",
		Account:       "account",
		Database:      "database",
		Schema:        "schema",
		SessionParams: map[string]string{"query_tag": "t"},
	}
	credentialsMap := make(map[string]interface{})
	credentialsMap["Username"] = expected.Username
	credentialsMap["Password"] = expected.Password
	credentialsMap["Organization"] = expected.Organization
	credentialsMap["Account"] = expected.Account
	credentialsMap["Database"] = expected.Database
	credentialsMap["Schema"] = expected.Schema
	credentialsMap["SessionParams"] = expected.SessionParams
	b, err := json.Marshal(credentialsMap)
	if err != nil {
		t.Fatalf("could not marshal test data: %s", err.Error())
	}
	config := pc.SnowflakeConfig{}
	if err := config.Deserialize(pc.SerializedConfig(b)); err != nil {
		t.Fatalf("could not deserialize config: %s", err.Error())
	}
	if !reflect.DeepEqual(expected, config) {
		t.Fatalf("Expected: %v, Got %v", expected, config)
	}
}

func TestSnowflakeDeserializeLegacyCredentials(t *testing.T) {
	expected := pc.SnowflakeConfig{
		Username:       "username",
		Password:       "password",
		AccountLocator: "accountlocator",
		Database:       "database",
		Schema:         "schema",
	}
	credentialsMap := make(map[string]string)
	credentialsMap["Username"] = expected.Username
	credentialsMap["Password"] = expected.Password
	credentialsMap["AccountLocator"] = expected.AccountLocator
	credentialsMap["Database"] = expected.Database
	credentialsMap["Schema"] = expected.Schema
	b, err := json.Marshal(credentialsMap)
	if err != nil {
		t.Fatalf("could not marshal test data: %s", err.Error())
	}
	config := pc.SnowflakeConfig{}
	if err := config.Deserialize(pc.SerializedConfig(b)); err != nil {
		t.Fatalf("could not deserialize config: %s", err.Error())
	}
	if !reflect.DeepEqual(expected, config) {
		t.Fatalf("Expected: %v, Got %v", expected, config)
	}
}

// TEST FUNCTION

func CrossDatabaseJoinTest(t *testing.T, tester offlineSqlTest) {
	if !tester.testCrossDbJoins {
		t.Skip("skipping cross database join test")
	}

	dbName := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	t.Logf("Database Name1: %s\n", dbName)
	if err := tester.storeTester.CreateDatabase(dbName); err != nil {
		t.Fatalf("could not create database: %v", err)
	}

	dbName2 := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	t.Logf("Database Name2: %s\n", dbName2)
	if err := tester.storeTester.CreateDatabase(dbName2); err != nil {
		t.Fatalf("could not create database: %v", err)
	}

	t.Cleanup(func() {
		if err := tester.storeTester.DropDatabase(dbName); err != nil {
			t.Fatalf("could not drop database: %v", err)
		}
		if err := tester.storeTester.DropDatabase(dbName2); err != nil {
			t.Fatalf("could not drop database: %v", err)
		}
	})

	tableName1 := "DUMMY_TABLE"
	sqlLocation := location.NewSQLLocationWithDBSchemaTable(dbName, "PUBLIC", tableName1).(*location.SQLLocation)
	records, err := createDummyTable(tester.storeTester, *sqlLocation, 3)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	tableName2 := "DUMMY_TABLE2"
	sqlLocation2 := location.NewSQLLocationWithDBSchemaTable(dbName2, "PUBLIC", tableName2).(*location.SQLLocation)
	records2, err := createDummyTable(tester.storeTester, *sqlLocation2, 10)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	// Register the tables
	primary1, primaryErr := tester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: tableName1, Variant: "test", Type: Primary},
		sqlLocation,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	primary2, primaryErr := tester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: tableName2, Variant: "test", Type: Primary},
		sqlLocation2,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	// Verify the table contents
	verifyPrimaryTable(t, primary1, records)
	verifyPrimaryTable(t, primary2, records2)

	targetTableId := ResourceID{Name: "DUMMY_TABLE_TF", Variant: "test", Type: Transformation}
	// union the tables using a transformation
	tfConfig := TransformationConfig{
		Type:          SQLTransformation,
		TargetTableID: targetTableId,
		Query:         fmt.Sprintf("SELECT NAME FROM %s UNION SELECT NAME FROM %s", sqlLocation.TableLocation().String(), sqlLocation2.TableLocation().String()),
	}

	err = tester.storeTester.CreateTransformation(tfConfig)
	if err != nil {
		t.Fatalf("could not create transformation: %v", err)
	}

	// Verify the union table contents
	tfTable, err := tester.storeTester.GetTransformationTable(targetTableId)
	if err != nil {
		t.Fatalf("could not get transformation table: %v", err)
	}

	numRows, err := tfTable.NumRows()
	if err != nil {
		t.Fatalf("could not get number of rows: %v", err)
	}

	assert.Equal(t, int64(13), numRows, "expected 13 rows")
}

func RegisterTwoTablesInSameSchemaTest(t *testing.T, tester offlineSqlTest) {
	schemaName1 := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	schemaName2 := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	if err := tester.storeTester.CreateSchema("", schemaName1); err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	if err := tester.storeTester.CreateSchema("", schemaName2); err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	// Create the first table
	tableName := "DUMMY_TABLE"
	sqlLocation := location.NewSQLLocationWithDBSchemaTable("", schemaName1, tableName).(*location.SQLLocation)
	records, err := createDummyTable(tester.storeTester, *sqlLocation, 3)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	// Create the second table using the same table name
	sqlLocation2 := location.NewSQLLocationWithDBSchemaTable("", schemaName2, tableName).(*location.SQLLocation)
	records2, err := createDummyTable(tester.storeTester, *sqlLocation2, 10)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	primary1, primaryErr := tester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: "tb1", Variant: "test", Type: Primary},
		sqlLocation,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	primary2, primaryErr := tester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: "tb2", Variant: "test", Type: Primary},
		sqlLocation2,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	// Verify the table contents
	verifyPrimaryTable(t, primary1, records)
	verifyPrimaryTable(t, primary2, records2)
}

func RegisterTableInDifferentDatabaseTest(t *testing.T, storeTester offlineSqlTest) {
	dbName := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))

	err := storeTester.storeTester.CreateDatabase(dbName)
	if err != nil {
		t.Fatalf("could not create database: %v", err)
	}

	t.Cleanup(func() {
		if err := storeTester.storeTester.DropDatabase(dbName); err != nil {
			t.Fatalf("could not drop database: %v", err)
		}
	})

	// Create the schema
	schemaName := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	if err := storeTester.storeTester.CreateSchema(dbName, schemaName); err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	// Create the table
	tableName := "DUMMY_TABLE"
	sqlLocation := location.NewSQLLocationWithDBSchemaTable(dbName, schemaName, tableName).(*location.SQLLocation)
	records, err := createDummyTable(storeTester.storeTester, *sqlLocation, 3)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	primary, primaryErr := storeTester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: tableName, Variant: "test", Type: Primary},
		sqlLocation,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	// Verify the table contents
	verifyPrimaryTable(t, primary, records)
}

func RegisterTableInSameDatabaseDifferentSchemaTest(t *testing.T, storeTester offlineSqlTest) {
	schemaName := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	if err := storeTester.storeTester.CreateSchema("", schemaName); err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	// Create the table
	tableName := "DUMMY_TABLE"
	sqlLocation := location.NewSQLLocationWithDBSchemaTable("", schemaName, tableName).(*location.SQLLocation)
	records, err := createDummyTable(storeTester.storeTester, *sqlLocation, 3)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	primary, primaryErr := storeTester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: tableName, Variant: "test", Type: Primary},
		sqlLocation,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	// Verify the table contents
	verifyPrimaryTable(t, primary, records)
}

func RegisterTransformationOnPrimaryInDifferentDatabaseTest(t *testing.T, tester offlineSqlTest) {
	transformationTest := newDatasetSQLTest(tester.storeTester, simpleTransactionsTransformation, 5)
	transformationTest.initDatabaseSchemaTable(t)
	transformationTest.initPrimaryDataset(t)
	transformationTest.createTestTransformation(t, nil)
	transformationTest.assertTransformationDataset(t, 0)
}

func RegisterChainedTransformationsTest(t *testing.T, tester offlineSqlTest) {
	transformationTest := newDatasetSQLTest(tester.storeTester, simpleTransactionsTransformation, 5)
	transformationTest.initDatabaseSchemaTable(t)
	transformationTest.initPrimaryDataset(t)
	transformationTest.createTestTransformation(t, nil)
	transformationTest.assertTransformationDataset(t, 0)
	transformationTest.createChainedTestTransformation(t, nil)
	transformationTest.assertTransformationDataset(t, 1)
}

func RegisterMaterializationNoDuplicatesWithTimestampTest(t *testing.T, tester offlineSqlTest) {
	matTest := newMaterializationSQLTest(tester.storeTester, simpleTransactionsMaterialization, 5)
	matTest.initDatabaseSchemaTable(t)
	matTest.initPrimaryDataset(t)
	matTest.createTestMaterialization(t, nil)
	matTest.assertTestMaterialization(t)
}

func RegisterMaterializationDuplicateEntitiesNoTimestampTest(t *testing.T, tester offlineSqlTest) {
	matTest := newMaterializationSQLTest(tester.storeTester, transactionsDuplicatesNoTSMaterialization, 5)
	matTest.initDatabaseSchemaTable(t)
	matTest.initPrimaryDataset(t)
	matTest.createTestMaterialization(t, nil)
	matTest.assertTestMaterialization(t)
}

func RegisterMaterializationDuplicateEntitiesTimestampTest(t *testing.T, tester offlineSqlTest) {
	matTest := newMaterializationSQLTest(tester.storeTester, transactionsDuplicatesTSMaterialization, 5)
	matTest.initDatabaseSchemaTable(t)
	matTest.initPrimaryDataset(t)
	matTest.createTestMaterialization(t, nil)
	matTest.assertTestMaterialization(t)
}

func RegisterMaterializationWithDefaultTargetLagTest(t *testing.T, tester offlineSqlTest) {
	matTest := newMaterializationSQLTest(tester.storeTester, simpleTransactionsMaterialization, 5)
	matTest.initDatabaseSchemaTable(t)
	matTest.initPrimaryDataset(t)
	opts := &MaterializationOptions{
		ResourceSnowflakeConfig: &metadata.ResourceSnowflakeConfig{
			DynamicTableConfig: &metadata.SnowflakeDynamicTableConfig{
				TargetLag: "90 seconds",
			},
		},
		Schema: ResourceSchema{
			Entity:      matTest.dataset.schema.Columns[entityIdx].Name,
			Value:       matTest.dataset.schema.Columns[valueIdx].Name,
			TS:          matTest.dataset.schema.Columns[tsIdx].Name,
			SourceTable: &matTest.dataset.location,
		},
	}
	matTest.createTestMaterialization(t, opts)
	matTest.assertTestMaterialization(t)
	matTest.appendToDataset(t, 5)

	snowflakeTester, isSnowflakeTester := tester.storeTester.(*snowflakeOfflineStoreTester)
	if !isSnowflakeTester {
		t.Fatalf("expected store tester to be snowflakeOfflineStoreTester")
	}

	if err := snowflakeTester.PollTableRefresh(matTest.featureID, 90*time.Second, 3*time.Minute); err != nil {
		t.Fatalf("expected table to refresh: %v", err)
	}

	matTest.assertTestMaterialization(t)
}

func RegisterMaterializationWithDifferentWarehouseTest(t *testing.T, tester offlineSqlTest) {
	matTest := newMaterializationSQLTest(tester.storeTester, simpleTransactionsMaterialization, 5)
	matTest.initDatabaseSchemaTable(t)
	matTest.initPrimaryDataset(t)
	warehouse := "TEST_WH"
	opts := &MaterializationOptions{
		ResourceSnowflakeConfig: &metadata.ResourceSnowflakeConfig{
			DynamicTableConfig: &metadata.SnowflakeDynamicTableConfig{
				TargetLag: "90 seconds",
			},
			Warehouse: warehouse,
		},
		Schema: ResourceSchema{
			Entity:      matTest.dataset.schema.Columns[entityIdx].Name,
			Value:       matTest.dataset.schema.Columns[valueIdx].Name,
			TS:          matTest.dataset.schema.Columns[tsIdx].Name,
			SourceTable: &matTest.dataset.location,
		},
	}
	matTest.createTestMaterialization(t, opts)

	snowflakeTester, isSnowflakeTester := tester.storeTester.(*snowflakeOfflineStoreTester)
	if !isSnowflakeTester {
		t.Fatalf("expected store tester to be snowflakeOfflineStoreTester")
	}

	usesWarehouse, err := snowflakeTester.CheckWarehouse(matTest.featureID, warehouse)
	if err != nil {
		t.Fatalf("failed to check warehouse: %v", err)
	}
	if !usesWarehouse {
		t.Fatalf("expected materialization to use warehouse %s", warehouse)
	}

	matTest.assertTestMaterialization(t)
}

func RegisterTrainingSetOnMaterializationTest(t *testing.T, tester offlineSqlTest) {
	primary, records := createAndRegisterPrimaryTable(t, tester)

	verifyPrimaryTable(t, primary, records)

	transformationID := createTransformationTable(t, primary, tester)

	transformationTable, err := tester.storeTester.GetTransformationTable(transformationID)
	if err != nil {
		t.Fatalf("could not get transformation table: %v", err)
	}

	// Given the primary table the tests create doesn't have duplicate customer_id values, the transformation table
	// should have the same number of rows as the primary table.
	verifyPrimaryTable(t, transformationTable, records)

	matID, featureID := createMaterialization(t, transformationTable, tester, &metadata.ResourceSnowflakeConfig{})

	mat, err := tester.storeTester.GetMaterialization(matID)
	if err != nil {
		t.Fatalf("could not get materialization: %v", err)
	}

	verifyMaterializationTable(t, mat, records, transactionsEntityIdx, transactionsValueIdx, transactionTimestampIdx)

	sqlPrimaryTbl, isSQLPrimaryTable := primary.(*sqlPrimaryTable)
	if !isSQLPrimaryTable {
		t.Fatalf("expected primary table to be sqlPrimaryTable")
	}

	// Register Label
	labelID, labelLoc := registerLabelResource(t, tester, sqlPrimaryTbl.sqlLocation)

	trainingSetID := createTrainingSet(t, tester, mat, labelID, featureID, labelLoc)

	ts, err := tester.storeTester.GetTrainingSet(trainingSetID)
	if err != nil {
		t.Fatalf("could not get training set: %v", err)
	}

	verifyTrainingSetTable(t, ts, records, transactionsValueIdx, transactionLabelIdx)
}

func RegisterTrainingSetWithDefaultTargetLagTest(t *testing.T, tester offlineSqlTest) {
	primary, records := createAndRegisterPrimaryTable(t, tester)

	verifyPrimaryTable(t, primary, records)

	transformationID := createTransformationTable(t, primary, tester)

	transformationTable, err := tester.storeTester.GetTransformationTable(transformationID)
	if err != nil {
		t.Fatalf("could not get transformation table: %v", err)
	}

	// Given the primary table the tests create doesn't have duplicate customer_id values, the transformation table
	// should have the same number of rows as the primary table.
	verifyPrimaryTable(t, transformationTable, records)

	matID, featureID := createMaterialization(t, transformationTable, tester, &metadata.ResourceSnowflakeConfig{})

	mat, err := tester.storeTester.GetMaterialization(matID)
	if err != nil {
		t.Fatalf("could not get materialization: %v", err)
	}

	verifyMaterializationTable(t, mat, records, transactionsEntityIdx, transactionsValueIdx, transactionTimestampIdx)

	sqlPrimaryTbl, isSQLPrimaryTable := primary.(*sqlPrimaryTable)
	if !isSQLPrimaryTable {
		t.Fatalf("expected primary table to be sqlPrimaryTable")
	}

	// Register Label
	labelID, labelLoc := registerLabelResource(t, tester, sqlPrimaryTbl.sqlLocation)

	trainingSetID := createTrainingSet(t, tester, mat, labelID, featureID, labelLoc)

	additionalRecords := generateTransactionRecords(5)
	if err := primary.WriteBatch(additionalRecords); err != nil {
		t.Fatalf("could not write batch: %v", err)
	}

	t.Log("Waiting 90 seconds for target lag to pass...")
	time.Sleep(90 * time.Second)

	ts, err := tester.storeTester.GetTrainingSet(trainingSetID)
	if err != nil {
		t.Fatalf("could not get training set: %v", err)
	}

	verifyTrainingSetTable(t, ts, append(records, additionalRecords...), transactionsValueIdx, transactionLabelIdx)
}

// HELPER FUNCTIONS

func createDummyTable(storeTester offlineSqlStoreTester, sqlLocation location.SQLLocation, numRows int) ([]GenericRecord, error) {
	// Create the table
	// create simple Schema
	schema := TableSchema{
		Columns: []TableColumn{
			{
				Name:      "ID",
				ValueType: types.Int,
			},
			{
				Name:      "NAME",
				ValueType: types.String,
			},
		},
	}

	primaryTable, err := storeTester.CreateTable(&sqlLocation, schema)
	if err != nil {
		return nil, err
	}

	genericRecords := make([]GenericRecord, 0)
	randomNum := uuid.NewString()[:5]
	for i := 0; i < numRows; i++ {
		genericRecords = append(genericRecords, []interface{}{i, fmt.Sprintf("Name_%d_%s", i, randomNum)})
	}

	if err := primaryTable.WriteBatch(genericRecords); err != nil {
		return nil, err
	}

	return genericRecords, nil
}

func verifyPrimaryTable(t *testing.T, primary PrimaryTable, records []GenericRecord) {
	t.Helper()
	numRows, err := primary.NumRows()
	if err != nil {
		t.Fatalf("could not get number of rows: %v", err)
	}

	if numRows == 0 {
		t.Fatalf("expected more than 0 rows")
	}

	iterator, err := primary.IterateSegment(100)
	if err != nil {
		t.Fatalf("Could not get generic iterator: %v", err)
	}

	i := 0
	for iterator.Next() {
		for j, v := range iterator.Values() {
			// NOTE: we're handling float64 differently hear given the values returned by Snowflake have less precision
			// and therefore are not equal unless we round them; if tests require handling of other types, we can add
			// additional cases here, otherwise the default case will cover all other types
			switch v.(type) {
			case float64:
				assert.Equal(t, roundFloat(v.(float64), floatRoundingDecimals), roundFloat(records[i][j].(float64), floatRoundingDecimals), "expected same values")
			case time.Time:
				assert.Equal(t, records[i][j].(time.Time).Truncate(time.Microsecond), v.(time.Time).Truncate(time.Microsecond), "expected same values")
			default:
				assert.Equal(t, v, records[i][j], "expected same values")
			}
		}
		i++
	}
}

func verifyMaterializationTable(t *testing.T, mat Materialization, records []GenericRecord, entityIdx int, valueIdx int, tsIdx int) {
	t.Helper()
	// We're not concerned with order of records in the materialization table,
	// so to verify the table contents, we'll create a map of the records by
	// entity ID and then iterate over the materialization table to verify
	recordsMap := make(map[string]GenericRecord)
	for _, rec := range records {
		recordsMap[rec[entityIdx].(string)] = rec
	}
	numRows, err := mat.NumRows()
	if err != nil {
		t.Fatalf("could not get number of rows: %v", err)
	}

	assert.Equal(t, len(records), int(numRows), "expected same number of rows")

	itr, err := mat.IterateSegment(0, 100)
	if err != nil {
		t.Fatalf("could not get iterator: %v", err)
	}

	i := 0
	for itr.Next() {
		matRec := itr.Value()
		rec, hasRecord := recordsMap[matRec.Entity]
		if !hasRecord {
			t.Fatalf("expected with entity ID %s record to exist", matRec.Entity)
		}

		assert.Equal(t, rec[entityIdx], matRec.Entity, "expected same entity")
		assert.Equal(t, roundFloat(rec[valueIdx].(float64), floatRoundingDecimals), roundFloat(matRec.Value.(float64), floatRoundingDecimals), "expected same value")
		assert.Equal(t, rec[tsIdx].(time.Time).Truncate(time.Microsecond), matRec.TS.Truncate(time.Microsecond), "expected same ts")
		i++
	}
}

func verifyTrainingSetTable(t *testing.T, ts TrainingSetIterator, records []GenericRecord, featureIdx int, labelIdx int) {
	recordsMap := make(map[float64]GenericRecord)
	for _, rec := range records {
		recordsMap[roundFloat(rec[featureIdx].(float64), floatRoundingDecimals)] = rec
	}

	for ts.Next() {
		features := ts.Features()
		label := ts.Label()
		rec, hasRecord := recordsMap[roundFloat(features[0].(float64), floatRoundingDecimals)]
		if !hasRecord {
			t.Fatalf("expected with feature %f record to exist", roundFloat(features[0].(float64), floatRoundingDecimals))
		}

		assert.Equal(t, roundFloat(rec[featureIdx].(float64), floatRoundingDecimals), roundFloat(features[0].(float64), floatRoundingDecimals), "expected same feature")
		assert.Equal(t, rec[labelIdx], label, "expected same label")
	}
}

func getSnowflakeConfig(t *testing.T, dbName string) (pc.SnowflakeConfig, error) {
	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}

	snowFlakeDatabase := strings.ToUpper(uuid.NewString())
	if dbName != "" {
		snowFlakeDatabase = dbName
	}
	t.Log("Snowflake Database: ", snowFlakeDatabase)

	username, ok := os.LookupEnv("SNOWFLAKE_USERNAME")
	if !ok {
		t.Fatalf("missing SNOWFLAKE_USERNAME variable")
	}
	password, ok := os.LookupEnv("SNOWFLAKE_PASSWORD")
	if !ok {
		t.Fatalf("missing SNOWFLAKE_PASSWORD variable")
	}
	org, ok := os.LookupEnv("SNOWFLAKE_ORG")
	if !ok {
		t.Fatalf("missing SNOWFLAKE_ORG variable")
	}
	account, ok := os.LookupEnv("SNOWFLAKE_ACCOUNT")
	if !ok {
		t.Fatalf("missing SNOWFLAKE_ACCOUNT variable")
	}
	externalVolume, ok := os.LookupEnv("SNOWFLAKE_EXTERNAL_VOLUME")
	if !ok {
		t.Fatalf("missing SNOWFLAKE_EXTERNAL_VOLUME variable")
	}
	baseLocation, ok := os.LookupEnv("SNOWFLAKE_BASE_LOCATION")
	if !ok {
		t.Fatalf("missing SNOWFLAKE_BASE_LOCATION variable")
	}
	snowflakeConfig := pc.SnowflakeConfig{
		Username:     username,
		Password:     password,
		Organization: org,
		Account:      account,
		Database:     snowFlakeDatabase,
		Warehouse:    "COMPUTE_WH",
		Catalog: &pc.SnowflakeCatalogConfig{
			ExternalVolume: externalVolume,
			BaseLocation:   baseLocation,
			TableConfig: pc.SnowflakeTableConfig{
				TargetLag:   "DOWNSTREAM",
				RefreshMode: "AUTO",
				Initialize:  "ON_CREATE",
			},
		},
	}

	return snowflakeConfig, nil
}

func createSnowflakeDatabase(c pc.SnowflakeConfig) error {
	url := fmt.Sprintf("%s:%s@%s-%s", c.Username, c.Password, c.Organization, c.Account)
	db, err := sql.Open("snowflake", url)
	if err != nil {
		return err
	}
	databaseQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sanitize(c.Database))
	if _, err := db.Exec(databaseQuery); err != nil {
		return err
	}
	return nil
}

func destroySnowflakeDatabase(c pc.SnowflakeConfig) error {
	url := fmt.Sprintf("%s:%s@%s-%s", c.Username, c.Password, c.Organization, c.Account)
	db, err := sql.Open("snowflake", url)
	if err != nil {
		return err
	}
	databaseQuery := fmt.Sprintf("DROP DATABASE IF EXISTS %s", sanitize(c.Database))
	if _, err := db.Exec(databaseQuery); err != nil {
		return err
	}
	return nil
}

func getConfiguredTester(t *testing.T, useCrossDBJoins bool) offlineSqlTest {
	dbName := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))
	t.Logf("Creating Parent Database: %s\n", dbName)
	snowflakeConfig, err := getSnowflakeConfig(t, dbName)
	if err != nil {
		t.Fatalf("could not get snowflake config: %s", err)
	}
	if err := createSnowflakeDatabase(snowflakeConfig); err != nil {
		t.Fatalf("%v", err)
	}

	t.Cleanup(func() {
		t.Logf("Dropping Parent Database: %s\n", dbName)
		err := destroySnowflakeDatabase(snowflakeConfig)
		if err != nil {
			t.Logf("failed to cleanup database: %s\n", err)
		}
	})

	store, err := GetOfflineStore(pt.SnowflakeOffline, snowflakeConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	offlineStoreTester := &snowflakeOfflineStoreTester{store.(*snowflakeOfflineStore)}

	return offlineSqlTest{
		storeTester:      offlineStoreTester,
		testCrossDbJoins: useCrossDBJoins,
	}
}

func createPrimaryTransactionsTable(storeTester offlineSqlStoreTester, sqlLocation location.SQLLocation, numRows int) ([]GenericRecord, error) {
	schema := TableSchema{
		Columns: []TableColumn{
			{
				Name:      "CUSTOMER_ID",
				ValueType: types.String,
			},
			{
				Name:      "TRANSACTION_AMOUNT",
				ValueType: types.Float64,
			},
			{
				Name:      "TIMESTAMP",
				ValueType: types.Timestamp,
			},
			{
				Name:      "IS_FRAUD",
				ValueType: types.Bool,
			},
		},
	}

	primaryTable, err := storeTester.CreateTable(&sqlLocation, schema)
	if err != nil {
		return nil, err
	}

	genericRecords := generateTransactionRecords(numRows)

	sort.Slice(genericRecords, func(i, j int) bool {
		return genericRecords[i][2].(time.Time).Before(genericRecords[j][2].(time.Time))
	})

	if err := primaryTable.WriteBatch(genericRecords); err != nil {
		return nil, err
	}

	return genericRecords, nil
}

func createAndRegisterPrimaryTable(t *testing.T, storeTester offlineSqlTest) (PrimaryTable, []GenericRecord) {
	dbName := fmt.Sprintf("DB_%s", strings.ToUpper(uuid.NewString()[:5]))

	t.Logf("Creating Database: %s\n", dbName)
	err := storeTester.storeTester.CreateDatabase(dbName)
	if err != nil {
		t.Fatalf("could not create database: %v", err)
	}

	t.Cleanup(func() {
		t.Logf("Dropping Database: %s\n", dbName)
		if err := storeTester.storeTester.DropDatabase(dbName); err != nil {
			t.Fatalf("could not drop database: %v", err)
		}
	})

	schemaName := fmt.Sprintf("SCHEMA_%s", strings.ToUpper(uuid.NewString()[:5]))
	if err := storeTester.storeTester.CreateSchema(dbName, schemaName); err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	tableName := "TEST_TRANSACTIONS_TABLE"
	sqlLocation := location.NewSQLLocationWithDBSchemaTable(dbName, schemaName, tableName).(*location.SQLLocation)
	records, err := createPrimaryTransactionsTable(storeTester.storeTester, *sqlLocation, 5)
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	primary, primaryErr := storeTester.storeTester.RegisterPrimaryFromSourceTable(
		ResourceID{Name: tableName, Variant: "test", Type: Primary},
		sqlLocation,
	)
	if primaryErr != nil {
		t.Fatalf("could not register primary table: %v", primaryErr)
	}

	return primary, records
}

func createTransformationTable(t *testing.T, primary PrimaryTable, tester offlineSqlTest) ResourceID {
	sqlPrimaryTbl, isSQLPrimaryTable := primary.(*sqlPrimaryTable)
	if !isSQLPrimaryTable {
		t.Fatalf("expected primary table to be sqlPrimaryTable")
	}

	// Create a transformation
	queryFormat := "SELECT customer_id AS user_id, avg(transaction_amount) AS avg_transaction_amt, CAST(timestamp AS TIMESTAMP_NTZ(6)) AS ts FROM %s GROUP BY user_id, ts ORDER BY ts"

	transformationTableName := fmt.Sprintf("DUMMY_TABLE_TF_%s", strings.ToUpper(uuid.NewString()[:5]))
	transformationVariant := "test_1"
	transformationId := ResourceID{Name: transformationTableName, Variant: transformationVariant, Type: Transformation}
	transformationConfig := TransformationConfig{
		Type:          SQLTransformation,
		TargetTableID: transformationId,
		Query:         fmt.Sprintf(queryFormat, sqlPrimaryTbl.sqlLocation.TableLocation().String()),
		SourceMapping: []SourceMapping{
			{
				Template:       SanitizeSnowflakeIdentifier(sqlPrimaryTbl.sqlLocation.TableLocation()),
				Source:         sqlPrimaryTbl.sqlLocation.TableLocation().String(),
				ProviderType:   pt.SnowflakeOffline,
				ProviderConfig: tester.storeTester.Config(),
				Location:       sqlPrimaryTbl.sqlLocation,
			},
		},
	}

	err := tester.storeTester.CreateTransformation(transformationConfig)
	if err != nil {
		t.Fatalf("could not create transformation: %v", err)
	}

	return transformationId
}

func createMaterialization(t *testing.T, transformationTable PrimaryTable, tester offlineSqlTest, resConfig *metadata.ResourceSnowflakeConfig) (MaterializationID, ResourceID) {
	sqlPrimaryTblTrans, isSQLPrimaryTblTrans := transformationTable.(*sqlPrimaryTable)
	if !isSQLPrimaryTblTrans {
		t.Fatalf("expected transformation table to be sqlPrimaryTable")
	}

	opts := MaterializationOptions{
		ResourceSnowflakeConfig: resConfig,
		Schema: ResourceSchema{
			Entity:      "user_id",
			Value:       "avg_transaction_amt",
			TS:          "ts",
			SourceTable: sqlPrimaryTblTrans.sqlLocation,
		},
	}
	featureName := fmt.Sprintf("DUMMY_FEATURE_%s", strings.ToUpper(uuid.NewString()[:5]))
	featureID := ResourceID{Name: featureName, Variant: "test", Type: Feature}

	mat, matErr := tester.storeTester.CreateMaterialization(featureID, opts)
	if matErr != nil {
		t.Fatalf("could not create materialization: %v", err)
	}

	return mat.ID(), featureID
}

func registerLabelResource(t *testing.T, tester offlineSqlTest, sourceTableLoc pl.Location) (ResourceID, pl.Location) {
	labelID := ResourceID{Name: fmt.Sprintf("DUMMY_LABEL_%s", strings.ToUpper(uuid.NewString()[:5])), Variant: "test", Type: Label}
	schema := ResourceSchema{
		Entity:      "customer_id",
		Value:       "is_fraud",
		TS:          "timestamp",
		SourceTable: sourceTableLoc,
	}

	if _, err := tester.storeTester.RegisterResourceFromSourceTable(labelID, schema, &ResourceSnowflakeConfigOption{}); err != nil {
		t.Fatalf("could not register label: %v", err)
	}

	resourceTable, err := tester.storeTester.GetResourceTable(labelID)
	if err != nil {
		t.Fatalf("could not get resource table: %v", err)
	}
	snowflakeOfflineTbl, isSnowflakeOfflineTbl := resourceTable.(*snowflakeOfflineTable)
	if !isSnowflakeOfflineTbl {
		t.Fatalf("expected resource table to be snowflakeOfflineTable")
	}

	return labelID, snowflakeOfflineTbl.location
}

func createTrainingSet(t *testing.T, tester offlineSqlTest, mat Materialization, labelID, featureID ResourceID, labelLoc pl.Location) ResourceID {
	sqlMat, isSqlMat := mat.(*sqlMaterialization)
	if !isSqlMat {
		t.Fatalf("expected materialization to be sqlMaterialization")
	}
	// Create a training set
	trainingSetID := ResourceID{Name: fmt.Sprintf("DUMMY_TRAINING_SET_%s", strings.ToUpper(uuid.NewString()[:5])), Variant: "test", Type: TrainingSet}
	trainingSetDef := TrainingSetDef{
		ID:    trainingSetID,
		Label: labelID,
		LabelSourceMapping: SourceMapping{
			ProviderType:        pt.SnowflakeOffline,
			ProviderConfig:      tester.storeTester.Config(),
			TimestampColumnName: "timestamp",
			Location:            labelLoc,
		},
		Features: []ResourceID{featureID},
		FeatureSourceMappings: []SourceMapping{
			{
				ProviderType:        pt.SnowflakeOffline,
				ProviderConfig:      tester.storeTester.Config(),
				TimestampColumnName: "ts",
				Location:            sqlMat.location,
			},
		},
		ResourceSnowflakeConfig: &metadata.ResourceSnowflakeConfig{
			DynamicTableConfig: &metadata.SnowflakeDynamicTableConfig{
				TargetLag: "1 minutes",
			},
		},
	}

	if err := tester.storeTester.CreateTrainingSet(trainingSetDef); err != nil {
		t.Fatalf("could not create training set: %v", err)
	}

	return trainingSetID
}

func generateTransactionRecords(numRows int) []GenericRecord {
	genericRecords := make([]GenericRecord, 0)
	for i := 0; i < numRows; i++ {
		randStr := uuid.NewString()[:5]
		genericRecords = append(genericRecords, GenericRecord{
			fmt.Sprintf("C%d%s", i, randStr), // CUSTOMER_ID
			rand.Float64() * 1000,            // TRANSACTION_AMOUNT
			time.Now().UTC().Add(time.Duration(i) * time.Hour).Add(time.Duration(1) * time.Minute), // TIMESTAMP
			i%2 == 0, // IS_FRAUD
		})
	}
	return genericRecords
}
