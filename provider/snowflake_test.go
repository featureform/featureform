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
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"

	"github.com/featureform/fferr"
	types "github.com/featureform/fftypes"
	"github.com/featureform/metadata"
	"github.com/featureform/provider/dataset"
	"github.com/featureform/provider/location"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	ps "github.com/featureform/provider/provider_schema"
	pt "github.com/featureform/provider/provider_type"
	"github.com/featureform/provider/snowflake"
)

const (
	floatTolerance = 1e-9
	// SHOW DYNAMIC TABLES returns 21 columns; however, we're only concerned with the 11th column which is the warehouse.
	showDynamicTableColCount        = 21
	showDynamicTableWarehouseColIdx = 11
)

type snowflakeOfflineStoreTester struct {
	defaultDbName string
	*snowflakeOfflineStore
}

func (s *snowflakeOfflineStoreTester) GetTestDatabase() string {
	return s.defaultDbName
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

	// Create schema
	query := "CREATE SCHEMA IF NOT EXISTS " + sanitize(schema)
	_, err = db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

type WritableSnowflakeDataset struct {
	*dataset.SqlDataset
	db *sql.DB
}

func (w WritableSnowflakeDataset) WriteBatch(ctx context.Context, rows []types.Row) error {
	if len(rows) == 0 {
		return nil
	}
	schema := w.Schema()
	columns := schema.SanitizedColumnNames()
	sqlLocation, ok := w.Location().(*location.SQLLocation)
	if !ok {
		return fmt.Errorf("invalid location type")
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", sqlLocation.Sanitized(), strings.Join(columns, ", "))
	var args []any
	for i, row := range rows {
		if i > 0 {
			query += ","
		}
		query += "("
		for j := range columns {
			if j > 0 {
				query += ","
			}
			query += "?"
			args = append(args, row[j].Value)
		}
		query += ")"
	}
	_, err := w.db.ExecContext(ctx, query, args...)
	return err
}

func (s *snowflakeOfflineStoreTester) CreateWritableDataset(loc location.Location, schema types.Schema) (dataset.WriteableDataset, error) {
	return s.CreateTableFromSchema(loc, schema)
}

func (s *snowflakeOfflineStoreTester) CreateTableFromSchema(loc location.Location, schema types.Schema) (dataset.WriteableDataset, error) {
	sqlLocation, ok := loc.(*location.SQLLocation)
	if !ok {
		return nil, fmt.Errorf("invalid location type")
	}

	db, err := s.sqlOfflineStore.getDb("", "")
	if err != nil {
		return nil, err
	}

	//row := db.QueryRow("SELECT CURRENT_ROLE()")
	//var role string
	//_ = row.Scan(&role)
	//fmt.Println("Current Role:", role)

	// Build CREATE TABLE statement
	var queryBuilder strings.Builder
	queryBuilder.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", sqlLocation.Sanitized()))

	colNames := schema.SanitizedColumnNames()
	columnDefinitions := make([]string, 0, len(schema.Fields))
	for i, field := range schema.Fields {
		columnDefinitions = append(columnDefinitions, fmt.Sprintf("%s %s", colNames[i], field.NativeType))
	}

	queryBuilder.WriteString(strings.Join(columnDefinitions, ", "))
	queryBuilder.WriteString(")")

	query := queryBuilder.String()
	_, err = db.Exec(query)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	// Create and return dataset
	sqlDataset, err := dataset.NewSqlDataset(db, sqlLocation, schema, snowflake.Converter{}, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to create dataset: %w", err)
	}

	return WritableSnowflakeDataset{
		SqlDataset: sqlDataset,
		db:         db,
	}, nil
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

	row := db.QueryRow("SELECT CURRENT_ROLE()")
	var role string
	_ = row.Scan(&role)
	fmt.Println("Current Role:", role)

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
		SqlPrimaryTable{
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
	if err := id.check(Feature, TrainingSet); err != nil {
		return err
	}
	var resourceType string
	if id.Type == TrainingSet {
		resourceType = TrainingSet.String()
	} else {
		resourceType = FeatureMaterialization.String()
	}
	tableName, err := ps.ResourceToTableName(resourceType, id.Name, id.Variant)
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
			if count >= 1 {
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

func (s *snowflakeOfflineStoreTester) AssertTrainingSetType(t *testing.T, id ResourceID, tsType metadata.TrainingSetType) {
	db, err := s.sqlOfflineStore.getDb("", "")
	if err != nil {
		t.Fatalf("could not get database: %v", err)
	}

	tableName, err := ps.ResourceToTableName(TrainingSet.String(), id.Name, id.Variant)
	if err != nil {
		t.Fatalf("could not get table name: %v", err)
	}

	query := fmt.Sprintf("SELECT TABLE_TYPE, IS_DYNAMIC, IS_ICEBERG FROM information_schema.tables WHERE TABLE_NAME = '%s'", tableName)

	r := db.QueryRow(query)

	var tableType string
	var isDynamic string
	var isIceberg string

	if err := r.Scan(&tableType, &isDynamic, &isIceberg); err != nil {
		if err == sql.ErrNoRows {
			// Handle the case where no rows were returned
			t.Fatal("No tables found with the specified name.")
		}
		t.Fatalf("could not get table name: %v", err)
	}

	var isCorrectTrainingSetType bool
	var tsTypeErr error
	switch tsType {
	case metadata.DynamicTrainingSet:
		isCorrectTrainingSetType = tableType == "BASE TABLE" && isDynamic == "YES" && isIceberg == "YES"
	case metadata.StaticTrainingSet:
		isCorrectTrainingSetType = tableType == "BASE TABLE" && isDynamic == "NO" && isIceberg == "YES"
	case metadata.ViewTrainingSet:
		isCorrectTrainingSetType = tableType == "VIEW" && isDynamic == "NO" && isIceberg == "NO"
	default:
		tsTypeErr = fferr.NewInvalidArgumentErrorf("invalid training set type: %s", tsType)
	}

	if tsTypeErr != nil {
		t.Fatalf("failed to check training set type: %v", err)
	}
	if !isCorrectTrainingSetType {
		t.Fatalf("expected training set to be of type %s", tsType)
	}
}

// TESTS

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

func RegisterMaterializationWithDefaultTargetLagTest(t *testing.T, test OfflineSqlTest) {
	useTimestamps := true
	isIncremental := true
	matTest := newSQLMaterializationTest(test, useTimestamps)
	primary := initSqlPrimaryDataset(t, matTest.tester, matTest.data.location, matTest.data.schema, matTest.data.records)
	matTest.data.opts.ResourceSnowflakeConfig = &metadata.ResourceSnowflakeConfig{
		DynamicTableConfig: &metadata.SnowflakeDynamicTableConfig{
			TargetLag: "90 seconds",
		},
	}
	mat, err := matTest.tester.CreateMaterialization(matTest.data.id, matTest.data.opts)
	if err != nil {
		t.Fatalf("could not create materialization: %v", err)
	}

	if err := primary.WriteBatch(matTest.data.incrementalRecords); err != nil {
		t.Fatalf("could not write batch: %v", err)
	}

	snowflakeTester, isSnowflakeTester := test.storeTester.(*snowflakeOfflineStoreTester)
	if !isSnowflakeTester {
		t.Fatalf("expected store tester to be snowflakeOfflineStoreTester")
	}

	if err := snowflakeTester.PollTableRefresh(matTest.data.id, 90*time.Second, 3*time.Minute); err != nil {
		t.Fatalf("expected table to refresh: %v", err)
	}

	matIncr, err := matTest.tester.GetMaterialization(mat.ID())
	if err != nil {
		t.Fatalf("could not get materialization: %v", err)
	}

	matTest.data.Assert(t, matIncr, isIncremental)
}

func RegisterMaterializationWithDifferentWarehouseTest(t *testing.T, test OfflineSqlTest) {
	useTimestamps := true
	isIncremental := true
	matTest := newSQLMaterializationTest(test, useTimestamps)
	primary := initSqlPrimaryDataset(t, matTest.tester, matTest.data.location, matTest.data.schema, matTest.data.records)
	warehouse := "TEST_WH"
	matTest.data.opts.ResourceSnowflakeConfig = &metadata.ResourceSnowflakeConfig{
		DynamicTableConfig: &metadata.SnowflakeDynamicTableConfig{
			TargetLag: "90 seconds",
		},
		Warehouse: warehouse,
	}
	mat, err := matTest.tester.CreateMaterialization(matTest.data.id, matTest.data.opts)
	if err != nil {
		t.Fatalf("could not create materialization: %v", err)
	}

	if err := primary.WriteBatch(matTest.data.incrementalRecords); err != nil {
		t.Fatalf("could not write batch: %v", err)
	}

	snowflakeTester, isSnowflakeTester := test.storeTester.(*snowflakeOfflineStoreTester)
	if !isSnowflakeTester {
		t.Fatalf("expected store tester to be snowflakeOfflineStoreTester")
	}

	if err := snowflakeTester.PollTableRefresh(matTest.data.id, 90*time.Second, 3*time.Minute); err != nil {
		t.Fatalf("expected table to refresh: %v", err)
	}

	usesWarehouse, err := snowflakeTester.CheckWarehouse(matTest.data.id, warehouse)
	if err != nil {
		t.Fatalf("failed to check warehouse: %v", err)
	}
	if !usesWarehouse {
		t.Fatalf("expected materialization to use warehouse %s", warehouse)
	}

	matIncr, err := matTest.tester.GetMaterialization(mat.ID())
	if err != nil {
		t.Fatalf("could not get materialization: %v", err)
	}

	matTest.data.Assert(t, matIncr, isIncremental)
}

func RegisterTrainingSetWithType(t *testing.T, test OfflineSqlTest, tsDatasetType trainingSetDatasetType, tsType metadata.TrainingSetType) {
	tsTest := newSQLTrainingSetTest(test, tsDatasetType)
	_ = initSqlPrimaryDataset(t, tsTest.tester, tsTest.data.location, tsTest.data.schema, tsTest.data.records)
	_ = initSqlPrimaryDataset(t, tsTest.tester, tsTest.data.labelLocation, tsTest.data.labelSchema, tsTest.data.labelRecords)

	tsTest.data.def.Type = tsType
	if err := tsTest.tester.CreateTrainingSet(tsTest.data.def); err != nil {
		t.Fatalf("could not create training set: %v", err)
	}
	ts, err := tsTest.tester.GetTrainingSet(tsTest.data.id)
	if err != nil {
		t.Fatalf("could not get training set: %v", err)
	}
	tsTest.data.Assert(t, ts)

	snowflakeTester, isSnowflakeTester := test.storeTester.(*snowflakeOfflineStoreTester)
	if !isSnowflakeTester {
		t.Fatalf("expected store tester to be snowflakeOfflineStoreTester")
	}

	snowflakeTester.AssertTrainingSetType(t, tsTest.data.id, tsType)
}

// HELPER FUNCTIONS

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

func getConfiguredSnowflakeTester(t *testing.T) OfflineSqlTest {
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

	offlineStoreTester := &snowflakeOfflineStoreTester{
		defaultDbName:         dbName,
		snowflakeOfflineStore: store.(*snowflakeOfflineStore),
	}

	return OfflineSqlTest{
		storeTester: offlineStoreTester,
		testConfig: OfflineSqlTestConfig{
			sanitizeTableName: func(obj pl.FullyQualifiedObject) string { return SanitizeSnowflakeIdentifier(obj) },
		},
	}
}
