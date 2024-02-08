// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
package provider

import (
	"database/sql"
	"encoding/json"
	"fmt"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestOfflineStoreSnowflake(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	err := godotenv.Load("../.env")
	if err != nil {
		t.Logf("could not open .env file... Checking environment: %s", err)
	}

	snowFlakeDatabase := strings.ToUpper(uuid.NewString())
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

	snowflakeConfig := pc.SnowflakeConfig{
		Username:     username,
		Password:     password,
		Organization: org,
		Account:      account,
		Database:     snowFlakeDatabase,
	}
	if err := createSnowflakeDatabase(snowflakeConfig); err != nil {
		t.Fatalf("%v", err)
	}

	t.Cleanup(func() {
		err := destroySnowflakeDatabase(snowflakeConfig)
		if err != nil {
			t.Logf("failed to cleanup database: %s\n", err)
		}
	})

	store, err := GetOfflineStore(pt.SnowflakeOffline, snowflakeConfig.Serialize())
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OfflineStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
	test.RunSQL()
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
		fmt.Errorf(err.Error())
		return err
	}
	databaseQuery := fmt.Sprintf("DROP DATABASE IF EXISTS %s", sanitize(c.Database))
	if _, err := db.Exec(databaseQuery); err != nil {
		fmt.Errorf(err.Error())
		return err
	}
	return nil
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
			"Neither Current Or Legacy",
			fields{Username: "u", Password: "p", Database: "d", Schema: "s"},
			"",
			true,
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
			}
			got, err := sf.ConnectionString()
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
		Username:     "username",
		Password:     "password",
		Organization: "org",
		Account:      "account",
		Database:     "database",
		Schema:       "schema",
	}
	credentialsMap := make(map[string]string)
	credentialsMap["Username"] = expected.Username
	credentialsMap["Password"] = expected.Password
	credentialsMap["Organization"] = expected.Organization
	credentialsMap["Account"] = expected.Account
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
