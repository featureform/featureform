// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/stringset"
)

func TestSnowflakeConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Username":      true,
		"Password":      true,
		"Role":          true,
		"Schema":        true,
		"Database":      true,
		"Warehouse":     true,
		"SessionParams": true,
	}

	config := SnowflakeConfig{
		Username:       "featureformer",
		Password:       "password",
		AccountLocator: "xy12345.snowflakecomputing.com",
		Organization:   "featureform",
		Account:        "featureform-test",
		Database:       "transactions_db",
		Schema:         "fraud",
		Warehouse:      "ff_wh_xs",
		Role:           "sysadmin",
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestSnowflakeConfigDifferingFields(t *testing.T) {
	type args struct {
		a SnowflakeConfig
		b SnowflakeConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: SnowflakeConfig{
				Username:       "featureformer",
				Password:       "password",
				AccountLocator: "xy12345.snowflakecomputing.com",
				Organization:   "featureform",
				Account:        "featureform-test",
				Database:       "transactions_db",
				Schema:         "fraud",
				Warehouse:      "ff_wh_xs",
				Role:           "sysadmin",
			},
			b: SnowflakeConfig{
				Username:       "featureformer",
				Password:       "password",
				AccountLocator: "xy12345.snowflakecomputing.com",
				Organization:   "featureform",
				Account:        "featureform-test",
				Database:       "transactions_db",
				Schema:         "fraud",
				Warehouse:      "ff_wh_xs",
				Role:           "sysadmin",
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: SnowflakeConfig{
				Username:       "featureformer",
				Password:       "password",
				AccountLocator: "xy12345.snowflakecomputing.com",
				Organization:   "featureform",
				Account:        "featureform-test",
				Database:       "transactions_db",
				Schema:         "fraud",
				Warehouse:      "ff_wh_xs",
				Role:           "sysadmin",
				SessionParams:  map[string]string{"query_tag": "featureform"},
			},
			b: SnowflakeConfig{
				Username:       "fformer2",
				Password:       "admin123",
				AccountLocator: "xy12345.snowflakecomputing.com",
				Organization:   "featureform",
				Account:        "featureform-test",
				Database:       "transactions_db_v2",
				Schema:         "fraud",
				Warehouse:      "ff_wh_xs",
				Role:           "featureformerrole",
				SessionParams:  map[string]string{"query_tag": "ff"},
			},
		}, ss.StringSet{
			"Username":      true,
			"Password":      true,
			"Role":          true,
			"Database":      true,
			"SessionParams": true,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := tt.args.a.DifferingFields(tt.args.b)

			if err != nil {
				t.Errorf("Failed to get differing fields due to error: %v", err)
			}

			if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("Expected %v, but instead found %v", tt.expected, actual)
			}

		})
	}

}

func TestSnowflakeConfigSerializationDeserialization(t *testing.T) {
	tests := []struct {
		name     string
		config   SnowflakeConfig
		expected []byte
	}{
		{"No Catalog", SnowflakeConfig{
			Username:       "featureformer",
			Password:       "password",
			AccountLocator: "xy12345.snowflakecomputing.com",
			Organization:   "featureform",
			Account:        "featureform-test",
			Database:       "transactions_db",
			Schema:         "fraud",
			Warehouse:      "ff_wh_xs",
			Role:           "sysadmin",
			Catalog:        nil,
		}, []byte(`{"Username":"featureformer","Password":"password","AccountLocator":"xy12345.snowflakecomputing.com","Organization":"featureform","Account":"featureform-test","Database":"transactions_db","Schema":"fraud","Warehouse":"ff_wh_xs","Role":"sysadmin","Catalog":null,"SessionParams":null}`)},
		{"With Catalog", SnowflakeConfig{
			Username:       "featureformer",
			Password:       "password",
			AccountLocator: "xy12345.snowflakecomputing.com",
			Organization:   "featureform",
			Account:        "featureform-test",
			Database:       "transactions_db",
			Schema:         "fraud",
			Warehouse:      "ff_wh_xs",
			Role:           "sysadmin",
			Catalog: &SnowflakeCatalogConfig{
				ExternalVolume: "external_volume",
				BaseLocation:   "base_location",
				TableConfig: SnowflakeTableConfig{
					TargetLag:   "target_lag",
					RefreshMode: "refresh_mode",
					Initialize:  "initialize",
				},
			},
		}, []byte(`{"Username":"featureformer","Password":"password","AccountLocator":"xy12345.snowflakecomputing.com","Organization":"featureform","Account":"featureform-test","Database":"transactions_db","Schema":"fraud","Warehouse":"ff_wh_xs","Role":"sysadmin","Catalog":{"ExternalVolume":"external_volume","BaseLocation":"base_location","TableConfig":{"TargetLag":"target_lag","RefreshMode":"refresh_mode","Initialize":"initialize"}},"SessionParams":null}`)},
		{"With Catalog And Session Params", SnowflakeConfig{
			Username:       "featureformer",
			Password:       "password",
			AccountLocator: "xy12345.snowflakecomputing.com",
			Organization:   "featureform",
			Account:        "featureform-test",
			Database:       "transactions_db",
			Schema:         "fraud",
			Warehouse:      "ff_wh_xs",
			Role:           "sysadmin",
			Catalog: &SnowflakeCatalogConfig{
				ExternalVolume: "external_volume",
				BaseLocation:   "base_location",
				TableConfig: SnowflakeTableConfig{
					TargetLag:   "target_lag",
					RefreshMode: "refresh_mode",
					Initialize:  "initialize",
				},
			},
			SessionParams: map[string]string{"query_tag": "featureform"},
		}, []byte(`{"Username":"featureformer","Password":"password","AccountLocator":"xy12345.snowflakecomputing.com","Organization":"featureform","Account":"featureform-test","Database":"transactions_db","Schema":"fraud","Warehouse":"ff_wh_xs","Role":"sysadmin","Catalog":{"ExternalVolume":"external_volume","BaseLocation":"base_location","TableConfig":{"TargetLag":"target_lag","RefreshMode":"refresh_mode","Initialize":"initialize"}},"SessionParams":{"query_tag":"featureform"}}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualSerialized := tt.config.Serialize()

			if !reflect.DeepEqual(actualSerialized, tt.expected) {
				t.Errorf("Expected %v, but instead found %v", tt.expected, actualSerialized)
			}

			var actualDeserialized SnowflakeConfig
			err := actualDeserialized.Deserialize(tt.expected)
			if err != nil {
				t.Errorf("Failed to deserialize due to error: %v", err)
			}
			if !reflect.DeepEqual(tt.config, actualDeserialized) {
				t.Errorf("Expected %v, but instead found %v", tt.config, actualDeserialized)
			}
		})
	}
}
