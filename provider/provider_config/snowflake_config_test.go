package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestSnowflakeConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Username": true,
		"Password": true,
		"Role":     true,
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
			},
		}, ss.StringSet{
			"Username": true,
			"Password": true,
			"Role":     true,
			"Database": true,
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
