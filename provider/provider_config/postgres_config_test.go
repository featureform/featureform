package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestPostgresConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Username": true,
		"Password": true,
		"Port":     true,
	}

	config := PostgresConfig{
		Host:     "0.0.0.0",
		Port:     "5432",
		Username: "postgres",
		Password: "password",
		Database: "postgres",
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestPostgresConfigDifferingFields(t *testing.T) {
	type args struct {
		a PostgresConfig
		b PostgresConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: PostgresConfig{
				Host:     "0.0.0.0",
				Port:     "5432",
				Username: "postgres",
				Password: "password",
				Database: "postgres",
			},
			b: PostgresConfig{
				Host:     "0.0.0.0",
				Port:     "5432",
				Username: "postgres",
				Password: "password",
				Database: "postgres",
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: PostgresConfig{
				Host:     "0.0.0.0",
				Port:     "5432",
				Username: "postgres",
				Password: "password",
				Database: "postgres",
			},
			b: PostgresConfig{
				Host:     "127.0.0.1",
				Port:     "5432",
				Username: "root",
				Password: "password",
				Database: "transaction",
			},
		}, ss.StringSet{
			"Host":     true,
			"Username": true,
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
