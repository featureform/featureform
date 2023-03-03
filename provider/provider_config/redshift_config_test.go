package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestRedshiftConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Username": true,
		"Password": true,
		"Port":     true,
	}

	config := RedshiftConfig{
		Endpoint: "0.0.0.0",
		Port:     "5439",
		Username: "root",
		Password: "password",
		Database: "default",
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestRedshiftConfigDifferingFields(t *testing.T) {
	type args struct {
		a RedshiftConfig
		b RedshiftConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: RedshiftConfig{
				Endpoint: "0.0.0.0",
				Port:     "5439",
				Username: "root",
				Password: "password",
				Database: "default",
			},
			b: RedshiftConfig{
				Endpoint: "0.0.0.0",
				Port:     "5439",
				Username: "root",
				Password: "password",
				Database: "default",
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: RedshiftConfig{
				Endpoint: "0.0.0.0",
				Port:     "5439",
				Username: "root",
				Password: "password",
				Database: "default",
			},
			b: RedshiftConfig{
				Endpoint: "0.0.0.0",
				Port:     "5439",
				Username: "featureformer",
				Password: "pass123word",
				Database: "default",
			},
		}, ss.StringSet{
			"Username": true,
			"Password": true,
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
