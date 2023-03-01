package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestDynamoConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"AccessKey": true,
		"SecretKey": true,
	}

	config := DynamodbConfig{
		Prefix:    "Featureform_table__",
		Region:    "us-east-1",
		AccessKey: "root",
		SecretKey: "secret",
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestDynamoConfigDifferingFields(t *testing.T) {
	type args struct {
		a DynamodbConfig
		b DynamodbConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: DynamodbConfig{
				Prefix:    "Featureform_table__",
				Region:    "us-east-1",
				AccessKey: "root",
				SecretKey: "secret",
			},
			b: DynamodbConfig{
				Prefix:    "Featureform_table__",
				Region:    "us-east-1",
				AccessKey: "root",
				SecretKey: "secret",
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: DynamodbConfig{
				Prefix:    "Featureform_table__",
				Region:    "us-east-1",
				AccessKey: "root",
				SecretKey: "secret",
			},
			b: DynamodbConfig{
				Prefix:    "Featureform_table__",
				Region:    "us-west-2",
				AccessKey: "root-user",
				SecretKey: "secret2",
			},
		}, ss.StringSet{
			"Region":    true,
			"AccessKey": true,
			"SecretKey": true,
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
