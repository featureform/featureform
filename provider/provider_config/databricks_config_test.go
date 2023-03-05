package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestDatabricksConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Username": true,
		"Password": true,
		"Token":    true,
	}

	config := DatabricksConfig{
		Host:     "https://featureform.cloud.databricks.com",
		Username: "featureformer",
		Password: "password",
		Cluster:  "1115-164516-often242",
		Token:    "dapi1234567890ab1cde2f3ab456c7d89efa",
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestDatabricksConfigDifferingFields(t *testing.T) {
	type args struct {
		a DatabricksConfig
		b DatabricksConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: DatabricksConfig{
				Host:     "https://featureform.cloud.databricks.com",
				Username: "featureformer",
				Password: "password",
				Cluster:  "1115-164516-often242",
				Token:    "dapi1234567890ab1cde2f3ab456c7d89efa",
			},
			b: DatabricksConfig{
				Host:     "https://featureform.cloud.databricks.com",
				Username: "featureformer",
				Password: "password",
				Cluster:  "1115-164516-often242",
				Token:    "dapi1234567890ab1cde2f3ab456c7d89efa",
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: DatabricksConfig{
				Host:     "https://featureform.cloud.databricks.com",
				Username: "featureformer",
				Password: "password",
				Cluster:  "1115-164516-often242",
				Token:    "dapi1234567890ab1cde2f3ab456c7d89efa",
			},
			b: DatabricksConfig{
				Host:     "https://featureform.cloud.databricks.com",
				Username: "ff-user-2",
				Password: "pass123word",
				Cluster:  "1115-164516-often242",
				Token:    "dapi1234567890ab1cde2f3ab456c7d89efa",
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
