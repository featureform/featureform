package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestAzureFileStoreConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"AccountKey": true,
	}

	config := AzureFileStoreConfig{
		AccountName:   "featureform-str",
		AccountKey:    "secret-account-key",
		ContainerName: "transactions_container",
		Path:          "custom/path/in/container",
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestAzureFileStoreConfigDifferingFields(t *testing.T) {
	type args struct {
		a AzureFileStoreConfig
		b AzureFileStoreConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: AzureFileStoreConfig{
				AccountName:   "featureform-str",
				AccountKey:    "secret-account-key",
				ContainerName: "transactions_container",
				Path:          "custom/path/in/container",
			},
			b: AzureFileStoreConfig{
				AccountName:   "featureform-str",
				AccountKey:    "secret-account-key",
				ContainerName: "transactions_container",
				Path:          "custom/path/in/container",
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: AzureFileStoreConfig{
				AccountName:   "featureform-str",
				AccountKey:    "secret-account-key",
				ContainerName: "transactions_container",
				Path:          "custom/path/in/container",
			},
			b: AzureFileStoreConfig{
				AccountName:   "featureform-str",
				AccountKey:    "secret-account-key-2",
				ContainerName: "transactions_container",
				Path:          "custom/path/in/transactions_container",
			},
		}, ss.StringSet{
			"AccountKey": true,
			"Path":       true,
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
