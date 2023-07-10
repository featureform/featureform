package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestPineconeConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"ApiKey": true,
	}

	config := PineconeConfig{
		ProjectID:   "default",
		Environment: "us-west1-gcp-free",
		ApiKey:      "pinecone-api-key",
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestPineconeConfigDifferingFields(t *testing.T) {
	type args struct {
		a PineconeConfig
		b PineconeConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: PineconeConfig{
				ProjectID:   "default",
				Environment: "us-west1-gcp-free",
				ApiKey:      "pinecone-api-key",
			},
			b: PineconeConfig{
				ProjectID:   "default",
				Environment: "us-west1-gcp-free",
				ApiKey:      "pinecone-api-key",
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: PineconeConfig{
				ProjectID:   "default",
				Environment: "us-west1-gcp-free",
				ApiKey:      "pinecone-api-key",
			},
			b: PineconeConfig{
				ProjectID:   "default",
				Environment: "us-west1-gcp-free",
				ApiKey:      "pinecone-api-key-2",
			},
		}, ss.StringSet{
			"ApiKey": true,
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
