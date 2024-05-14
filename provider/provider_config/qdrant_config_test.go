package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestQdrantConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"ApiKey": true,
	}

	config := QdrantConfig{
		GrpcHost: "xyz-example.eu-central.aws.cloud.qdrant.io:6334",
		ApiKey:   "<SOME_API_KEY>",
		UseTls:   true,
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestQdrantConfigDifferingFields(t *testing.T) {
	type args struct {
		a QdrantConfig
		b QdrantConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: QdrantConfig{
				GrpcHost: "xyz-example.eu-central.aws.cloud.qdrant.io:6334",
				ApiKey:   "<SOME_API_KEY>",
			},
			b: QdrantConfig{
				GrpcHost: "xyz-example.eu-central.aws.cloud.qdrant.io:6334",
				ApiKey:   "<SOME_API_KEY>",
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: QdrantConfig{
				GrpcHost: "xyz-example.eu-east.aws.cloud.qdrant.io:6334",
				ApiKey:   "<SOME_API_KEY>",
			},
			b: QdrantConfig{
				GrpcHost: "xyz-example.eu-east.aws.cloud.qdrant.io:6334",
				ApiKey:   "<SOME_OTHER_API_KEY>",
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
