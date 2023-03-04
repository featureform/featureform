package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestExecutorConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"DockerImage": true,
	}

	config := ExecutorConfig{
		DockerImage: "featureformcom:exe-1",
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestExecutorConfigDifferingFields(t *testing.T) {
	type args struct {
		a ExecutorConfig
		b ExecutorConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: ExecutorConfig{
				DockerImage: "featureformcom:exe-1",
			},
			b: ExecutorConfig{
				DockerImage: "featureformcom:exe-1",
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: ExecutorConfig{
				DockerImage: "featureformcom:exe-1",
			},
			b: ExecutorConfig{
				DockerImage: "featureformcom:exe-2",
			},
		}, ss.StringSet{
			"DockerImage": true,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := tt.args.a.DifferingFields(&tt.args.b)

			if err != nil {
				t.Errorf("Failed to get differing fields due to error: %v", err)
			}

			if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("Expected %v, but instead found %v", tt.expected, actual)
			}

		})
	}

}
