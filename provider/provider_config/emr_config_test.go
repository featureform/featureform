package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestEMRConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Credentials": true,
	}

	config := EMRConfig{
		Credentials:   AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
		ClusterRegion: "us-east-1",
		ClusterName:   "featureform-clst",
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestEMRConfigDifferingFields(t *testing.T) {
	type args struct {
		a EMRConfig
		b EMRConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: EMRConfig{
				Credentials:   AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
				ClusterRegion: "us-east-1",
				ClusterName:   "featureform-clst",
			},
			b: EMRConfig{
				Credentials:   AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
				ClusterRegion: "us-east-1",
				ClusterName:   "featureform-clst",
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: EMRConfig{
				Credentials:   AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
				ClusterRegion: "us-east-1",
				ClusterName:   "featureform-clst",
			},
			b: EMRConfig{
				Credentials:   AWSCredentials{AWSAccessKeyId: "aws-key2", AWSSecretKey: "aws-secret2"},
				ClusterRegion: "us-west-2",
				ClusterName:   "ff-clst2",
			},
		}, ss.StringSet{
			"Credentials":   true,
			"ClusterName":   true,
			"ClusterRegion": true,
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
