package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestS3ConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Credentials": true,
	}

	config := S3FileStoreConfig{
		Credentials:  AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
		BucketRegion: "us-east-1",
		BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
		Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestS3ConfigDifferingFields(t *testing.T) {
	type args struct {
		a *S3FileStoreConfig
		b *S3FileStoreConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: &S3FileStoreConfig{
				Credentials:  AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
				BucketRegion: "us-east-1",
				BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
			},
			b: &S3FileStoreConfig{
				Credentials:  AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
				BucketRegion: "us-east-1",
				BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: &S3FileStoreConfig{
				Credentials:  AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
				BucketRegion: "us-east-1",
				BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
			},
			b: &S3FileStoreConfig{
				Credentials:  AWSCredentials{AWSAccessKeyId: "aws-key2", AWSSecretKey: "aws-secret2"},
				BucketRegion: "us-west-2",
				BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions-v2",
				Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
			},
		}, ss.StringSet{
			"Credentials":  true,
			"BucketRegion": true,
			"BucketPath":   true,
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
