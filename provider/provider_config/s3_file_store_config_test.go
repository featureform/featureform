// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/stringset"
)

func TestS3ConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Credentials": true,
	}

	config := S3FileStoreConfig{
		Credentials:  AWSStaticCredentials{AccessKeyId: "aws-key", SecretKey: "aws-secret"},
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
		a S3FileStoreConfig
		b S3FileStoreConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: S3FileStoreConfig{
				Credentials:  AWSStaticCredentials{AccessKeyId: "aws-key", SecretKey: "aws-secret"},
				BucketRegion: "us-east-1",
				BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
			},
			b: S3FileStoreConfig{
				Credentials:  AWSStaticCredentials{AccessKeyId: "aws-key", SecretKey: "aws-secret"},
				BucketRegion: "us-east-1",
				BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: S3FileStoreConfig{
				Credentials:  AWSStaticCredentials{AccessKeyId: "aws-key", SecretKey: "aws-secret"},
				BucketRegion: "us-east-1",
				BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
			},
			b: S3FileStoreConfig{
				Credentials:  AWSStaticCredentials{AccessKeyId: "aws-key2", SecretKey: "aws-secret2"},
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

func TestS3FileStoreConfig_SerializationDeserialization(t *testing.T) {
	tests := []struct {
		name    string
		config  S3FileStoreConfig
		wantErr bool
	}{
		{
			name: "static credentials",
			config: S3FileStoreConfig{
				BucketRegion: "us-east-1",
				BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				Credentials: AWSStaticCredentials{
					AccessKeyId: "AKIA1234567890",
					SecretKey:   "secret",
				},
			},
			wantErr: false,
		},
		{
			name: "assume role credentials",
			config: S3FileStoreConfig{
				BucketRegion: "us-east-1",
				BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				Credentials:  AWSAssumeRoleCredentials{},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serialized, err := tt.config.Serialize()
			if err != nil {
				t.Errorf("Serialize() error = %v", err)
				return
			}

			var deserializedConfig S3FileStoreConfig
			err = deserializedConfig.Deserialize(serialized)
			if (err != nil) != tt.wantErr {
				t.Errorf("Deserialize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(tt.config, deserializedConfig) {
				t.Errorf("Serialized/Deserialized config mismatch. got = %v, want %v", deserializedConfig, tt.config)
			}
		})
	}
}
