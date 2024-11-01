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

func TestDynamoConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Credentials":  true,
		"ImportFromS3": true,
	}

	config := DynamodbConfig{
		Prefix:      "Featureform_table__",
		Region:      "us-east-1",
		Credentials: AWSStaticCredentials{AccessKeyId: "aws-key", SecretKey: "aws-secret"},
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
				Prefix:      "Featureform_table__",
				Region:      "us-east-1",
				Credentials: AWSStaticCredentials{AccessKeyId: "aws-key", SecretKey: "aws-secret"},
			},
			b: DynamodbConfig{
				Prefix:      "Featureform_table__",
				Region:      "us-east-1",
				Credentials: AWSStaticCredentials{AccessKeyId: "aws-key", SecretKey: "aws-secret"},
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: DynamodbConfig{
				Prefix:      "Featureform_table__",
				Region:      "us-east-1",
				Credentials: AWSStaticCredentials{AccessKeyId: "aws-key", SecretKey: "aws-secret"},
			},
			b: DynamodbConfig{
				Prefix:      "Featureform_table__",
				Region:      "us-west-2",
				Credentials: AWSStaticCredentials{AccessKeyId: "aws-key", SecretKey: "aws-secret"},
			},
		}, ss.StringSet{
			"Region": true,
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

func TestDynamodbConfig_SerializationDeserialization(t *testing.T) {
	tests := []struct {
		name    string
		config  DynamodbConfig
		wantErr bool
	}{
		{
			name: "static credentials",
			config: DynamodbConfig{
				Prefix: "myTablePrefix",
				Region: "us-west-2",
				Credentials: AWSStaticCredentials{
					AccessKeyId: "AKIA1234567890",
					SecretKey:   "secret",
				},
				ImportFromS3: true,
			},
			wantErr: false,
		},
		{
			name: "assume role credentials",
			config: DynamodbConfig{
				Prefix:       "otherTablePrefix",
				Region:       "eu-central-1",
				Credentials:  AWSAssumeRoleCredentials{},
				ImportFromS3: false,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serialized := tt.config.Serialized()

			var deserializedConfig DynamodbConfig
			err := deserializedConfig.Deserialize(serialized)
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
