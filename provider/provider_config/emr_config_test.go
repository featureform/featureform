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

func TestEMRConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Credentials":   true,
		"ClusterName":   true,
		"ClusterRegion": true,
	}

	config := EMRConfig{
		Credentials:   AWSStaticCredentials{AccessKeyId: "aws-key", SecretKey: "aws-secret"},
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
				Credentials:   AWSStaticCredentials{AccessKeyId: "aws-key", SecretKey: "aws-secret"},
				ClusterRegion: "us-east-1",
				ClusterName:   "featureform-clst",
			},
			b: EMRConfig{
				Credentials:   AWSStaticCredentials{AccessKeyId: "aws-key", SecretKey: "aws-secret"},
				ClusterRegion: "us-east-1",
				ClusterName:   "featureform-clst",
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: EMRConfig{
				Credentials:   AWSStaticCredentials{AccessKeyId: "aws-key", SecretKey: "aws-secret"},
				ClusterRegion: "us-east-1",
				ClusterName:   "featureform-clst",
			},
			b: EMRConfig{
				Credentials:   AWSStaticCredentials{AccessKeyId: "aws-key2", SecretKey: "aws-secret2"},
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

func TestEMRConfig_SerializationDeserialization(t *testing.T) {
	tests := []struct {
		name    string
		config  EMRConfig
		wantErr bool
	}{
		{
			name: "static credentials",
			config: EMRConfig{
				ClusterRegion: "us-east-1",
				ClusterName:   "featureform-clst",
				Credentials: AWSStaticCredentials{
					AccessKeyId: "AKIA1234567890",
					SecretKey:   "secret",
				},
			},
			wantErr: false,
		},
		{
			name: "assume role credentials",
			config: EMRConfig{
				ClusterRegion: "us-east-1",
				ClusterName:   "featureform-clst",
				Credentials:   AWSAssumeRoleCredentials{},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serialized, err := tt.config.Serialize()
			if (err != nil) != tt.wantErr {
				t.Errorf("Serialize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			var deserializedConfig EMRConfig
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
