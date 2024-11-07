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

func TestHDFSConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Host":             true,
		"Port":             true,
		"Path":             true,
		"HDFSSiteConf":     true,
		"CoreSiteConf":     true,
		"CredentialType":   true,
		"CredentialConfig": true,
	}

	config := HDFSFileStoreConfig{
		Host:           "",
		Port:           "",
		Path:           "",
		HDFSSiteConf:   "",
		CoreSiteConf:   "",
		CredentialType: BasicCredential,
		CredentialConfig: &BasicCredentialConfig{
			Username: "",
			Password: "",
		},
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestHDFSConfigDifferingFields(t *testing.T) {
	t.Skip("Needs to be fixed @ahmad")
	type args struct {
		a HDFSFileStoreConfig
		b HDFSFileStoreConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: HDFSFileStoreConfig{
				Host:           "localhost",
				Port:           "9000",
				CredentialType: BasicCredential,
				CredentialConfig: &BasicCredentialConfig{
					Username: "hdfs",
				},
			},
			b: HDFSFileStoreConfig{
				Host:           "localhost",
				Port:           "9000",
				CredentialType: BasicCredential,
				CredentialConfig: &BasicCredentialConfig{
					Username: "hdfs",
				},
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: HDFSFileStoreConfig{
				Host:           "localhost",
				Port:           "9000",
				CredentialType: BasicCredential,
				CredentialConfig: &BasicCredentialConfig{
					Username: "hdfs",
				},
			},
			b: HDFSFileStoreConfig{
				Host:           "localhost",
				Port:           "9000",
				CredentialType: BasicCredential,
				CredentialConfig: &BasicCredentialConfig{
					Username: "root",
				},
			},
		}, ss.StringSet{
			"Username": true,
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
