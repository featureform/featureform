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

func TestClickHouseConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Username": true,
		"Password": true,
		"Port":     true,
		"SSL":      true,
	}

	config := ClickHouseConfig{
		Host:     "0.0.0.0",
		Port:     9000,
		Username: "default",
		Password: "password",
		Database: "clickhouse",
		SSL:      false,
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestClickHouseConfigDifferingFields(t *testing.T) {
	type args struct {
		a ClickHouseConfig
		b ClickHouseConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: ClickHouseConfig{
				Host:     "0.0.0.0",
				Port:     9000,
				Username: "clickhouse",
				Password: "password",
				Database: "default",
				SSL:      false,
			},
			b: ClickHouseConfig{
				Host:     "0.0.0.0",
				Port:     9000,
				Username: "clickhouse",
				Password: "password",
				Database: "default",
				SSL:      false,
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: ClickHouseConfig{
				Host:     "0.0.0.0",
				Port:     9000,
				Username: "clickhouse",
				Password: "password",
				Database: "default",
				SSL:      false,
			},
			b: ClickHouseConfig{
				Host:     "127.0.0.1",
				Port:     9000,
				Username: "root",
				Password: "password",
				Database: "transaction",
				SSL:      true,
			},
		}, ss.StringSet{
			"Host":     true,
			"Username": true,
			"Database": true,
			"SSL":      true,
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
