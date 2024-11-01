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

func TestRedisConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Password": true,
	}

	config := RedisConfig{
		Addr:     "0.0.0.0:6379",
		Password: "password",
		DB:       0,
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestRedisConfigDifferingFields(t *testing.T) {
	type args struct {
		a RedisConfig
		b RedisConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: RedisConfig{
				Addr:     "0.0.0.0:6379",
				Password: "password",
				DB:       0,
			},
			b: RedisConfig{
				Addr:     "0.0.0.0:6379",
				Password: "password",
				DB:       0,
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: RedisConfig{
				Addr:     "0.0.0.0:6379",
				Password: "password",
				DB:       0,
			},
			b: RedisConfig{
				Addr:     "0.0.0.0:6379",
				Password: "abc123",
				DB:       0,
			},
		}, ss.StringSet{
			"Password": true,
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
