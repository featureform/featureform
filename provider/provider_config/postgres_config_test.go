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

	"github.com/featureform/provider/retriever"
	"github.com/stretchr/testify/assert"

	ss "github.com/featureform/helpers/stringset"
)

func TestPostgresConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Username": true,
		"Password": true,
		"Port":     true,
		"SSLMode":  true,
	}

	config := PostgresConfig{
		Host:     "0.0.0.0",
		Port:     "5432",
		Username: "postgres",
		Password: retriever.NewStaticValue[string]("password"),
		Database: "postgres",
		SSLMode:  "disable",
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestPostgresConfigDifferingFields(t *testing.T) {
	type args struct {
		a PostgresConfig
		b PostgresConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: PostgresConfig{
				Host:     "0.0.0.0",
				Port:     "5432",
				Username: "postgres",
				Password: retriever.NewStaticValue[string]("password"),
				Database: "postgres",
				SSLMode:  "disable",
			},
			b: PostgresConfig{
				Host:     "0.0.0.0",
				Port:     "5432",
				Username: "postgres",
				Password: retriever.NewStaticValue[string]("password"),
				Database: "postgres",
				SSLMode:  "disable",
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: PostgresConfig{
				Host:     "0.0.0.0",
				Port:     "5432",
				Username: "postgres",
				Password: retriever.NewStaticValue[string]("password"),
				Database: "postgres",
				SSLMode:  "disable",
			},
			b: PostgresConfig{
				Host:     "127.0.0.1",
				Port:     "5432",
				Username: "root",
				Password: retriever.NewStaticValue[string]("password"),
				Database: "transaction",
				SSLMode:  "require",
			},
		}, ss.StringSet{
			"Host":     true,
			"Username": true,
			"Database": true,
			"SSLMode":  true,
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

func TestPostgresConfig_Serde_BackwardsCmp(t *testing.T) {
	serializedConfig := []byte(`{"Host":"localhost","Port":"5432","Username":"user","Password":"password","Database":"testdb","SSLMode":"disable"}`)

	var deserializedConfig PostgresConfig
	err := deserializedConfig.Deserialize(serializedConfig)
	if err != nil {
		t.Fatalf("Failed to deserialize config: %v", err)
	}

	expectedConfig := &PostgresConfig{
		Host:     "localhost",
		Port:     "5432",
		Username: "user",
		Password: retriever.NewStaticValue[string]("password"),
		Database: "testdb",
		Schema:   "public",
		SSLMode:  "disable",
	}

	// Compare original and deserialized configs
	assert.Equal(t, expectedConfig, &deserializedConfig)
}

func TestPostgresConfig_Serde_MissingPassword(t *testing.T) {
	serializedConfig := []byte(`{"Host":"localhost","Port":"5432","Username":"user","Database":"testdb","SSLMode":"disable"}`)

	var deserializedConfig PostgresConfig
	err := deserializedConfig.Deserialize(serializedConfig)
	if err != nil {
		t.Fatalf("Failed to deserialize config: %v", err)
	}

	expectedConfig := &PostgresConfig{
		Host:     "localhost",
		Port:     "5432",
		Username: "user",
		Password: retriever.NewStaticValue[string](""),
		Database: "testdb",
		Schema:   "public",
		SSLMode:  "disable",
	}

	assert.Equal(t, expectedConfig, &deserializedConfig)
}
