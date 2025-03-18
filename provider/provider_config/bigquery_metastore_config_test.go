// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

package provider_config

import (
	"reflect"
	"testing"

	"github.com/featureform/filestore"
	ss "github.com/featureform/helpers/stringset"
)

func TestBigQueryMetastoreConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"Credentials": true,
	}
	config := BigQueryMetastoreConfig{}
	actual := config.MutableFields()
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestBigQueryMetastoreConfigDifferingFields(t *testing.T) {
	type args struct {
		a BigQueryMetastoreConfig
		b BigQueryMetastoreConfig
	}
	warehouse, err := filestore.NewGCSFilepath("bucket", "dir/abc", true)
	if err != nil {
		t.Fatalf("Failed to create warehouse: %s", err)
	}
	sameWarehouse, err := filestore.NewGCSFilepath("bucket", "dir/abc", true)
	if err != nil {
		t.Fatalf("Failed to create warehouse: %s", err)
	}
	diffWarehouse, err := filestore.NewGCSFilepath("bucket", "dir/diff", true)
	if err != nil {
		t.Fatalf("Failed to create warehouse: %s", err)
	}
	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{
			name: "No Differing Fields",
			args: args{
				a: BigQueryMetastoreConfig{
					ProjectID: "proj",
					Region:    "reg",
					Warehouse: *warehouse,
					Credentials: map[string]any{
						"abc": 123,
						"def": "123",
						"ghi": map[string]any{
							"inner": true,
						},
					},
				},
				b: BigQueryMetastoreConfig{
					ProjectID: "proj",
					Region:    "reg",
					Warehouse: *sameWarehouse,
					Credentials: map[string]any{
						"abc": 123,
						"def": "123",
						"ghi": map[string]any{
							"inner": true,
						},
					},
				},
			},
			expected: ss.New(),
		},
		{
			name: "Diff Warehouse",
			args: args{
				a: BigQueryMetastoreConfig{
					ProjectID: "proj",
					Region:    "reg",
					Warehouse: *warehouse,
					Credentials: map[string]any{
						"abc": 123,
						"def": "123",
						"ghi": map[string]any{
							"inner": true,
						},
					},
				},
				b: BigQueryMetastoreConfig{
					ProjectID: "proj",
					Region:    "reg",
					Warehouse: *diffWarehouse,
					Credentials: map[string]any{
						"abc": 123,
						"def": "123",
						"ghi": map[string]any{
							"inner": true,
						},
					},
				},
			},
			expected: ss.New("Warehouse"),
		},
		{
			name: "Diff All",
			args: args{
				a: BigQueryMetastoreConfig{
					ProjectID: "proj",
					Region:    "reg",
					Warehouse: *warehouse,
					Credentials: map[string]any{
						"abc": 123,
						"def": "123",
						"ghi": map[string]any{
							"inner": true,
						},
					},
				},
				b: BigQueryMetastoreConfig{
					ProjectID: "proj2",
					Region:    "reg2",
					Warehouse: *diffWarehouse,
					Credentials: map[string]any{
						"abc": 124,
						"def": "123",
						"ghi": map[string]any{
							"inner": true,
						},
					},
				},
			},
			expected: ss.New("ProjectID", "Region", "Credentials", "Warehouse"),
		},
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

func TestBigQueryMetastoreConfigSerDe(t *testing.T) {
	warehouse, err := filestore.NewGCSFilepath("bucket", "dir/abc", true)
	if err != nil {
		t.Fatalf("Failed to create warehouse: %s", err)
	}
	tests := []struct {
		name    string
		config  BigQueryMetastoreConfig
		wantErr bool
	}{
		{
			name: "SimpleConfig",
			config: BigQueryMetastoreConfig{
				ProjectID: "proj",
				Region:    "reg",
				Warehouse: *warehouse,
				Credentials: map[string]any{
					"abc": "123",
					"def": "456",
					"ghi": map[string]any{
						"inner": true,
					},
				},
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

			var deserializedConfig BigQueryMetastoreConfig
			err = deserializedConfig.Deserialize(serialized)
			if (err != nil) != tt.wantErr {
				t.Errorf("Deserialize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(tt.config, deserializedConfig) {
				t.Errorf("Serialized/Deserialized config mismatch. got = %#v, want %#v", deserializedConfig, tt.config)
			}
		})
	}
}
