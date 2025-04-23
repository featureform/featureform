// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package equivalence

import (
	"testing"

	pb "github.com/featureform/metadata/proto"

	"github.com/featureform/provider/types"
	"github.com/stretchr/testify/assert"
)

func TestFeatureVariantIsEquivalent(t *testing.T) {

	tests := []struct {
		name     string
		fv1      featureVariant
		fv2      Equivalencer
		expected bool
	}{
		{
			name: "Identical featureVariants with Column location",
			fv1: featureVariant{
				Name:            "Feature1",
				Provider:        "Provider1",
				ValueType:       types.Int8,
				EntityColumn:    "user_id",
				ComputationMode: "Mode1",
				Location: column{
					Entity: "Entity1",
					Value:  "Value1",
					Ts:     "Timestamp1",
				},
			},
			fv2: featureVariant{
				Name:            "Feature1",
				Provider:        "Provider1",
				ValueType:       types.Int8,
				EntityColumn:    "user_id",
				ComputationMode: "Mode1",
				Location: column{
					Entity: "Entity1",
					Value:  "Value1",
					Ts:     "Timestamp1",
				},
			},
			expected: true,
		},
		{
			name: "Different EntityColumn",
			fv1: featureVariant{
				Name:            "Feature1",
				Provider:        "Provider1",
				ValueType:       types.Int8,
				EntityColumn:    "user_id",
				ComputationMode: "Mode1",
				Location: column{
					Entity: "Entity1",
					Value:  "Value1",
					Ts:     "Timestamp1",
				},
			},
			fv2: featureVariant{
				Name:            "Feature1",
				Provider:        "Provider1",
				ValueType:       types.Int8,
				EntityColumn:    "customer_id",
				ComputationMode: "Mode1",
				Location: column{
					Entity: "Entity1",
					Value:  "Value1",
					Ts:     "Timestamp1",
				},
			},
			expected: false,
		},
		{
			name: "Different Names",
			fv1: featureVariant{
				Name:            "Feature1",
				Provider:        "Provider1",
				ValueType:       types.Int8,
				EntityColumn:    "user_id",
				ComputationMode: "Mode1",
				Location: column{
					Entity: "Entity1",
					Value:  "Value1",
					Ts:     "Timestamp1",
				},
			},
			fv2: featureVariant{
				Name:            "Feature2", // Different Name
				Provider:        "Provider1",
				ValueType:       types.Int8,
				EntityColumn:    "user_id",
				ComputationMode: "Mode1",
				Location: column{
					Entity: "Entity1",
					Value:  "Value1",
					Ts:     "Timestamp1",
				},
			},
			expected: false,
		},
		{
			name: "Different Providers",
			fv1: featureVariant{
				Name:            "Feature1",
				Provider:        "Provider1",
				ValueType:       types.Int8,
				EntityColumn:    "user_id",
				ComputationMode: "Mode1",
				Location: column{
					Entity: "Entity1",
					Value:  "Value1",
					Ts:     "Timestamp1",
				},
			},
			fv2: featureVariant{
				Name:            "Feature1",
				Provider:        "Provider2", // Different Provider
				ValueType:       types.Int8,
				EntityColumn:    "user_id",
				ComputationMode: "Mode1",
				Location: column{
					Entity: "Entity1",
					Value:  "Value1",
					Ts:     "Timestamp1",
				},
			},
			expected: false,
		},
		{
			name: "Different ValueTypes",
			fv1: featureVariant{
				Name:            "Feature1",
				Provider:        "Provider1",
				ValueType:       types.Int8,
				EntityColumn:    "user_id",
				ComputationMode: "Mode1",
				Location: column{
					Entity: "Entity1",
					Value:  "Value1",
					Ts:     "Timestamp1",
				},
			},
			fv2: featureVariant{
				Name:            "Feature1",
				Provider:        "Provider1",
				ValueType:       types.Int16,
				EntityColumn:    "user_id",
				ComputationMode: "Mode1",
				Location: column{
					Entity: "Entity1",
					Value:  "Value1",
					Ts:     "Timestamp1",
				},
			},
			expected: false,
		},
		{
			name: "Different ComputationModes",
			fv1: featureVariant{
				Name:            "Feature1",
				Provider:        "Provider1",
				ValueType:       types.Int8,
				EntityColumn:    "user_id",
				ComputationMode: "Mode1",
				Location: column{
					Entity: "Entity1",
					Value:  "Value1",
					Ts:     "Timestamp1",
				},
			},
			fv2: featureVariant{
				Name:            "Feature1",
				Provider:        "Provider1",
				ValueType:       types.Int8,
				EntityColumn:    "user_id",
				ComputationMode: "Mode2",
				Location: column{
					Entity: "Entity1",
					Value:  "Value1",
					Ts:     "Timestamp1",
				},
			},
			expected: false,
		},
		{
			name: "Different Locations (Column vs PythonFunction)",
			fv1: featureVariant{
				Name:         "Feature1",
				EntityColumn: "user_id",
				Location:     column{Entity: "Entity1", Value: "Value1", Ts: "Timestamp1"},
			},
			fv2: featureVariant{
				Name:         "Feature1",
				EntityColumn: "user_id",
				Location:     pythonFunction{Query: []byte("SELECT * FROM table")},
			},
			expected: false,
		},
		{
			name: "Identical featureVariants with PythonFunction location",
			fv1: featureVariant{
				Name:         "Feature1",
				Provider:     "Provider1",
				EntityColumn: "user_id",
				Location:     pythonFunction{Query: []byte("SELECT * FROM table")},
			},
			fv2: featureVariant{
				Name:         "Feature1",
				Provider:     "Provider1",
				EntityColumn: "user_id",
				Location:     pythonFunction{Query: []byte("SELECT * FROM table")},
			},
			expected: true,
		},
		{
			name: "Different PythonFunction queries",
			fv1: featureVariant{
				Name:         "Feature1",
				EntityColumn: "user_id",
				Location:     pythonFunction{Query: []byte("SELECT * FROM table1")},
			},
			fv2: featureVariant{
				Name:         "Feature1",
				EntityColumn: "user_id",
				Location:     pythonFunction{Query: []byte("SELECT * FROM table2")}, // Different Query
			},
			expected: false,
		},
		{
			name: "Identical featureVariants with Stream location",
			fv1: featureVariant{
				Name:         "Feature1",
				Provider:     "Provider1",
				EntityColumn: "user_id",
				Location:     stream{OfflineProvider: "OfflineProvider1"},
			},
			fv2: featureVariant{
				Name:         "Feature1",
				Provider:     "Provider1",
				EntityColumn: "user_id",
				Location:     stream{OfflineProvider: "OfflineProvider1"},
			},
			expected: true,
		},
		{
			name: "Different Stream offlineProviders",
			fv1: featureVariant{
				Name:         "Feature1",
				EntityColumn: "user_id",
				Location:     stream{OfflineProvider: "OfflineProvider1"},
				// ... other fields
			},
			fv2: featureVariant{
				Name:         "Feature1",
				EntityColumn: "user_id",
				Location:     stream{OfflineProvider: "OfflineProvider2"}, // Different OfflineProvider
				// ... other fields
			},
			expected: false,
		},
		{
			name: "Different Types",
			fv1: featureVariant{
				Name:         "Feature1",
				EntityColumn: "user_id",
				// ... other fields
			},
			fv2: column{
				Entity: "Entity1",
				Value:  "Value1",
				Ts:     "Timestamp1",
			}, // Different type
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.fv1.IsEquivalent(tt.fv2)
			assert.Equal(t, tt.expected, result, "IsEquivalent() mismatch in test case: %s", tt.name)
		})
	}
}

func TestColumnIsEquivalent(t *testing.T) {
	tests := []struct {
		name     string
		c1       column
		c2       Equivalencer
		expected bool
	}{
		{
			name:     "Identical columns",
			c1:       column{Entity: "Entity1", Value: "Value1", Ts: "Timestamp1"},
			c2:       column{Entity: "Entity1", Value: "Value1", Ts: "Timestamp1"},
			expected: true,
		},
		{
			name:     "Different entities",
			c1:       column{Entity: "Entity1", Value: "Value1", Ts: "Timestamp1"},
			c2:       column{Entity: "Entity2", Value: "Value1", Ts: "Timestamp1"}, // Different Entity
			expected: false,
		},
		{
			name:     "Different values",
			c1:       column{Entity: "Entity1", Value: "Value1", Ts: "Timestamp1"},
			c2:       column{Entity: "Entity1", Value: "Value2", Ts: "Timestamp1"}, // Different value
			expected: false,
		},
		{
			name:     "Different timestamps",
			c1:       column{Entity: "Entity1", Value: "Value1", Ts: "Timestamp1"},
			c2:       column{Entity: "Entity1", Value: "Value1", Ts: "Timestamp2"}, // Different ts
			expected: false,
		},
		{
			name:     "Different types",
			c1:       column{Entity: "Entity1", Value: "Value1", Ts: "Timestamp1"},
			c2:       pythonFunction{Query: []byte("SELECT * FROM table")}, // Different type
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.c1.IsEquivalent(tt.c2)
			assert.Equal(t, tt.expected, result, "IsEquivalent() mismatch in test case: %s", tt.name)
		})
	}
}

func TestPythonFunctionIsEquivalent(t *testing.T) {
	tests := []struct {
		name     string
		pf1      pythonFunction
		pf2      Equivalencer
		expected bool
	}{
		{
			name:     "Identical pythonFunctions",
			pf1:      pythonFunction{Query: []byte("SELECT * FROM table")},
			pf2:      pythonFunction{Query: []byte("SELECT * FROM table")},
			expected: true,
		},
		{
			name:     "Different queries",
			pf1:      pythonFunction{Query: []byte("SELECT * FROM table1")},
			pf2:      pythonFunction{Query: []byte("SELECT * FROM table2")}, // Different Query
			expected: false,
		},
		{
			name:     "Different types",
			pf1:      pythonFunction{Query: []byte("SELECT * FROM table")},
			pf2:      stream{OfflineProvider: "OfflineProvider1"}, // Different type
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.pf1.IsEquivalent(tt.pf2)
			assert.Equal(t, tt.expected, result, "IsEquivalent() mismatch in test case: %s", tt.name)
		})
	}
}

func TestStreamIsEquivalent(t *testing.T) {
	tests := []struct {
		name     string
		s1       stream
		s2       Equivalencer
		expected bool
	}{
		{
			name:     "Identical streams",
			s1:       stream{OfflineProvider: "OfflineProvider1"},
			s2:       stream{OfflineProvider: "OfflineProvider1"},
			expected: true,
		},
		{
			name:     "Different offlineProviders",
			s1:       stream{OfflineProvider: "OfflineProvider1"},
			s2:       stream{OfflineProvider: "OfflineProvider2"}, // Different OfflineProvider
			expected: false,
		},
		{
			name:     "Different types",
			s1:       stream{OfflineProvider: "OfflineProvider1"},
			s2:       column{Entity: "Entity1", Value: "Value1", Ts: "Timestamp1"}, // Different type
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.s1.IsEquivalent(tt.s2)
			assert.Equal(t, tt.expected, result, "IsEquivalent() mismatch in test case: %s", tt.name)
		})
	}
}

func TestFeatureVariantFromProto(t *testing.T) {
	valueType := &pb.ValueType{
		Type: &pb.ValueType_Scalar{
			Scalar: pb.ScalarType_FLOAT32,
		},
	}

	vt, err := types.ValueTypeFromProto(valueType)
	assert.NoError(t, err)
	tests := []struct {
		name     string
		input    *pb.FeatureVariant
		expected featureVariant
		wantErr  bool
	}{
		{
			name: "column based feature",
			input: &pb.FeatureVariant{
				Name:     "user_spend",
				Provider: "snowflake",
				Type:     valueType,
				Entity:   "user_id",
				Mode:     pb.ComputationMode_PRECOMPUTED,
				Location: &pb.FeatureVariant_Columns{
					Columns: &pb.Columns{
						Entity: "user_id",
						Value:  "spend_amount",
						Ts:     "event_timestamp",
					},
				},
				ResourceSnowflakeConfig: &pb.ResourceSnowflakeConfig{
					Warehouse: "compute_wh",
				},
			},
			expected: featureVariant{
				Name:            "user_spend",
				Provider:        "snowflake",
				ValueType:       vt,
				EntityColumn:    "user_id",
				ComputationMode: "PRECOMPUTED",
				Location: column{
					Entity: "user_id",
					Value:  "spend_amount",
					Ts:     "event_timestamp",
				},
				ResourceSnowflakeConfig: resourceSnowflakeConfig{
					Warehouse: "compute_wh",
				},
			},
		},
		{
			name: "python function feature",
			input: &pb.FeatureVariant{
				Name:     "user_category",
				Provider: "python",
				Type:     valueType,
				Entity:   "user_id",
				Mode:     pb.ComputationMode_PRECOMPUTED,
				Location: &pb.FeatureVariant_Function{
					Function: &pb.PythonFunction{
						Query: []byte("def compute(row): return row['category'].upper()"),
					},
				},
			},
			expected: featureVariant{
				Name:            "user_category",
				Provider:        "python",
				ValueType:       vt,
				EntityColumn:    "user_id",
				ComputationMode: "PRECOMPUTED",
				Location: pythonFunction{
					Query: []byte("def compute(row): return row['category'].upper()"),
				},
				ResourceSnowflakeConfig: resourceSnowflakeConfig{},
			},
		},
		{
			name: "stream feature",
			input: &pb.FeatureVariant{
				Name:     "click_count",
				Provider: "kafka",
				Type:     valueType,
				Entity:   "user_id",
				Mode:     pb.ComputationMode_STREAMING,
				Location: &pb.FeatureVariant_Stream{
					Stream: &pb.Stream{
						OfflineProvider: "snowflake",
					},
				},
			},
			expected: featureVariant{
				Name:            "click_count",
				Provider:        "kafka",
				ValueType:       vt,
				EntityColumn:    "user_id",
				ComputationMode: "STREAMING",
				Location: stream{
					OfflineProvider: "snowflake",
				},
				ResourceSnowflakeConfig: resourceSnowflakeConfig{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := FeatureVariantFromProto(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
