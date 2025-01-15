// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package equivalence

import (
	pb "github.com/featureform/metadata/proto"
	"github.com/featureform/provider/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLabelVariantIsEquivalent(t *testing.T) {
	tests := []struct {
		name     string
		lv1      labelVariant
		lv2      Equivalencer
		expected bool
	}{
		{
			name: "Identical labelVariants",
			lv1: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity1",
				Type:   types.Int8,
			},
			lv2: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity1",
				Type:   types.Int8,
			},
			expected: true,
		},
		{
			name: "Different Names",
			lv1: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity1",
				Type:   types.Int8,
			},
			lv2: labelVariant{
				Name:   "Label2", // Different Name
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity1",
				Type:   types.Int8,
			},
			expected: false,
		},
		{
			name: "Different Sources",
			lv1: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity1",
				Type:   types.Int8,
			},
			lv2: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source2", Variant: "v1"}, // Different Source.Name
				Entity: "Entity1",
				Type:   types.Int8,
			},
			expected: false,
		},
		{
			name: "Different Entities",
			lv1: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity1",
				Type:   types.Int8,
			},
			lv2: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity2", // Different Entity
				Type:   types.Int8,
			},
			expected: false,
		},
		{
			name: "Different Types",
			lv1: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity1",
				Type:   types.Int8,
			},
			lv2: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity1",
				Type:   types.Int16, // Different Type
			},
			expected: false,
		},
		{
			name: "Different Columns",
			lv1: labelVariant{
				Name:    "Label1",
				Source:  nameVariant{Name: "Source1", Variant: "v1"},
				Columns: column{Entity: "Entity1", Value: "Value1", Ts: "TS1"},
				Entity:  "Entity1",
				Type:    types.Int8,
			},
			lv2: labelVariant{
				Name:    "Label1",
				Source:  nameVariant{Name: "Source1", Variant: "v1"},
				Columns: column{Entity: "Entity2", Value: "Value2", Ts: "TS2"}, // Different Columns
				Entity:  "Entity1",
				Type:    types.Int8,
			},
			expected: true, // Columns are not compared
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.lv1.IsEquivalent(tt.lv2)
			assert.Equal(t, tt.expected, result, "IsEquivalent() mismatch in test case: %s", tt.name)
		})
	}
}

func TestLabelVariantFromProto(t *testing.T) {
	valueType := &pb.ValueType{
		Type: &pb.ValueType_Scalar{
			Scalar: pb.ScalarType_FLOAT32,
		},
	}

	vt, err := types.ValueTypeFromProto(valueType)
	assert.NoError(t, err)

	tests := []struct {
		name     string
		input    *pb.LabelVariant
		expected labelVariant
		wantErr  bool
	}{
		{
			name: "complete label variant with snowflake config",
			input: &pb.LabelVariant{
				Name:   "churn_label",
				Entity: "user_id",
				Type:   valueType,
				Source: &pb.NameVariant{
					Name:    "churn_events",
					Variant: "v1",
				},
				ResourceSnowflakeConfig: &pb.ResourceSnowflakeConfig{
					Warehouse: "compute_wh",
					DynamicTableConfig: &pb.SnowflakeDynamicTableConfig{
						TargetLag:   "1h",
						RefreshMode: pb.RefreshMode_REFRESH_MODE_FULL,
						Initialize:  pb.Initialize_INITIALIZE_ON_SCHEDULE,
					},
				},
			},
			expected: labelVariant{
				Name:   "churn_label",
				Entity: "user_id",
				Type:   vt,
				Source: nameVariant{
					Name:    "churn_events",
					Variant: "v1",
				},
				ResourceSnowflakeConfig: resourceSnowflakeConfig{
					Warehouse: "compute_wh",
					DynamicTableConfig: snowflakeDynamicTableConfig{
						TargetLag:   "1h",
						RefreshMode: "REFRESH_MODE_FULL",
						Initialize:  "INITIALIZE_ON_SCHEDULE",
					},
				},
			},
		},
		{
			name: "minimal label variant without snowflake config",
			input: &pb.LabelVariant{
				Name:   "simple_label",
				Entity: "customer_id",
				Type:   valueType,
				Source: &pb.NameVariant{
					Name: "revenue_data",
				},
			},
			expected: labelVariant{
				Name:   "simple_label",
				Entity: "customer_id",
				Type:   vt,
				Source: nameVariant{
					Name: "revenue_data",
				},
				ResourceSnowflakeConfig: resourceSnowflakeConfig{},
			},
		},
		{
			name: "label variant with source only",
			input: &pb.LabelVariant{
				Name:   "categorical_label",
				Entity: "product_id",
				Type:   valueType,
				Source: &pb.NameVariant{
					Name:    "product_categories",
					Variant: "latest",
				},
			},
			expected: labelVariant{
				Name:   "categorical_label",
				Entity: "product_id",
				Type:   vt,
				Source: nameVariant{
					Name:    "product_categories",
					Variant: "latest",
				},
				ResourceSnowflakeConfig: resourceSnowflakeConfig{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := LabelVariantFromProto(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
