// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package equivalence

import (
	"testing"
	"time"

	pb "github.com/featureform/metadata/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/stretchr/testify/assert"
)

func TestTrainingSetVariantIsEquivalent(t *testing.T) {
	tests := []struct {
		name     string
		ts1      trainingSetVariant
		ts2      Equivalencer
		expected bool
	}{
		{
			name: "Identical trainingSetVariants",
			ts1: trainingSetVariant{
				Name: "set1",
				Features: []nameVariant{
					{Name: "feature1", Variant: "v1"},
					{Name: "feature2", Variant: "v1"},
				},
				Label: nameVariant{Name: "label1", Variant: "v1"},
				LagFeatures: []featureLag{
					{Feature: "feature1", Name: "name1", Variant: "v1", Lag: "1h"},
				},
			},
			ts2: trainingSetVariant{
				Name: "set1",
				Features: []nameVariant{
					{Name: "feature1", Variant: "v1"},
					{Name: "feature2", Variant: "v1"},
				},
				Label: nameVariant{Name: "label1", Variant: "v1"},
				LagFeatures: []featureLag{
					{Feature: "feature1", Name: "name1", Variant: "v1", Lag: "1h"},
				},
			},
			expected: true,
		},
		{
			name: "Different Names",
			ts1: trainingSetVariant{
				Name: "set1",
				Features: []nameVariant{
					{Name: "feature1", Variant: "v1"},
					{Name: "feature2", Variant: "v1"},
				},
				Label: nameVariant{Name: "label1", Variant: "v1"},
				LagFeatures: []featureLag{
					{Feature: "feature1", Name: "name1", Variant: "v1", Lag: "1h"},
				},
			},
			ts2: trainingSetVariant{
				Name: "set2",
				Features: []nameVariant{
					{Name: "feature1", Variant: "v1"},
					{Name: "feature2", Variant: "v1"},
				},
				Label: nameVariant{Name: "label1", Variant: "v1"},
				LagFeatures: []featureLag{
					{Feature: "feature1", Name: "name1", Variant: "v1", Lag: "1h"},
				},
			},
			expected: false,
		},
		{
			name: "Different Features",
			ts1: trainingSetVariant{
				Name:        "set1",
				Features:    []nameVariant{{Name: "feature1", Variant: "v1"}},
				Label:       nameVariant{Name: "label1", Variant: "v1"},
				LagFeatures: []featureLag{},
			},
			ts2: trainingSetVariant{
				Name:        "set1",
				Features:    []nameVariant{{Name: "feature2", Variant: "v1"}}, // Different Feature
				Label:       nameVariant{Name: "label1", Variant: "v1"},
				LagFeatures: []featureLag{},
			},
			expected: false,
		},
		{
			name: "Different Labels",
			ts1: trainingSetVariant{
				Name:        "set1",
				Label:       nameVariant{Name: "label1", Variant: "v1"},
				Features:    []nameVariant{{Name: "feature2", Variant: "v1"}},
				LagFeatures: []featureLag{},
			},
			ts2: trainingSetVariant{
				Name:        "set1",
				Label:       nameVariant{Name: "label2", Variant: "v1"}, // Different Label
				Features:    []nameVariant{{Name: "feature2", Variant: "v1"}},
				LagFeatures: []featureLag{},
			},
			expected: false,
		},
		{
			name: "Different LagFeatures",
			ts1: trainingSetVariant{
				Name: "set1",
				LagFeatures: []featureLag{
					{Feature: "feature1", Name: "name1", Variant: "v1", Lag: "1h"},
				},
				Features: []nameVariant{{Name: "feature2", Variant: "v1"}},
				Label:    nameVariant{Name: "label1", Variant: "v1"},
			},
			ts2: trainingSetVariant{
				Name: "set1",
				LagFeatures: []featureLag{
					{Feature: "feature2", Name: "name1", Variant: "v1", Lag: "1h"}, // Different Lag Feature
				},
				Features: []nameVariant{{Name: "feature2", Variant: "v1"}},
				Label:    nameVariant{Name: "label1", Variant: "v1"},
			},
			expected: false,
		},
		{
			name: "Different Feature Order",
			ts1: trainingSetVariant{
				Name:        "set1",
				Features:    []nameVariant{{Name: "feature1", Variant: "v1"}, {Name: "feature2", Variant: "v1"}},
				Label:       nameVariant{Name: "label1", Variant: "v1"},
				LagFeatures: []featureLag{},
			},
			ts2: trainingSetVariant{
				Name:        "set1",
				Features:    []nameVariant{{Name: "feature2", Variant: "v1"}, {Name: "feature1", Variant: "v1"}}, // Different order
				Label:       nameVariant{Name: "label1", Variant: "v1"},
				LagFeatures: []featureLag{},
			},
			expected: false,
		},
		{
			name: "Different TrainingSetTypes",
			ts1: trainingSetVariant{
				Name: "set1",
				Features: []nameVariant{
					{Name: "feature1", Variant: "v1"},
					{Name: "feature2", Variant: "v1"},
				},
				Label: nameVariant{Name: "label1", Variant: "v1"},
				Type:  dynamicTrainingSet,
			},
			ts2: trainingSetVariant{
				Name: "set1",
				Features: []nameVariant{
					{Name: "feature1", Variant: "v1"},
					{Name: "feature2", Variant: "v1"},
				},
				Label: nameVariant{Name: "label1", Variant: "v1"},
				Type:  viewTrainingSet,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.ts1.IsEquivalent(tt.ts2)
			assert.Equal(t, tt.expected, result, "IsEquivalent() mismatch in test case: %s", tt.name)
		})
	}
}

func TestFeatureLagIsEquivalent(t *testing.T) {
	tests := []struct {
		name     string
		lag1     featureLag
		lag2     Equivalencer
		expected bool
	}{
		{
			name: "Identical featureLags",
			lag1: featureLag{
				Feature: "feature1",
				Name:    "name1",
				Variant: "v1",
				Lag:     "1h",
			},
			lag2: featureLag{
				Feature: "feature1",
				Name:    "name1",
				Variant: "v1",
				Lag:     "1h",
			},
			expected: true,
		},
		{
			name: "Different Feature names",
			lag1: featureLag{
				Feature: "feature1",
				Name:    "name1",
				Variant: "v1",
				Lag:     "1h",
			},
			lag2: featureLag{
				Feature: "feature2", // Different Feature
				Name:    "name1",
				Variant: "v1",
				Lag:     "1h",
			},
			expected: false,
		},
		{
			name: "Different lags",
			lag1: featureLag{
				Feature: "feature1",
				Name:    "name1",
				Variant: "v1",
				Lag:     "1h",
			},
			lag2: featureLag{
				Feature: "feature1",
				Name:    "name1",
				Variant: "v1",
				Lag:     "2h", // Different Lag
			},
			expected: false,
		},
		{
			name: "Different types",
			lag1: featureLag{
				Feature: "feature1",
				Name:    "name1",
				Variant: "v1",
				Lag:     "1h",
			},
			lag2: nameVariant{
				Name:    "name1",
				Variant: "v1",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.lag1.IsEquivalent(tt.lag2)
			assert.Equal(t, tt.expected, result, "IsEquivalent() mismatch in test case: %s", tt.name)
		})
	}
}

func TestTrainingSetVariantFromProto(t *testing.T) {
	tests := []struct {
		name     string
		input    *pb.TrainingSetVariant
		expected trainingSetVariant
		wantErr  bool
	}{
		{
			name: "complete training set variant with snowflake config",
			input: &pb.TrainingSetVariant{
				Name: "test_variant",
				Features: []*pb.NameVariant{
					{Name: "feature1"},
					{Name: "feature2", Variant: "v1"},
				},
				Label: &pb.NameVariant{
					Name: "target_label",
				},
				FeatureLags: []*pb.FeatureLag{
					{
						Feature: "feature1",
						Lag:     durationpb.New(time.Hour),
					},
				},
				ResourceSnowflakeConfig: &pb.ResourceSnowflakeConfig{
					Warehouse: "test_warehouse",
					DynamicTableConfig: &pb.SnowflakeDynamicTableConfig{
						TargetLag:   "1h",
						RefreshMode: pb.RefreshMode_REFRESH_MODE_FULL,
						Initialize:  pb.Initialize_INITIALIZE_ON_SCHEDULE,
					},
				},
				Type: pb.TrainingSetType_TRAINING_SET_TYPE_DYNAMIC,
			},
			expected: trainingSetVariant{
				Name: "test_variant",
				Features: []nameVariant{
					{Name: "feature1"},
					{Name: "feature2", Variant: "v1"},
				},
				Label: nameVariant{
					Name: "target_label",
				},
				LagFeatures: []featureLag{
					{
						Feature: "feature1",
						Lag:     durationpb.New(time.Hour).String(),
					},
				},
				ResourceSnowflakeConfig: resourceSnowflakeConfig{
					Warehouse: "test_warehouse",
					DynamicTableConfig: snowflakeDynamicTableConfig{
						TargetLag:   "1h",
						RefreshMode: "REFRESH_MODE_FULL",
						Initialize:  "INITIALIZE_ON_SCHEDULE",
					},
				},
				Type: dynamicTrainingSet,
			},
		},
		{
			name: "training set variant without snowflake config",
			input: &pb.TrainingSetVariant{
				Name: "no_snowflake",
				Features: []*pb.NameVariant{
					{Name: "feature1"},
					{Name: "feature2", Variant: "v1"},
				},
				Label: &pb.NameVariant{
					Name: "target_label",
				},
				FeatureLags: []*pb.FeatureLag{
					{
						Feature: "feature1",
						Lag:     durationpb.New(time.Hour),
					},
				},
				ResourceSnowflakeConfig: nil, // explicitly nil
				Type:                    pb.TrainingSetType_TRAINING_SET_TYPE_DYNAMIC,
			},
			expected: trainingSetVariant{
				Name: "no_snowflake",
				Features: []nameVariant{
					{Name: "feature1"},
					{Name: "feature2", Variant: "v1"},
				},
				Label: nameVariant{
					Name: "target_label",
				},
				LagFeatures: []featureLag{
					{
						Feature: "feature1",
						Lag:     durationpb.New(time.Hour).String(),
					},
				},
				ResourceSnowflakeConfig: resourceSnowflakeConfig{}, // empty config
				Type:                    dynamicTrainingSet,
			},
		},
		{
			name: "minimal training set variant",
			input: &pb.TrainingSetVariant{
				Name: "minimal_variant",
				Features: []*pb.NameVariant{
					{Name: "feature1"},
				},
				Label: &pb.NameVariant{
					Name: "label",
				},
				Type: pb.TrainingSetType_TRAINING_SET_TYPE_DYNAMIC,
			},
			expected: trainingSetVariant{
				Name: "minimal_variant",
				Features: []nameVariant{
					{Name: "feature1"},
				},
				Label: nameVariant{
					Name: "label",
				},
				ResourceSnowflakeConfig: resourceSnowflakeConfig{}, // empty config
				Type:                    dynamicTrainingSet,
			},
		},
		{
			name: "static training set variant",
			input: &pb.TrainingSetVariant{
				Name: "minimal_variant",
				Features: []*pb.NameVariant{
					{Name: "feature1"},
				},
				Label: &pb.NameVariant{
					Name: "label",
				},
				Type: pb.TrainingSetType_TRAINING_SET_TYPE_STATIC,
			},
			expected: trainingSetVariant{
				Name: "minimal_variant",
				Features: []nameVariant{
					{Name: "feature1"},
				},
				Label: nameVariant{
					Name: "label",
				},
				ResourceSnowflakeConfig: resourceSnowflakeConfig{}, // empty config
				Type:                    staticTrainingSet,
			},
		},
		{
			name: "view training set variant",
			input: &pb.TrainingSetVariant{
				Name: "minimal_variant",
				Features: []*pb.NameVariant{
					{Name: "feature1"},
				},
				Label: &pb.NameVariant{
					Name: "label",
				},
				Type: pb.TrainingSetType_TRAINING_SET_TYPE_VIEW,
			},
			expected: trainingSetVariant{
				Name: "minimal_variant",
				Features: []nameVariant{
					{Name: "feature1"},
				},
				Label: nameVariant{
					Name: "label",
				},
				ResourceSnowflakeConfig: resourceSnowflakeConfig{}, // empty config
				Type:                    viewTrainingSet,
			},
		},
		{
			name: "unset training set type",
			input: &pb.TrainingSetVariant{
				Name: "minimal_variant",
				Features: []*pb.NameVariant{
					{Name: "feature1"},
				},
				Label: &pb.NameVariant{
					Name: "label",
				},
			},
			expected: trainingSetVariant{
				Name: "minimal_variant",
				Features: []nameVariant{
					{Name: "feature1"},
				},
				Label: nameVariant{
					Name: "label",
				},
				ResourceSnowflakeConfig: resourceSnowflakeConfig{}, // empty config
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := TrainingSetVariantFromProto(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
