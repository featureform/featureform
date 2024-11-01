// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package equivalence

import (
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
				Type:   "Type1",
			},
			lv2: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity1",
				Type:   "Type1",
			},
			expected: true,
		},
		{
			name: "Different Names",
			lv1: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity1",
				Type:   "Type1",
			},
			lv2: labelVariant{
				Name:   "Label2", // Different Name
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity1",
				Type:   "Type1",
			},
			expected: false,
		},
		{
			name: "Different Sources",
			lv1: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity1",
				Type:   "Type1",
			},
			lv2: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source2", Variant: "v1"}, // Different Source.Name
				Entity: "Entity1",
				Type:   "Type1",
			},
			expected: false,
		},
		{
			name: "Different Entities",
			lv1: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity1",
				Type:   "Type1",
			},
			lv2: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity2", // Different Entity
				Type:   "Type1",
			},
			expected: false,
		},
		{
			name: "Different Types",
			lv1: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity1",
				Type:   "Type1",
			},
			lv2: labelVariant{
				Name:   "Label1",
				Source: nameVariant{Name: "Source1", Variant: "v1"},
				Entity: "Entity1",
				Type:   "Type2", // Different Type
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
				Type:    "Type1",
			},
			lv2: labelVariant{
				Name:    "Label1",
				Source:  nameVariant{Name: "Source1", Variant: "v1"},
				Columns: column{Entity: "Entity2", Value: "Value2", Ts: "TS2"}, // Different Columns
				Entity:  "Entity1",
				Type:    "Type1",
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
