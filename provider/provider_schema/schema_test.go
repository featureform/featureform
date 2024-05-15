package provider_schema

import (
	"fmt"
	"testing"
)

func TestResourceToDirectoryPath(t *testing.T) {
	type testCase struct {
		resourceType string
		name         string
		variant      string
		expected     string
	}

	testCases := []testCase{
		{
			resourceType: "Primary",
			name:         "name",
			variant:      "variant",
			expected:     "featureform/Primary/name/variant",
		},
		{
			resourceType: "Transformation",
			name:         "name",
			variant:      "variant",
			expected:     "featureform/Transformation/name/variant",
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("ResourceToDirectoryPath(%s, %s, %s)", tc.resourceType, tc.name, tc.variant), func(t *testing.T) {
			actual := ResourceToDirectoryPath(tc.resourceType, tc.name, tc.variant)
			if actual != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, actual)
			}
		})
	}
}

func TestResourceToPicklePath(t *testing.T) {
	type testCase struct {
		name     string
		variant  string
		expected string
	}

	testCases := []testCase{
		{
			name:     "name",
			variant:  "variant",
			expected: "featureform/DFTransformations/name/variant/transformation.pkl",
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("ResourceToPicklePath(%s, %s)", tc.name, tc.variant), func(t *testing.T) {
			actual := ResourceToPicklePath(tc.name, tc.variant)
			if actual != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, actual)
			}
		})
	}
}
