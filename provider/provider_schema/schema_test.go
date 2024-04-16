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

func TestTableNameToResource(t *testing.T) {
	tests := []struct {
		name            string
		tableName       string
		expectedType    string
		expectedName    string
		expectedVariant string
		expectError     bool
	}{
		{
			name:            "correct format",
			tableName:       "featureform_primary__name__variant",
			expectedType:    "Primary",
			expectedName:    "name",
			expectedVariant: "variant",
			expectError:     false,
		},
		{
			name:        "missing prefix",
			tableName:   "primary__name__variant",
			expectError: true,
		},
		{
			name:        "incorrect number of parts",
			tableName:   "featureform_primary__name",
			expectError: true,
		},
		{
			name:        "invalid resource type",
			tableName:   "featureform_invalid__name__variant",
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resourceType, name, variant, err := TableNameToResource(test.tableName)
			if (err != nil) != test.expectError {
				t.Errorf("Expected error: %v, got %v", test.expectError, err)
			}
			if !test.expectError {
				if resourceType != test.expectedType {
					t.Errorf("Expected resource type %s, got %s", test.expectedType, resourceType)
				}
				if name != test.expectedName {
					t.Errorf("Expected name %s, got %s", test.expectedName, name)
				}
				if variant != test.expectedVariant {
					t.Errorf("Expected variant %s, got %s", test.expectedVariant, variant)
				}
			}
		})
	}
}

func TestValidateResourceName(t *testing.T) {
	tests := []struct {
		name         string
		inputName    string
		inputVariant string
		expectError  bool
	}{
		{
			name:         "valid inputs",
			inputName:    "validName",
			inputVariant: "validVariant",
			expectError:  false,
		},
		{
			name:         "name with double underscores",
			inputName:    "invalid__name",
			inputVariant: "validVariant",
			expectError:  true,
		},
		{
			name:         "variant with double underscores",
			inputName:    "validName",
			inputVariant: "invalid__variant",
			expectError:  true,
		},
		{
			name:         "both with double underscores",
			inputName:    "invalid__name",
			inputVariant: "invalid__variant",
			expectError:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateResourceName(test.inputName, test.inputVariant)
			if (err != nil) != test.expectError {
				t.Errorf("Test %s failed. Expected error: %v, got %v", test.name, test.expectError, err)
			}
		})
	}
}
