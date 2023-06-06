package provider

import (
	"encoding/json"
	"testing"
)

func TestVectorTypeUnmarshaling(t *testing.T) {
	type testCase struct {
		serialized []byte
		expected   ValueType
		expectErr  bool
	}

	cases := []testCase{
		{
			serialized: []byte(`{"ValueType":{"ScalarType":"float32","Dimension":384,"IsEmbedding":true}}`),
			expected:   VectorType{ScalarType: Float32, Dimension: 384, IsEmbedding: true},
			expectErr:  false,
		},
		{
			serialized: []byte(`{"ValueType":"float32"}`),
			expected:   Float32,
			expectErr:  false,
		},
		{
			serialized: []byte(`{"ValueType":{"ScalarType":"float32","Dimension":384,"IsEmbedding":true}}`),
			expected:   Float32,
			expectErr:  true,
		},
	}

	for _, c := range cases {
		vt := ValueTypeJSONWrapper{}
		err := vt.UnmarshalJSON(c.serialized)
		if err != nil {
			t.Errorf("failed to unmarshal value type due to unexpected error: %v", err)
		}
		if vt.ValueType != c.expected && !c.expectErr {
			t.Errorf("expected %v, got %v", c.expected, vt.ValueType)
		}
	}
}

func TestVectorTypeMarshaling(t *testing.T) {
	type testCase struct {
		wrapped  ValueTypeJSONWrapper
		expected []byte
	}

	cases := []testCase{
		{
			wrapped:  ValueTypeJSONWrapper{ValueType: VectorType{ScalarType: Float32, Dimension: 384, IsEmbedding: true}},
			expected: []byte(`{"ValueType":{"ScalarType":"float32","Dimension":384,"IsEmbedding":true}}`),
		},
		{
			wrapped:  ValueTypeJSONWrapper{ValueType: Float32},
			expected: []byte(`{"ValueType":"float32"}`),
		},
	}

	for _, c := range cases {
		serialized, err := json.Marshal(c.wrapped)
		if err != nil {
			t.Errorf("failed to marshal value type due to unexpected error: %v", err)
		}
		if string(serialized) != string(c.expected) {
			t.Errorf("expected %v, got %v", c.expected, serialized)
		}
	}
}
