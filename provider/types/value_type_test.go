package types

import (
	"encoding/json"
	"testing"
)

func TestSerializeDeserialize(t *testing.T) {
	numScalars := len(ScalarTypes)
	types := make([]ValueType, 0, numScalars*3)
	for scalar, _ := range ScalarTypes {
		types = append(types, scalar)
	}
	for i := 0; i < numScalars; i++ {
		tf := []bool{true, false}
		for _, isEmb := range tf {
			types = append(types, VectorType{
				ScalarType:  types[i].(ScalarType),
				Dimension:   128,
				IsEmbedding: isEmb,
			})
		}
	}
	for _, typ := range types {
		str := SerializeType(typ)
		desT, err := DeserializeType(str)
		if err != nil {
			t.Fatalf("Failed to serialize/deserialize %v\nSerialized: %s\n%s", typ, str, err.Error())
		}
		if typ != desT {
			t.Fatalf("Types not equal.\nFound: %v\nExpected: %v\n", desT, typ)
		}
	}
}

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
