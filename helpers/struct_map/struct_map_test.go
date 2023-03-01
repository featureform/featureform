package struct_map

import (
	"reflect"
	"testing"
)

func TestNewStructMap(t *testing.T) {
	type emptyStruct struct{}

	type args struct {
		s interface{}
	}
	tests := []struct {
		name        string
		args        args
		expected    *StructMap
		expectedErr bool
	}{
		{"Non Struct", args{make(map[string]string)}, nil, true},
		{"Empty Struct", args{emptyStruct{}}, &StructMap{
			size: 0,
			keys: []string{},
			val:  reflect.ValueOf(emptyStruct{}),
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := NewStructMap(tt.args.s)
			if (err != nil) != tt.expectedErr {
				t.Errorf("NewStructMap() error = %v, expectedErr %v", err, tt.expectedErr)
				return
			}
			if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("NewStructMap() actual = %v, expected %v", actual, tt.expected)
			}
		})
	}
}

func TestStructMapGet(t *testing.T) {
	type testCase struct {
		actual   interface{}
		expected map[string]interface{}
	}

	testCases := map[string]testCase{
		"Empty Struct": {
			actual: struct{}{},
			expected: map[string]interface{}{
				"Size": 0,
				"Keys": []string{},
			},
		},
		"Non-Empty Struct": {
			actual: struct {
				A string
				B string
			}{A: "a", B: "b"},
			expected: map[string]interface{}{
				"Size": 2,
				"Keys": []string{"A", "B"},
				"A":    "a",
				"B":    "b",
			},
		},
		"Non-Empty Struct with Private Fields": {
			actual: struct {
				A string
				B string
				c string
			}{A: "a", B: "b"},
			expected: map[string]interface{}{
				"Size": 2,
				"Keys": []string{"A", "B"},
				"A":    "a",
				"B":    "b",
			},
		},
	}

	for name, c := range testCases {
		t.Run(name, func(t *testing.T) {
			sMap, err := NewStructMap(c.actual)
			if err != nil {
				t.Errorf("could not create struct map: %v", err)
			}

			if sMap.Size() != c.expected["Size"] {
				t.Errorf("Expected size of %v but received %v", c.expected["Size"], sMap.Size())
			}

			if !reflect.DeepEqual(sMap.Keys(), c.expected["Keys"]) {
				t.Errorf("Expected keys %v but received %v", c.expected["Keys"], sMap.Keys())
			}

			for _, k := range sMap.Keys() {
				val, ok := sMap.Get(k)
				if !ok {
					t.Errorf("Expected key %v to be in struct map", k)
				}
				if !sMap.Has(k, c.expected[k]) {
					t.Errorf("Expect value %v but received %v", c.expected[k], val)
				}
			}
		})
	}
}
