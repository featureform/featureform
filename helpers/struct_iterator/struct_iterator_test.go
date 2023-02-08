package struct_iterator

import (
	"reflect"
	"testing"
)

func TestNewStructIterator(t *testing.T) {
	type emptyStruct struct{}

	type args struct {
		s interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    *StructIterator
		wantErr bool
	}{
		{"Non Struct", args{make(map[string]string)}, nil, true},
		{"Empty Struct", args{emptyStruct{}}, &StructIterator{
			idx:      -1,
			numField: 0,
			val:      reflect.ValueOf(emptyStruct{}),
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewStructIterator(tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewStructIterator() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewStructIterator() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStructIteratorIterate(t *testing.T) {
	type testCase struct {
		given    interface{}
		expected map[string]interface{}
	}

	testCases := map[string]testCase{
		"Empty": {
			given:    struct{}{},
			expected: map[string]interface{}{},
		},
		"Simple": {
			given: struct {
				Field1 string
			}{"1"},
			expected: map[string]interface{}{
				"Field1": "1",
			},
		},
		"Multiple": {
			given: struct {
				Field1 string
				Field2 int
				Field3 bool
			}{"1", 1, true},
			expected: map[string]interface{}{
				"Field1": "1",
				"Field2": 1,
				"Field3": true,
			},
		},
		"Unexported Fields": {
			given: struct {
				field1 string
			}{"1"},
			expected: map[string]interface{}{},
		},
		"Mixed Fields": {
			given: struct {
				field1 string
				Field2 string
				field3 string
				field4 string
				Field5 string
				field6 string
			}{"1", "2", "3", "4", "5", "6"},
			expected: map[string]interface{}{
				"Field2": "2",
				"Field5": "5",
			},
		},
	}

	checkValues := func(key string, value interface{}, expected map[string]interface{}, t *testing.T) {
		if expVal, ok := expected[key]; ok {
			if expVal != value {
				t.Errorf("Expected value %v, got %v for key %s", expVal, value, key)
			}
		} else {
			t.Errorf("Key %s not in expected results", key)
		}
	}

	for name, c := range testCases {
		t.Run(name, func(t *testing.T) {
			iterator, err := NewStructIterator(c.given)
			if err != nil {
				t.Errorf("could not create iterator: %v", err)
			}
			for iterator.Next() {
				checkValues(iterator.Key(), iterator.Value(), c.expected, t)
			}
		})
	}
}
