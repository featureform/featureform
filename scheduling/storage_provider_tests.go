package scheduling

import (
	"fmt"
	"testing"
)

func TestMemoryStorageProviderSet(t *testing.T) {
	tests := []struct {
		key   string
		value string
		err   error
	}{
		{"key1", "value1", nil},
		{"", "value1", fmt.Errorf("key cannot be empty")},
		{"key1", "", fmt.Errorf("value cannot be empty")},
	}

	var storageProvider MemoryStorageProvider

	for _, test := range tests {
		err := storageProvider.Set(test.key, test.value)
		if err != nil && err.Error() != test.err.Error() {
			t.Errorf("Set(%s, %s): expected error %v, got %v", test.key, test.value, test.err, err)
		}
	}
}

func TestMemoryStorageProviderGet(t *testing.T) {
	var storageProvider MemoryStorageProvider
	storageProvider.Set("key1", "value1")
	storageProvider.Set("key2", "value2")
	storageProvider.Set("prefix_key3", "value3")
	storageProvider.Set("prefix_key4", "value4")

	tests := []struct {
		key     string
		prefix  bool
		results []string
		err     error
	}{
		{"key1", false, []string{"value1"}, nil},
		{"key3", false, nil, &KeyNotFoundError{Key: "key3"}},
		{"prefix_key", true, []string{"value3", "value4"}, nil},
		{"prefix_key5", true, nil, &KeyNotFoundError{Key: "prefix_key5"}},
	}

	for _, test := range tests {
		results, err := storageProvider.Get(test.key, test.prefix)
		if err != nil && err.Error() != test.err.Error() {
			t.Errorf("Get(%s, %v): expected error %v, got %v", test.key, test.prefix, test.err, err)
		}
		if !compareStringSlices(results, test.results) {
			t.Errorf("Get(%s, %v): expected results %v, got %v", test.key, test.prefix, test.results, results)
		}
	}
}

func compareStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
