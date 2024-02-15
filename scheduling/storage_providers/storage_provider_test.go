package scheduling

import (
	"fmt"
	"sort"
	"testing"
)

type StorageProviderTest struct {
	t       *testing.T
	storage StorageProvider
}

func (test *StorageProviderTest) Run() {
	t := test.t
	storage := test.storage

	testFns := map[string]func(*testing.T, StorageProvider){
		"SetStorageProvider": StorageProviderSet,
		"GetStorageProvider": StorageProviderGet,
	}

	for name, fn := range testFns {
		t.Run(name, func(t *testing.T) {
			fn(t, storage)
		})
	}
}

func StorageProviderSet(t *testing.T, provider StorageProvider) {
	type TestCase struct {
		key   string
		value string
		err   error
	}
	tests := map[string]TestCase{
		"Simple":     {"key1", "value1", nil},
		"EmptyKey":   {"", "value1", fmt.Errorf("key is empty")},
		"EmptyValue": {"key1", "", fmt.Errorf("value is empty for key key1")},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := provider.Set(test.key, test.value)
			if err != nil && err.Error() != test.err.Error() {
				t.Errorf("Set(%s, %s): expected error %v, got %v", test.key, test.value, test.err, err)
			}
		})
	}
}

func StorageProviderGet(t *testing.T, provider StorageProvider) {
	type TestCase struct {
		key     string
		prefix  bool
		results []string
		err     error
	}

	tests := map[string]TestCase{
		"Simple":            {"key1", false, []string{"value1"}, nil},
		"KeyNotFound":       {"key3", false, nil, &KeyNotFoundError{Key: "key3"}},
		"EmptyKey":          {"", false, nil, fmt.Errorf("key is empty")},
		"PrefixNotCalled":   {"prefix/key", false, nil, &KeyNotFoundError{Key: "prefix/key"}},
		"Prefix":            {"prefix/key", true, []string{"value3", "value4"}, nil},
		"PrefixKeyNotFound": {"prefix/key5", true, nil, &KeyNotFoundError{Key: "prefix/key5"}},
	}

	runTestCase := func(t *testing.T, test TestCase) {
		provider.Set("key1", "value1")
		provider.Set("key2", "value2")
		provider.Set("prefix/key3", "value3")
		provider.Set("prefix/key4", "value4")
		results, err := provider.Get(test.key, test.prefix)
		if err != nil && err.Error() != test.err.Error() {
			t.Errorf("Get(%s, %v): expected error %v, got %v", test.key, test.prefix, test.err, err)
		}
		if !compareStringSlices(results, test.results) {
			t.Errorf("Get(%s, %v): expected results %v, got %v", test.key, test.prefix, test.results, results)
		}
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			runTestCase(t, test)
		})
	}
}

func compareStringSlices(a, b []string) bool {
	sort.Strings(a)
	sort.Strings(b)
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
