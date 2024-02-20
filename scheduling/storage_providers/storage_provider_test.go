package scheduling

import (
	"fmt"
	"sort"
	"testing"

	"github.com/google/uuid"
)

func TestMemoryStorageProvider(t *testing.T) {
	testFns := map[string]func(*testing.T){
		"SetStorageProvider":  StorageProviderSet,
		"GetStorageProvider":  StorageProviderGet,
		"ListStorageProvider": StorageProviderList,
	}

	for name, fn := range testFns {
		t.Run(name, func(t *testing.T) {
			fn(t)
		})
	}
}

func StorageProviderSet(t *testing.T) {
	provider := &MemoryStorageProvider{}
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

func StorageProviderGet(t *testing.T) {
	provider := &MemoryStorageProvider{}
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

func StorageProviderList(t *testing.T) {
	type TestCase struct {
		keys        []string
		prefix      string
		results     []string
		shouldError bool
	}

	tests := map[string]TestCase{
		"Single":      {[]string{"list/key1"}, "list", []string{"list/key1"}, false},
		"Multiple":    {[]string{"list/key1", "list/key2", "list/key3"}, "list", []string{"list/key1", "list/key2", "list/key3"}, false},
		"MixedPrefix": {[]string{"list/key1", "lost/key2", "list/key3"}, "list", []string{"list/key1", "list/key3"}, false},
		"EmptyPrefix": {[]string{"list/key1", "list/key2", "list/key3"}, "", []string{"list/key1", "list/key2", "list/key3"}, false},
	}

	runTestCase := func(t *testing.T, test TestCase) {
		provider := &MemoryStorageProvider{}
		for _, key := range test.keys {
			err := provider.Set(key, "value")
			if err != nil {
				t.Fatalf("could not set key: %v", err)
			}
		}

		results, err := provider.ListKeys(test.prefix)
		if err != nil {
			t.Fatalf("unable to list keys with prefix (%s): %v", test.prefix, err)
		}
		if len(results) != len(test.results) {
			t.Fatalf("Expected %d results, got %d results\nExpected List: %v, Got List: %v", len(test.results), len(results), test.results, results)
		}
		for !compareStringSlices(results, test.results) {
			t.Fatalf("Expected List: %v, Got List: %v", test.results, results)
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

func TestLockAndUnlock(t *testing.T) {
	provider := &MemoryStorageProvider{}

	id := uuid.New().String()
	key := "/tasks/metadata/task_id=1"

	// Test Lock
	err := provider.Lock(id, key)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Test Lock on already locked item with same UUID
	err = provider.Lock(id, key)
	if err != nil {
		t.Fatalf("Locking using the same id failed: %v", err)
	}

	// Test Lock on already locked item with different UUID
	diffId := uuid.New().String()
	err = provider.Lock(diffId, key)
	if err == nil {
		t.Fatalf("Locking using different id should have failed")
	}

	// Test UnLock with different UUID
	err = provider.Unlock(diffId, key)
	if err == nil {
		t.Fatalf("Unlocking using different id should have failed")
	}

	// Test UnLock with same UUID
	err = provider.Unlock(id, key)
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func TestLockAndUnlockWithGoRoutines(t *testing.T) {
	provider := &MemoryStorageProvider{}

	id := uuid.New().String()
	key := "/tasks/metadata/task_id=2"

	errChan := make(chan error)

	// Test Lock
	go lockGoRoutine(provider, id, key, errChan)
	err := <-errChan
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Test Lock on already locked item with same UUID
	go lockGoRoutine(provider, id, key, errChan)
	err = <-errChan
	if err != nil {
		t.Fatalf("Locking using the same id failed: %v", err)
	}

	// Test Lock on already locked item with different UUID
	diffId := uuid.New().String()
	go lockGoRoutine(provider, diffId, key, errChan)
	err = <-errChan
	if err == nil {
		t.Fatalf("Locking using different id should have failed")
	}

	// Test UnLock with different UUID
	go unlockGoRoutine(provider, diffId, key, errChan)
	err = <-errChan
	if err == nil {
		t.Fatalf("Unlocking using different id should have failed")
	}

	// Test UnLock with same UUID
	go unlockGoRoutine(provider, id, key, errChan)
	err = <-errChan
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func lockGoRoutine(provider *MemoryStorageProvider, id string, key string, errChan chan error) {
	err := provider.Lock(id, key)
	errChan <- err
}

func unlockGoRoutine(provider *MemoryStorageProvider, id string, key string, errChan chan error) {
	err := provider.Unlock(id, key)
	errChan <- err
}
