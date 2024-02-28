package scheduling

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

type StorageProviderTest struct {
	t       *testing.T
	storage StorageProvider
}

func (test *StorageProviderTest) Run() {
	t := test.t
	storage := test.storage

	testFns := map[string]func(*testing.T, StorageProvider){
		// "SetStorageProvider":          StorageProviderSet,
		// "GetStorageProvider":          StorageProviderGet,
		// "ListStorageProvider":         StorageProviderList,
		"LockAndUnlock": LockAndUnlock,
		// "LockTimeUpdates":             LockTimeUpdates,
		// "LockAndUnlockWithGoRoutines": LockAndUnlockWithGoRoutines,
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
		"Simple":     {"setTest/key1", "value1", nil},
		"EmptyKey":   {"", "value1", fmt.Errorf("key is empty")},
		"EmptyValue": {"setTest/key1", "", fmt.Errorf("value is empty for key setTest/key1")},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			lockObject, err := provider.Lock(test.key)
			if test.err != nil && err != nil && err.Error() == test.err.Error() {
				// continue to next test case
				return
			}
			if err != nil {
				t.Errorf("could not lock key: %v", err)
			}
			err = provider.Set(test.key, test.value, lockObject)
			if err != nil && err.Error() != test.err.Error() {
				t.Errorf("Set(%s, %s): expected error %v, got %v", test.key, test.value, test.err, err)
			}

			err = provider.Delete(test.key, lockObject)
			if err != nil {
				t.Errorf("could not delete key: %v", err)
			}
		})
	}
}

func StorageProviderGet(t *testing.T, provider StorageProvider) {
	type TestCase struct {
		key     string
		prefix  bool
		results map[string]string
		err     error
	}

	tests := map[string]TestCase{
		"Simple":            {"key1", false, map[string]string{"key1": "value1"}, nil},
		"KeyNotFound":       {"key3", false, nil, &KeyNotFoundError{Key: "key3"}},
		"EmptyKey":          {"", false, nil, fmt.Errorf("key is empty")},
		"PrefixNotCalled":   {"prefix/key", false, nil, &KeyNotFoundError{Key: "prefix/key"}},
		"Prefix":            {"prefix/key", true, map[string]string{"prefix/key3": "value3", "prefix/key4": "value4"}, nil},
		"PrefixKeyNotFound": {"prefix/key5", true, nil, &KeyNotFoundError{Key: "prefix/key5"}},
	}

	runTestCase := func(t *testing.T, test TestCase) {
		lockObject1, _ := provider.Lock("key1")
		provider.Set("key1", "value1", lockObject1)

		lockObject2, _ := provider.Lock("key2")
		provider.Set("key2", "value2", lockObject2)

		lockObject3, _ := provider.Lock("prefix/key3")
		provider.Set("prefix/key3", "value3", lockObject3)

		lockObject4, _ := provider.Lock("prefix/key4")
		provider.Set("prefix/key4", "value4", lockObject4)

		results, err := provider.Get(test.key, test.prefix)
		if err != nil && err.Error() != test.err.Error() {
			t.Errorf("Get(%s, %v): expected error %v, got %v", test.key, test.prefix, test.err, err)
		}
		if !compareMaps(results, test.results) {
			t.Errorf("Get(%s, %v): expected results %v, got %v", test.key, test.prefix, test.results, results)
		}

		err = provider.Delete("key1", lockObject1)
		if err != nil {
			t.Fatalf("could not delete key1: %v", err)
		}

		err = provider.Delete("key2", lockObject2)
		if err != nil {
			t.Fatalf("could not delete key2: %v", err)
		}

		err = provider.Delete("prefix/key3", lockObject3)
		if err != nil {
			t.Fatalf("could not delete prefix/key3: %v", err)
		}

		err = provider.Delete("prefix/key4", lockObject4)
		if err != nil {
			t.Fatalf("could not delete prefix/key4: %v", err)
		}
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			runTestCase(t, test)
		})
	}
}

func StorageProviderList(t *testing.T, provider StorageProvider) {
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
		for _, key := range test.keys {
			lockObject, err := provider.Lock(key)
			if err != nil {
				t.Fatalf("could not lock key: %v", err)
			}
			err = provider.Set(key, "value", lockObject)
			if err != nil {
				t.Fatalf("could not set key: %v", err)
			}
			if err = provider.Unlock(key, lockObject); err != nil {
				t.Fatalf("could not unlock key: %v", err)
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

		for _, key := range test.keys {
			lockObject, err := provider.Lock(key)
			if err != nil {
				t.Fatalf("could not lock key: %v", err)
			}
			err = provider.Delete(key, lockObject)
			if err != nil {
				t.Fatalf("could not set key: %v", err)
			}
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

func compareMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func LockAndUnlock(t *testing.T, provider StorageProvider) {
	key := "/tasks/metadata/task_id=1"

	// Test Lock
	lock, err := provider.Lock(key)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Test Lock on already locked item
	diffLock, err := provider.Lock(key)
	if err == nil {
		t.Fatalf("Locking using different id should have failed")
	}

	// Test Unlock with different lock
	err = provider.Unlock(key, diffLock)
	if err == nil {
		t.Fatalf("Unlocking using different id should have failed")
	}

	// Test Unlock with original lock
	err = provider.Unlock(key, lock)
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	// Lock & Delete key
	lock, err = provider.Lock(key)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	err = provider.Delete(key, lock)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

func LockAndUnlockWithGoRoutines(t *testing.T, provider StorageProvider) {
	key := "/tasks/metadata/task_id=2"
	lockChannel := make(chan LockObject)
	errChan := make(chan error)

	// Test Lock
	go lockGoRoutine(provider, key, lockChannel, errChan)
	lock := <-lockChannel
	err := <-errChan
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Test Lock on already locked item
	diffLockChannel := make(chan LockObject)
	go lockGoRoutine(provider, key, diffLockChannel, errChan)
	diffLock := <-diffLockChannel
	err = <-errChan
	if err == nil {
		t.Fatalf("Locking using different id should have failed")
	}

	// Test UnLock with different UUID
	go unlockGoRoutine(provider, diffLock, key, errChan)
	err = <-errChan
	if err == nil {
		t.Fatalf("Unlocking using different id should have failed")
	}

	// Test Unlock with original lock
	go unlockGoRoutine(provider, lock, key, errChan)
	err = <-errChan
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	// Lock & Delete key
	lock, err = provider.Lock(key)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	err = provider.Delete(key, lock)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

func LockTimeUpdates(t *testing.T, provider StorageProvider) {
	key := "/tasks/metadata/task_id=3"
	lock, err := provider.Lock(key)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	err = provider.Set(key, "value", lock)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Wait for 2 x ValidTimePeriod
	time.Sleep(2 * ValidTimePeriod)

	// Set the value to something else
	err = provider.Set(key, "value2", lock)

	// Check if the value has been updated
	results, err := provider.Get(key, false)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if results[key] != "value2" {
		t.Fatalf("Value not updated")
	}

	err = provider.Delete(key, lock)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Test the lock has been released
	err = provider.Set(key, "value3", lock)
	if err == nil {
		t.Fatalf("Set should have failed")
	}
}

func lockGoRoutine(provider StorageProvider, key string, lockChannel chan LockObject, errChan chan error) {
	lockObject, err := provider.Lock(key)
	lockChannel <- lockObject
	errChan <- err
}

func unlockGoRoutine(provider StorageProvider, lock LockObject, key string, errChan chan error) {
	err := provider.Unlock(key, lock)
	errChan <- err
}
