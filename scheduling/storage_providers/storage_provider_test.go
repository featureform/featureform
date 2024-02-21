package scheduling

import (
	"fmt"
	"sort"
	"testing"
	"time"

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
		id    string
		key   string
		value string
		err   error
	}
	tests := map[string]TestCase{
		"Simple":     {uuid.New().String(), "key1", "value1", nil},
		"EmptyKey":   {uuid.New().String(), "", "value1", fmt.Errorf("key is empty")},
		"EmptyValue": {uuid.New().String(), "key1", "", fmt.Errorf("value is empty for key key1")},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := provider.Set(test.id, test.key, test.value)
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
		id := uuid.New().String()
		provider.Set(id, "key1", "value1")
		provider.Set(id, "key2", "value2")
		provider.Set(id, "prefix/key3", "value3")
		provider.Set(id, "prefix/key4", "value4")
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

	id := uuid.New().String()
	runTestCase := func(t *testing.T, test TestCase) {
		provider := &MemoryStorageProvider{}
		for _, key := range test.keys {
			err := provider.Set(id, key, "value")
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
	provider := &MemoryStorageProvider{storage: make(map[string]string), lockedItems: make(map[string]LockInformation)}

	id := uuid.New().String()
	key := "/tasks/metadata/task_id=1"
	channel := make(chan error)

	// Test Lock
	_, err := provider.Lock(id, key, channel)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Test Lock on already locked item with same UUID
	_, err = provider.Lock(id, key, channel)
	if err != nil {
		t.Fatalf("Locking using the same id failed: %v", err)
	}

	// Test Lock on already locked item with different UUID
	diffId := uuid.New().String()
	_, err = provider.Lock(diffId, key, channel)
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
	provider := &MemoryStorageProvider{storage: make(map[string]string), lockedItems: make(map[string]LockInformation)}

	id := uuid.New().String()
	key := "/tasks/metadata/task_id=2"
	lockChannel := make(chan error)

	errChan := make(chan error)

	// Test Lock
	go lockGoRoutine(provider, id, key, lockChannel, errChan)
	err := <-errChan
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Test Lock on already locked item with same UUID
	go lockGoRoutine(provider, id, key, lockChannel, errChan)
	err = <-errChan
	if err != nil {
		t.Fatalf("Locking using the same id failed: %v", err)
	}

	// Test Lock on already locked item with different UUID
	diffId := uuid.New().String()
	go lockGoRoutine(provider, diffId, key, lockChannel, errChan)
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

func TestLockTimeUpdates(t *testing.T) {
	timeChannel := make(chan int)
	provider := NewMemoryStorageProvider(timeChannel)

	id := uuid.New().String()
	key := "/tasks/metadata/task_id=3"
	lockChannel := make(chan error)
	_, err := provider.Lock(id, key, lockChannel)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	err = provider.Set(id, key, "value")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Wait for 2 x ValidTimePeriod
	time.Sleep(2 * ValidTimePeriod)

	// Check if date has been updated
	lockInfo, ok := provider.lockedItems[key]
	if !ok {
		t.Fatalf("Key not found")
	}
	if time.Since(lockInfo.Date) > ValidTimePeriod {
		t.Fatalf("Lock time not updated: current time: %s, lock time: %s", time.Now().String(), lockInfo.Date.String())
	}

	// Unlock the key
	err = provider.Unlock(id, key)
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	// Check if date has been updated
	_, ok = provider.lockedItems[key]
	if ok {
		t.Fatalf("Key not deleted")
	}
}

func TestCleanup(t *testing.T) {
	timeChannel := make(chan int)
	provider := NewMemoryStorageProvider(timeChannel)

	id := uuid.New().String()
	key := "/tasks/metadata/task_id=4"

	lockChannel := make(chan error)
	errChan := make(chan error)
	go lockGoRoutine(provider, id, key, lockChannel, errChan)
	err := <-errChan
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Wait for 5 seconds
	time.Sleep(5 * time.Second)

	// Used to stop the go routine
	lockChannel <- fmt.Errorf("delete")

	// Wait for cleanup
	time.Sleep(CleanupInterval + 2*time.Second)

	// Check if the key has been deleted
	_, ok := provider.lockedItems[key]
	if ok {
		t.Fatalf("Key not deleted: %v", provider.lockedItems)
	}
}

func lockGoRoutine(provider *MemoryStorageProvider, id string, key string, lockChannel, errChan chan error) {
	_, err := provider.Lock(id, key, lockChannel)
	errChan <- err
}

func unlockGoRoutine(provider *MemoryStorageProvider, id string, key string, errChan chan error) {
	err := provider.Unlock(id, key)
	errChan <- err
}
