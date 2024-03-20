package ffsync

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/featureform/fferr"
)

type LockerTest struct {
	t      *testing.T
	locker Locker
}

func (test *LockerTest) Run() {
	t := test.t
	locker := test.locker

	testFns := map[string]func(*testing.T, Locker){
		"LockAndUnlock":               LockAndUnlock,
		"LockAndUnlockWithGoRoutines": LockAndUnlockWithGoRoutines,
		"LockTimeUpdates":             LockTimeUpdates,
		"StressTestLockAndUnlock":     StressTestLockAndUnlock,
		"LockAndUnlockPrefixes":       LockAndUnlockPrefixes,
	}

	for name, fn := range testFns {
		t.Run(name, func(t *testing.T) {
			fn(t, locker)
		})
	}
}

func LockAndUnlock(t *testing.T, locker Locker) {
	key := "/tasks/metadata/task_id=1"

	// Test Lock
	lock, err := locker.Lock(key)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Test Lock on already locked item
	diffLock, err := locker.Lock(key)
	if err == nil {
		t.Fatalf("Locking using different id should have failed")
	}

	// Test Unlock with different lock
	err = locker.Unlock(diffLock)
	if err == nil {
		t.Fatalf("Unlocking using different id should have failed")
	}

	// Test Unlock with original lock
	err = locker.Unlock(lock)
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func LockAndUnlockWithGoRoutines(t *testing.T, locker Locker) {
	key := "/tasks/metadata/task_id=2"
	lockChannel := make(chan Key)
	errChan := make(chan error)

	// Test Lock
	go lockGoRoutine(locker, key, lockChannel, errChan)
	lock := <-lockChannel
	err := <-errChan
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Test Lock on already locked item
	diffLockChannel := make(chan Key)
	go lockGoRoutine(locker, key, diffLockChannel, errChan)
	diffLock := <-diffLockChannel
	err = <-errChan
	if err == nil {
		t.Fatalf("Locking using different id should have failed")
	}

	// Test UnLock with different UUID
	go unlockGoRoutine(locker, diffLock, errChan)
	err = <-errChan
	if err == nil {
		t.Fatalf("Unlocking using different id should have failed")
	}

	// Test Unlock with original lock
	go unlockGoRoutine(locker, lock, errChan)
	err = <-errChan
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func LockTimeUpdates(t *testing.T, locker Locker) {
	key := "/tasks/metadata/task_id=3"
	lock, err := locker.Lock(key)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Wait for 2 x ValidTimePeriod
	time.Sleep(2 * ValidTimePeriod)

	// Test the lock has been released
	_, err = locker.Lock(key)
	if err == nil {
		t.Fatal("Second Lock should have failed but didn't")
	}

	// Release the lock
	err = locker.Unlock(lock)
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func StressTestLockAndUnlock(t *testing.T, locker Locker) {
	key := "/tasks/metadata/task_id=5"

	var wg sync.WaitGroup
	// Use a counter to track the number of errors
	errorCount := 0

	// In 1000 threads, lock and unlock the same key
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(t *testing.T, id int) {
			defer wg.Done()
			// So only one thread will be able to lock and unlock the key
			// if multiple threads are able to lock the key, it means
			// there is a race condition. And we are able to detect it because
			// we will fail to unlock the key

			lock, err := locker.Lock(key)
			if err != nil {
				return
			}

			time.Sleep(10 * time.Millisecond)

			err = locker.Unlock(lock)
			if err != nil {
				errorCount++
				return
			}
		}(t, i)
	}
	wg.Wait()

	if errorCount > 0 {
		t.Fatalf("race condition detected! %d threads failed to unlock the key", errorCount)
	}
}

func LockAndUnlockPrefixes(t *testing.T, locker Locker) {
	prefix := "/tasks/metadata"
	taskId := "task_id=5"
	key := fmt.Sprintf("%s/%s", prefix, taskId)
	keyLock, err := locker.Lock(key)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Lock a prefix
	_, err = locker.Lock(prefix)
	if err == nil {
		t.Fatalf("Locking using a prefix should have failed because of key already locked")
	}

	// Test Unlock with original lock
	err = locker.Unlock(keyLock)
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	// Lock a prefix
	prefixLock, err := locker.Lock(prefix)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Lock a key with the same prefix
	_, err = locker.Lock(key)
	if err == nil {
		t.Fatalf("Locking key should fail because prefix is locked")
	}

	// Unlock the prefix lock
	err = locker.Unlock(prefixLock)
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func lockGoRoutine(locker Locker, key string, lockChannel chan Key, errChan chan error) {
	lockObject, err := locker.Lock(key)
	lockChannel <- lockObject
	errChan <- err
}

func unlockGoRoutine(locker Locker, lock Key, errChan chan error) {
	err := locker.Unlock(lock)
	errChan <- err
}

func TestLockInformation(t *testing.T) {
	type testCase struct {
		name            string
		lockInformation LockInformation
		expectedError   error
	}

	tests := []testCase{
		{
			name: "Valid",
			lockInformation: LockInformation{
				ID:   "id",
				Key:  "key",
				Date: time.Now().UTC(),
			},
			expectedError: nil,
		},
		{
			name: "Missing ID",
			lockInformation: LockInformation{
				Key:  "key",
				Date: time.Now().UTC(),
			},
			expectedError: fferr.NewInvalidArgumentError(fmt.Errorf("lock information is missing ID")),
		},
		{
			name: "Missing Key",
			lockInformation: LockInformation{
				ID:   "id",
				Date: time.Now().UTC(),
			},
			expectedError: fferr.NewInvalidArgumentError(fmt.Errorf("lock information is missing Key")),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := test.lockInformation.Marshal()
			if err != nil {
				t.Fatalf("Marshal() failed: %v", err)
			}

			var lockInformation LockInformation
			err = lockInformation.Unmarshal(data)
			if err != nil && err.Error() != test.expectedError.Error() {
				t.Fatalf("Unmarshal() failed: %v", err)
			}
		})
	}
}
