// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package ffsync

import (
	"context"
	"fmt"
	"github.com/jonboulle/clockwork"
	"testing"
)

func TestMemoryLocker(t *testing.T) {
	clock := clockwork.NewFakeClock()

	locker, err := NewMemoryLocker()
	locker.clock = clock

	if err != nil {
		t.Fatalf("Failed to create Memory locker: %v", err)
	}

	test := LockerTest{
		t:          t,
		locker:     &locker,
		lockerType: "memory",
	}
	test.Run(clock)
}

func TestLockAndUnlockPrefixes(t *testing.T) {
	locker, err := NewMemoryLocker()
	if err != nil {
		t.Fatalf("Failed to create memory locker: %v", err)
	}

	prefix := "/tasks/metadata"
	taskId := "task_id=5"
	key := fmt.Sprintf("%s/%s", prefix, taskId)
	keyLock, err := locker.Lock(context.Background(), key, false)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Lock a prefix
	_, err = locker.Lock(context.Background(), prefix, false)
	if err == nil {
		t.Fatalf("Locking using a prefix should have failed because of key already locked")
	}

	// Test Unlock with original lock
	err = locker.Unlock(context.Background(), keyLock)
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	// Lock a prefix
	prefixLock, err := locker.Lock(context.Background(), prefix, false)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Lock a key with the same prefix
	_, err = locker.Lock(context.Background(), key, false)
	if err == nil {
		t.Fatalf("Locking key should fail because prefix is locked")
	}

	// Unlock the prefix lock
	err = locker.Unlock(context.Background(), prefixLock)
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}
