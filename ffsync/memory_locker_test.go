package ffsync

import (
	"testing"
)

func TestMemoryLocker(t *testing.T) {
	locker, err := NewMemoryLocker()
	if err != nil {
		t.Fatalf("Failed to create Memory locker: %v", err)
	}

	test := LockerTest{
		t:      t,
		locker: &locker,
	}
	test.Run()

}
