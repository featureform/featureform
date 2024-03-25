package ffsync

import (
	"testing"
)

func TestMemoryLocker(t *testing.T) {
	locker := NewMemoryLocker()

	test := LockerTest{
		t:      t,
		locker: &locker,
	}
	test.Run()

}
