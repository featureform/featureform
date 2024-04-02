package ffsync

import (
	"testing"
)

func TestMemoryLocker(t *testing.T) {
	locker, _ := NewMemoryLocker()

	test := LockerTest{
		t:      t,
		locker: &locker,
	}
	test.Run()

}
