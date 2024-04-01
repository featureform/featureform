package ffsync

import (
	"testing"
)

func TestETCDLocker(t *testing.T) {
	locker, err := NewETCDLocker()
	if err != nil {
		t.Fatalf("Failed to create ETCD locker: %v", err)
	}

	test := LockerTest{
		t:          t,
		locker:     locker,
		lockerType: "etcd",
	}
	test.Run()
}
