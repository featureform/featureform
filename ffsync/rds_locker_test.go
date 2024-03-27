package ffsync

import (
	"testing"
)

func TestRDSLocker(t *testing.T) {
	locker, err := NewRDSLocker()
	if err != nil {
		t.Fatalf("Failed to create RDS locker: %v", err)
	}

	test := LockerTest{
		t:      t,
		locker: locker,
	}
	test.Run()
}
