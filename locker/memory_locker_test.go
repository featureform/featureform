package locker

import (
	"sync"
	"testing"
)

func TestMemoryLocker(t *testing.T) {
	locker := MemoryLocker{
		LockedItems: sync.Map{},
		Mutex:       &sync.Mutex{},
	}

	test := LockerTest{
		t:      t,
		locker: &locker,
	}
	test.Run()

}
