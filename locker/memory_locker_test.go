package locker

import (
	"sync"
	"testing"
)

func TestMemoryLocker(t *testing.T) {
	locker := MemoryLocker{
		lockedItems: sync.Map{},
		mutex:       &sync.Mutex{},
	}

	test := LockerTest{
		t:      t,
		locker: &locker,
	}
	test.Run()

}
