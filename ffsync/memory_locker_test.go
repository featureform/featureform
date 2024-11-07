// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

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
		t:          t,
		locker:     &locker,
		lockerType: "memory",
	}
	test.Run()
}
