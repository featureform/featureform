// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package scheduling

import (
	"testing"
)

func TestStorageProviderMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration tests")
	}

	// storage := NewMemoryStorageProvider()

	// test := StorageProviderTest{
	// 	t:       t,
	// 	storage: storage,
	// }
	// test.Run()
}
