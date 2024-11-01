// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package storage

import "testing"

func TestMemoryMetadataStorage(t *testing.T) {
	storage, err := NewMemoryStorageImplementation()
	if err != nil {
		t.Fatalf("Failed to create Memory storage: %v", err)
	}

	test := MetadataStorageTest{
		t:       t,
		storage: &storage,
	}
	test.Run()
}
