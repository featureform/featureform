// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"testing"

	pt "github.com/featureform/provider/provider_type"
)

func TestOfflineStoreMemory(t *testing.T) {
	store, err := GetOfflineStore(pt.MemoryOffline, []byte{})
	if err != nil {
		t.Fatalf("could not initialize store: %s\n", err)
	}

	test := OfflineStoreTest{
		t:     t,
		store: store,
	}
	test.Run()
}
