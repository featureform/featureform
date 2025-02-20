// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package ffsync

import (
	"flag"
	"os"
	"testing"
)

var pgPort string

// Flag to enable whether to use env vars or built-in values for dockertest
var useEnv = flag.Bool("use-env", false, "Use environment variables for ETCD configuration")

func TestMain(m *testing.M) {
	flag.Parse()
	// Skip when using remote provider
	if *useEnv {
		return
	}
	code := m.Run()
	os.Exit(code)
}
