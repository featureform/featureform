// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package main

import (
	"log"

	"github.com/featureform/runner/worker"
)

func main() {
	if err := worker.CreateAndRun(); err != nil {
		log.Fatalln(err)
	}
}
