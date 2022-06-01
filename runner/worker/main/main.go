// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	worker "github.com/featureform/runner/worker"
	"log"
)

func main() {
	if err := worker.CreateAndRun(); err != nil {
		log.Fatalln(err)
	}
}
