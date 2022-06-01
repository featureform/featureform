// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
<<<<<<< HEAD:ff/serving/runner/worker/main/main.go
	runner "github.com/featureform/serving/runner"
	worker "github.com/featureform/serving/runner/worker"
=======
	"fmt"
	"github.com/featureform/runner"
	"github.com/featureform/runner/worker"
>>>>>>> 0ae8aa590a710f413bee74320d3f0f59ab849e56:runner/worker/main/main.go
	"log"
)

func init() {
	if err := runner.RegisterFactory(string(runner.CREATE_TRAINING_SET), runner.TrainingSetRunnerFactory); err != nil {
		log.Fatalf("Failed to register training set runner factory: %v", err)
	}
}

func main() {
	if err := runner.RegisterFactory(string(runner.CREATE_TRAINING_SET), runner.TrainingSetRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register training set runner factory: %w", err))
	}
	if err := worker.CreateAndRun(); err != nil {
		log.Fatalln(err)
	}
}
