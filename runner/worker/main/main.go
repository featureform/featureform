// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"log"

	"github.com/featureform/runner"
	"github.com/featureform/runner/worker"
)

func init() {
	if err := runner.RegisterFactory(runner.CREATE_TRAINING_SET, runner.TrainingSetRunnerFactory); err != nil {
		log.Fatalf("Failed to register training set runner factory: %v", err)
	}
	if err := runner.RegisterFactory(runner.COPY_TO_ONLINE, runner.MaterializedChunkRunnerFactory); err != nil {
		log.Fatalf("Failed to register copy to online runner factory: %v", err)
	}
	if err := runner.RegisterFactory(runner.MATERIALIZE, runner.MaterializeRunnerFactory); err != nil {
		log.Fatalf("Failed to register materialize runner factory: %v", err)
	}
	if err := runner.RegisterFactory(runner.CREATE_TRANSFORMATION, runner.CreateTransformationRunnerFactory); err != nil {
		log.Fatalf("Failed to register create transformation runner factory: %v", err)
	}
	if err := runner.RegisterFactory(runner.S3_IMPORT_DYNAMODB, runner.S3ImportDynamoDBRunnerFactory); err != nil {
		log.Fatalf("Failed to register S3 import to DynamoDB runner factory: %v", err)
	}
}

func main() {
	if err := worker.CreateAndRun(); err != nil {
		log.Fatalln(err)
	}
}
