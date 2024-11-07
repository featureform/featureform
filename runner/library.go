// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package runner

import (
	"fmt"

	"github.com/featureform/fferr"
	"github.com/featureform/types"
)

func init() {
	registerFactories()
}

func registerFactories() {
	if err := RegisterFactory(COPY_TO_ONLINE, MaterializedChunkRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register 'Copy to Online' factory: %w", err))
	}
	if err := RegisterFactory(MATERIALIZE, MaterializeRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register 'Materialize' factory: %w", err))
	}
	if err := RegisterFactory(CREATE_TRAINING_SET, TrainingSetRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register 'Create Training Set' factory: %w", err))
	}
	if err := RegisterFactory(S3_IMPORT_DYNAMODB, S3ImportDynamoDBRunnerFactory); err != nil {
		panic(fmt.Errorf("failed to register S3 import to DynamoDB factory: %v", err))
	}
}

type RunnerName string

func (n RunnerName) String() string {
	return string(n)
}

const (
	COPY_TO_ONLINE      RunnerName = "Copy to online"
	CREATE_TRAINING_SET RunnerName = "Create training set"
	REGISTER_SOURCE     RunnerName = "Register source"
	MATERIALIZE         RunnerName = "Materialize"
	S3_IMPORT_DYNAMODB  RunnerName = "S3 import to DynamoDB"
)

type Config []byte

type RunnerConfig interface {
	Serialize() (Config, error)
	Deserialize(config Config) error
}

type RunnerFactory func(config Config) (types.Runner, error)

var factoryMap = make(map[RunnerName]RunnerFactory)

// Don't use this in testing, it affects global state and can break other tests or cause race conditions.
func ResetFactoryMap() {
	factoryMap = make(map[RunnerName]RunnerFactory)
}

func RegisterFactory(name RunnerName, runnerFactory RunnerFactory) error {
	if _, exists := factoryMap[name]; exists {
		return fferr.NewInternalErrorf("factory already registered: %s", name)
	}
	factoryMap[name] = runnerFactory
	return nil
}

// Don't use this in testing, it affects global state and can break other tests or cause race conditions.
func UnregisterFactory(name RunnerName) error {
	if _, exists := factoryMap[name]; !exists {
		return fferr.NewInternalErrorf("factory %s not registered", name)
	}
	delete(factoryMap, name)
	return nil
}

func Create(name RunnerName, config Config) (types.Runner, error) {
	factory, exists := factoryMap[name]
	if !exists {
		return nil, fferr.NewInternalErrorf("factory does not exist: %s", name)
	}
	runner, err := factory(config)
	if err != nil {
		return nil, err
	}
	return runner, nil
}
