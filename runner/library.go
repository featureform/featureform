// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"fmt"

	"github.com/featureform/types"
)

type RunnerName string

const (
	COPY_TO_ONLINE        RunnerName = "Copy to online"
	CREATE_TRAINING_SET   RunnerName = "Create training set"
	REGISTER_SOURCE       RunnerName = "Register source"
	CREATE_TRANSFORMATION RunnerName = "Create transformation"
	MATERIALIZE           RunnerName = "Materialize"
	S3_IMPORT_DYNAMODB    RunnerName = "S3 import to DynamoDB"
)

type Config []byte

type RunnerConfig interface {
	Serialize() (Config, error)
	Deserialize(config Config) error
}

type RunnerFactory func(config Config) (types.Runner, error)

var factoryMap = make(map[RunnerName]RunnerFactory)

func ResetFactoryMap() {
	factoryMap = make(map[RunnerName]RunnerFactory)
}

func RegisterFactory(name RunnerName, runnerFactory RunnerFactory) error {
	if _, exists := factoryMap[name]; exists {
		return fmt.Errorf("factory already registered: %s", name)
	}
	factoryMap[name] = runnerFactory
	return nil
}

func UnregisterFactory(name RunnerName) error {
	if _, exists := factoryMap[name]; !exists {
		return fmt.Errorf("factory %s not registered", name)
	}
	delete(factoryMap, name)
	return nil
}

func Create(name RunnerName, config Config) (types.Runner, error) {
	factory, exists := factoryMap[name]
	if !exists {
		return nil, fmt.Errorf("factory does not exist: %s", name)
	}
	runner, err := factory(config)
	if err != nil {
		return nil, err
	}
	return runner, nil
}
