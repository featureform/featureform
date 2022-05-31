// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package runner

import (
	"fmt"
)

type RunnerName string

const (
	COPY_TO_ONLINE        RunnerName = "Copy to online"
	CREATE_TRAINING_SET              = "Create training set"
	REGISTER_SOURCE                  = "Register source"
	CREATE_TRANSFORMATION            = "Create transformation"
	MATERIALIZE                      = "Materialize"
)

type Config []byte

type RunnerConfig interface {
	Serialize() (Config, error)
	Deserialize(config Config) error
}

type RunnerFactory func(config Config) (Runner, error)

var factoryMap = make(map[string]RunnerFactory)

func ResetFactoryMap() {
	factoryMap = make(map[string]RunnerFactory)
}

func RegisterFactory(name string, runnerFactory RunnerFactory) error {
	if _, exists := factoryMap[name]; exists {
		return fmt.Errorf("factory %s already registered", name)
	}
	factoryMap[name] = runnerFactory
	return nil
}

func UnregisterFactory(name string) error {
	if _, exists := factoryMap[name]; !exists {
		return fmt.Errorf("factory %s not registered", name)
	}
	delete(factoryMap, name)
	return nil
}

func Create(name string, config Config) (Runner, error) {
	factory, exists := factoryMap[name]
	if !exists {
		return nil, fmt.Errorf("factory %s does not exist", name)
	}
	runner, err := factory(config)
	if err != nil {
		return nil, err
	}
	return runner, nil
}
