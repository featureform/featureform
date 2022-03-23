package runner

import (
	"fmt"
)

type Config []byte

type RunnerConfig interface {
	Serialize() (Config, error)
	Deserialize(Config) error
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
