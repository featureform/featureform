package worker

import (
	"errors"
	runner "github.com/featureform/serving/runner"
	"os"
)

func CreateAndRun() error {
	config, ok := os.LookupEnv("CONFIG")
	if !ok {
		return errors.New("CONFIG not set")
	}
	name, ok := os.LookupEnv("NAME")
	if !ok {
		return errors.New("NAME not set")
	}
	runner, err := runner.Create(name, []byte(config))
	if err != nil {
		return err
	}
	watcher, err := runner.Run()
	if err != nil {
		return err
	}
	if err := watcher.Wait(); err != nil {
		return err
	}
	return nil
}

