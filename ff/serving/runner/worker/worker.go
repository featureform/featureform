// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package worker

import (
	"errors"
	"fmt"
	runner "github.com/featureform/serving/runner"
	"os"
	"strconv"
)

func CreateAndRun() error {
	config, ok := os.LookupEnv("CONFIG")

	if !ok {
		return errors.New("CONFIG not set")
	}
	fmt.Printf("Config: %v", config)
	name, ok := os.LookupEnv("NAME")

	if !ok {
		return errors.New("NAME not set")
	}
	fmt.Printf("Name: %v", name)
	jobRunner, err := runner.Create(name, []byte(config))
	if err != nil {
		return err
	}
	indexString, hasIndexEnv := os.LookupEnv("JOB_COMPLETION_INDEX")
	indexRunner, isIndexRunner := jobRunner.(runner.IndexRunner)
	if isIndexRunner && !hasIndexEnv {
		return errors.New("index runner needs index set")
	}
	if !isIndexRunner && hasIndexEnv {
		return errors.New("runner is not an index runner")
	}
	if hasIndexEnv && isIndexRunner {
		index, err := strconv.Atoi(indexString)
		if err != nil {
			return errors.New("index not of type int")
		}
		if err := indexRunner.SetIndex(index); err != nil {
			return errors.New("cannot set index")
		}
		jobRunner = indexRunner
	}
	watcher, err := jobRunner.Run()
	if err != nil {
		return err
	}
	if err := watcher.Wait(); err != nil {
		return err
	}
	return nil
}
