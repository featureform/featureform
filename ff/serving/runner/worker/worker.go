// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package worker

import (
	"errors"
	runner "github.com/featureform/serving/runner"
	"os"
	"github.com/google/uuid"
	"strconv"
)

type Config []byte

type ResourceUpdatedEvent struct {
	ResourceID metadataResourceID
	Completed time.Time
}

func (c *ResourceUpdatedEvent) Serialize() (Config, error) {
	config, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return config, nil
}

func (c *ResourceUpdatedEvent) Deserialize(config Config) error {
	err := json.Unmarshal(config, c)
	if err != nil {
		return err
	}
	return nil
}

//todo add etcd client to the kubernetes environment variables
func CreateAndRun() error {
	config, ok := os.LookupEnv("CONFIG")
	if !ok {
		return errors.New("CONFIG not set")
	}
	name, ok := os.LookupEnv("NAME")
	if !ok {
		return errors.New("NAME not set")
	}
	etcdConfig, ok := os.LookupEnv("ETCD_CONFIG")
	if !ok {
		return errors.New("ETCD_CONFIG not set")
	}
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
	if jobRunner.IsUpdateJob() {
		etcdConfig := &coordinator.ETCDConfig{}
		err := etcdConfig.Deserialize()
		if err != nil {
			return err
		}
		cli, err := clientv3.New(clientv3.Config{Endpoints: etcdConfig.Endpoints})
		if err != nil {
			return nil, err
		}
		resourceID := jobRunner.Resource()
		timeCompleted := time.Now()
		updatedEvent := &ResourceUpdatedEvent{
			ResourceID: resourceID,
			Completed: timeCompleted,
		}
		serializedEvent, err := updatedEvent.Serialize()
		if err != nil {
			return err
		}
		eventID := uuid.New().String()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()
		_, err := cli.Put(ctx, fmt.Sprint("UPDATE_EVENT_%s", eventID), string(serializedEvent))
		if err != nil {
			return err
		}
	}
	return nil
}
