// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"encoding/json"
	"fmt"
	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	pb "github.com/featureform/metadata/proto"
	scheduling "github.com/featureform/scheduling/storage_providers"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// Create Resource Lookup Using ETCD
type MemoryResourceLookup struct {
	Connection scheduling.StorageProvider
}

// Wrapper around Resource/Job messages. Allows top level storage for info about saved value
type MemoryRow struct {
	ResourceType ResourceType //Resource Type. For use when getting stored keys
	//ResourceType string
	StorageType StorageType //Type of storage. Resource or Job
	Message     []byte      //Contents to be stored
}

type MemoryRowTemp struct {
	//ResourceType ResourceType //Resource Type. For use when getting stored keys
	ResourceType ResourceType
	StorageType  StorageType //Type of storage. Resource or Job
	Message      []byte      //Contents to be stored
}

// Takes a populated ETCD storage struct and a resource
// Checks to make sure the given ETCD Storage Object contains a Resource, not job
// Deserializes Resource value into the provided Resource object
func (s MemoryResourceLookup) ParseResource(res EtcdRow, resType Resource) (Resource, error) {
	if res.StorageType != RESOURCE {
		return nil, fmt.Errorf("payload is not resource type")
	}

	if !resType.Proto().ProtoReflect().IsValid() {
		return nil, fmt.Errorf("cannot parse to invalid resource")
	}

	if res.Message == nil {
		return nil, fmt.Errorf("cannot parse invalid message")
	}

	if err := proto.Unmarshal(res.Message, resType.Proto()); err != nil {
		return nil, err
	}

	return resType, nil
}

// Returns an empty Resource Object of the given type to unmarshal etcd value into
func (lookup MemoryResourceLookup) createEmptyResource(t ResourceType) (Resource, error) {
	var resource Resource
	switch t {
	case FEATURE:
		resource = &featureResource{&pb.Feature{}}
		break
	case FEATURE_VARIANT:
		resource = &featureVariantResource{&pb.FeatureVariant{}}
		break
	case LABEL:
		resource = &labelResource{&pb.Label{}}
		break
	case LABEL_VARIANT:
		resource = &labelVariantResource{&pb.LabelVariant{}}
		break
	case USER:
		resource = &userResource{&pb.User{}}
		break
	case ENTITY:
		resource = &entityResource{&pb.Entity{}}
		break
	case PROVIDER:
		resource = &providerResource{&pb.Provider{}}
		break
	case SOURCE:
		resource = &SourceResource{&pb.Source{}}
		break
	case SOURCE_VARIANT:
		resource = &sourceVariantResource{&pb.SourceVariant{}}
		break
	case TRAINING_SET:
		resource = &trainingSetResource{&pb.TrainingSet{}}
		break
	case TRAINING_SET_VARIANT:
		resource = &trainingSetVariantResource{&pb.TrainingSetVariant{}}
		break
	case MODEL:
		resource = &modelResource{&pb.Model{}}
		break
	default:
		return nil, fmt.Errorf("Invalid Type\n")
	}
	return resource, nil
}

// Serializes the entire ETCD Storage Object to be put into ETCD
func (lookup MemoryResourceLookup) serializeResource(res Resource) ([]byte, error) {
	p, err := proto.Marshal(res.Proto())
	if err != nil {
		return nil, err
	}
	msg := EtcdRowTemp{
		ResourceType: res.ID().Type,
		Message:      p,
		StorageType:  RESOURCE,
	}
	serialMsg, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return serialMsg, nil
}

// Deserializes object into ETCD Storage Object
func (lookup MemoryResourceLookup) deserialize(value []byte) (EtcdRow, error) {
	var tmp EtcdRowTemp
	if err := json.Unmarshal(value, &tmp); err != nil {
		return EtcdRow{}, errors.Wrap(err, fmt.Sprintf("failed to parse resource: %s", value))
	}
	msg := EtcdRow{
		ResourceType: ResourceType(tmp.ResourceType),
		StorageType:  tmp.StorageType,
		Message:      tmp.Message,
	}
	return msg, nil
}

func (lookup MemoryResourceLookup) Lookup(id ResourceID) (Resource, error) {
	logger := logging.NewLogger("lookup")
	key := createKey(id)
	logger.Infow("Get", "key", key)
	resp, err := lookup.Connection.Get(key, false)
	if err != nil || len(resp) == 0 {
		return nil, fferr.NewKeyNotFoundError(key, err)
	}
	logger.Infow("Deserialize", "key", key)
	msg, err := lookup.deserialize([]byte(resp[key]))
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to deserialize: %s", id))
	}
	logger.Infow("Create empty resource", "key", key)
	resType, err := lookup.createEmptyResource(msg.ResourceType)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to create empty resource: %s", id))
	}
	logger.Infow("Parse resource", "key", key)
	resource, err := lookup.ParseResource(msg, resType)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to parse resource: %s", id))
	}
	logger.Infow("Return", "key", key)
	return resource, nil
}

func (lookup MemoryResourceLookup) GetCountWithPrefix(id string) (int, error) {
	get, err := lookup.Connection.Get(id, true)
	if err != nil {
		return 0, err
	}
	return len(get), nil
}

func (lookup MemoryResourceLookup) Has(id ResourceID) (bool, error) {
	key := createKey(id)
	count, err := lookup.GetCountWithPrefix(key)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}
	return true, nil
}

func (lookup MemoryResourceLookup) HasJob(id ResourceID) (bool, error) {
	job_key := GetJobKey(id)
	count, err := lookup.GetCountWithPrefix(job_key)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}
	return true, nil
}

func (lookup MemoryResourceLookup) SetJob(id ResourceID, schedule string) error {
	if jobAlreadySet, _ := lookup.HasJob(id); jobAlreadySet {
		return fmt.Errorf("Job already set")
	}
	coordinatorJob := CoordinatorJob{
		Attempts: 0,
		Resource: id,
		Schedule: schedule,
	}
	serialized, err := coordinatorJob.Serialize()
	if err != nil {
		return err
	}
	jobKey := GetJobKey(id)
	lock, err := lookup.Connection.Lock(jobKey)
	if err != nil {
		return err
	}
	defer func(Connection scheduling.StorageProvider, key string, lock scheduling.LockObject) {
		err := Connection.Unlock(key, lock)
		if err != nil {

		}
	}(lookup.Connection, jobKey, lock)
	if err := lookup.Connection.Set(jobKey, string(serialized), lock); err != nil {
		return err
	}
	return nil
}

func (lookup MemoryResourceLookup) SetSchedule(id ResourceID, schedule string) error {
	coordinatorScheduleJob := CoordinatorScheduleJob{
		Attempts: 0,
		Resource: id,
		Schedule: schedule,
	}
	serialized, err := coordinatorScheduleJob.Serialize()
	if err != nil {
		return err
	}
	jobKey := GetScheduleJobKey(id)
	lock, err := lookup.Connection.Lock(jobKey)
	if err != nil {
		return err
	}
	defer func(Connection scheduling.StorageProvider, key string, lock scheduling.LockObject) {
		err := Connection.Unlock(key, lock)
		if err != nil {

		}
	}(lookup.Connection, jobKey, lock)
	if err := lookup.Connection.Set(jobKey, string(serialized), lock); err != nil {
		return err
	}
	return nil
}

func (lookup MemoryResourceLookup) Set(id ResourceID, res Resource) error {

	serRes, err := lookup.serializeResource(res)
	if err != nil {
		return err
	}
	key := createKey(id)
	lock, err := lookup.Connection.Lock(key)
	if err != nil {
		return err
	}
	defer func(Connection scheduling.StorageProvider, key string, lock scheduling.LockObject) {
		err := Connection.Unlock(key, lock)
		if err != nil {

		}
	}(lookup.Connection, key, lock)
	if err := lookup.Connection.Set(key, string(serRes), lock); err != nil {
		return err
	}
	return nil
}

func (lookup MemoryResourceLookup) Submap(ids []ResourceID) (ResourceLookup, error) {
	resources := make(LocalResourceLookup, len(ids))

	for _, id := range ids {
		key := createKey(id)
		resp, err := lookup.Connection.Get(key, false)
		if err != nil {
			return nil, fferr.NewKeyNotFoundError(key, err)
		}
		etcdStore, err := lookup.deserialize([]byte(resp[key]))
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("submap deserialize: %s", id))
		}

		resource, err := lookup.createEmptyResource(etcdStore.ResourceType)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("submap create empty resource: %s", id))
		}

		res, err := lookup.ParseResource(etcdStore, resource)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("submap parse resource: %s", id))
		}
		resources[id] = res
	}
	return resources, nil
}

func (lookup MemoryResourceLookup) ListForType(t ResourceType) ([]Resource, error) {
	resources := make([]Resource, 0)
	resp, err := lookup.Connection.Get(t.String(), true)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("could not get prefix: %s", t))
	}
	for k, v := range resp {
		etcdStore, err := lookup.deserialize([]byte(v))
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("could not deserialize: %s", k))
		}
		resource, err := lookup.createEmptyResource(etcdStore.ResourceType)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("could not create empty resource: %s", k))
		}
		resource, err = lookup.ParseResource(etcdStore, resource)
		if resource.ID().Type == t {
			resources = append(resources, resource)
		}
	}
	return resources, nil
}

func (lookup MemoryResourceLookup) List() ([]Resource, error) {
	resources := make([]Resource, 0)
	resp, err := lookup.Connection.Get("", true)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("could not get prefix: %v", resources))
	}
	for k, v := range resp {
		etcdStore, err := lookup.deserialize([]byte(v))
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("list deserialize: %s", k))
		}
		resource, err := lookup.createEmptyResource(etcdStore.ResourceType)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("list create empty resource: %s", k))
		}
		resource, err = lookup.ParseResource(etcdStore, resource)
		resources = append(resources, resource)
	}
	return resources, nil
}

func (lookup MemoryResourceLookup) SetStatus(id ResourceID, status pb.ResourceStatus) error {
	res, err := lookup.Lookup(id)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("could not lookup ID: %v", id))
	}
	if err := res.UpdateStatus(status); err != nil {
		return errors.Wrap(err, fmt.Sprintf("could not update ID: %v", id))
	}
	if err := lookup.Set(id, res); err != nil {
		return errors.Wrap(err, fmt.Sprintf("could not set ID: %v", id))
	}
	return nil
}
