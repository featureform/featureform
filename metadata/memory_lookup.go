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
	"github.com/featureform/storage"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// Create Resource Lookup Using ETCD
type MemoryResourceLookup struct {
	Connection storage.MetadataStorageImplementation
}

// Wrapper around Resource/Job messages. Allows top level storage for info about saved value
type MemoryRow struct {
	ResourceType ResourceType //Resource Type. For use when getting stored keys
	StorageType  StorageType  //Type of storage. Resource or Job
	Message      []byte       //Contents to be stored
}

type MemoryRowTemp struct {
	ResourceType ResourceType
	StorageType  StorageType //Type of storage. Resource or Job
	Message      []byte      //Contents to be stored
}

// Takes a populated ETCD Storage struct and a resource
// Checks to make sure the given ETCD Storage Object contains a Resource, not job
// Deserializes Resource value into the provided Resource object
func (s MemoryResourceLookup) ParseResource(res EtcdRow, resType Resource) (Resource, error) {
	id := resType.ID()
	if res.StorageType != RESOURCE {
		return nil, fferr.NewInvalidResourceTypeError(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), nil)
	}

	if !resType.Proto().ProtoReflect().IsValid() {
		return nil, fferr.NewInvalidResourceTypeErrorf(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), "invalid proto")
	}

	if res.Message == nil {
		return nil, fferr.NewInvalidResourceTypeErrorf(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), "invalid message")
	}

	if err := proto.Unmarshal(res.Message, resType.Proto()); err != nil {
		return nil, fferr.NewInvalidResourceTypeError(id.Name, id.Variant, fferr.ResourceType(id.Type.String()), err)
	}

	return resType, nil
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
	resp, err := lookup.Connection.Get(key)
	if err != nil || len(resp) == 0 {
		return nil, fferr.NewKeyNotFoundError(key, err)
	}
	logger.Infow("Deserialize", "key", key)
	msg, err := lookup.deserialize([]byte(resp))
	if err != nil {
		return nil, err
	}
	logger.Infow("Create empty resource", "key", key)
	resType, err := createEmptyResource(msg.ResourceType)
	if err != nil {
		return nil, err
	}
	logger.Infow("Parse resource", "key", key)
	resource, err := lookup.ParseResource(msg, resType)
	if err != nil {
		return nil, err
	}
	logger.Infow("Return", "key", key)
	return resource, nil
}

func (lookup MemoryResourceLookup) GetCountWithPrefix(id string) (int, error) {
	get, err := lookup.Connection.List(id)
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
		return fferr.NewInternalErrorf("job %v has already been created", id)
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
	if err := lookup.Connection.Set(jobKey, string(serialized)); err != nil {
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
	if err := lookup.Connection.Set(jobKey, string(serialized)); err != nil {
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
	if err := lookup.Connection.Set(key, string(serRes)); err != nil {
		return err
	}
	return nil
}

func (lookup MemoryResourceLookup) Submap(ids []ResourceID) (ResourceLookup, error) {
	resources := make(LocalResourceLookup, len(ids))

	for _, id := range ids {
		key := createKey(id)
		resp, err := lookup.Connection.Get(key)
		if err != nil {
			return nil, fferr.NewKeyNotFoundError(key, err)
		}
		etcdStore, err := lookup.deserialize([]byte(resp))
		if err != nil {
			return nil, err
		}

		resource, err := createEmptyResource(etcdStore.ResourceType)
		if err != nil {
			return nil, err
		}

		res, err := lookup.ParseResource(etcdStore, resource)
		if err != nil {
			return nil, err
		}
		resources[id] = res
	}
	return resources, nil
}

func (lookup MemoryResourceLookup) ListForType(t ResourceType) ([]Resource, error) {
	resources := make([]Resource, 0)
	resp, err := lookup.Connection.List(t.String())
	if err != nil {
		return nil, err
	}
	for _, v := range resp {
		etcdStore, err := lookup.deserialize([]byte(v))
		if err != nil {
			return nil, err
		}
		resource, err := createEmptyResource(etcdStore.ResourceType)
		if err != nil {
			return nil, err
		}
		resource, err = lookup.ParseResource(etcdStore, resource)
		if err != nil {
			return nil, err
		}
		if resource.ID().Type == t {
			resources = append(resources, resource)
		}
	}
	return resources, nil
}

func (lookup MemoryResourceLookup) List() ([]Resource, error) {
	resources := make([]Resource, 0)
	resp, err := lookup.Connection.List("")
	if err != nil {
		return nil, err
	}
	for _, v := range resp {
		etcdStore, err := lookup.deserialize([]byte(v))
		if err != nil {
			return nil, err
		}
		resource, err := createEmptyResource(etcdStore.ResourceType)
		if err != nil {
			return nil, err
		}
		resource, err = lookup.ParseResource(etcdStore, resource)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}
	return resources, nil
}

func (lookup *MemoryResourceLookup) SetStatus(id ResourceID, status *pb.ResourceStatus) error {
	res, err := lookup.Lookup(id)
	if err != nil {
		return err
	}
	if err := res.UpdateStatus(status); err != nil {
		return err
	}
	if err := lookup.Set(id, res); err != nil {
		return err
	}
	return nil
}
