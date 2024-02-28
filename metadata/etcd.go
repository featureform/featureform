// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/featureform/fferr"
	help "github.com/featureform/helpers"
	"github.com/featureform/logging"
	pb "github.com/featureform/metadata/proto"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

type StorageType string

const (
	RESOURCE StorageType = "Resource"
	JOB                  = "Job"
)

type EtcdNode struct {
	Host string
	Port string
}

// Configuration For ETCD Cluster
type EtcdConfig struct {
	Nodes []EtcdNode
}

type CoordinatorJob struct {
	Attempts int
	Resource ResourceID
	Schedule string
}

type CoordinatorScheduleJob struct {
	Attempts int
	Resource ResourceID
	Schedule string
}

func (c *CoordinatorScheduleJob) Serialize() ([]byte, error) {
	serialized, err := json.Marshal(c)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return serialized, nil
}

func (c *CoordinatorScheduleJob) Deserialize(serialized []byte) error {
	err := json.Unmarshal(serialized, c)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

type TempJob struct {
	Attempts int
	Name     string
	Variant  string
	Type     string
	Schedule string
}

func (c *CoordinatorJob) Serialize() ([]byte, error) {
	job := TempJob{
		Attempts: c.Attempts,
		Name:     c.Resource.Name,
		Variant:  c.Resource.Variant,
		Type:     c.Resource.Type.String(),
		Schedule: c.Schedule,
	}
	serialized, err := json.Marshal(job)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return serialized, nil
}

func (c *CoordinatorJob) Deserialize(serialized []byte) error {
	job := TempJob{}
	err := json.Unmarshal(serialized, &job)

	if err != nil {
		return fferr.NewInternalError(err)
	}
	c.Attempts = job.Attempts
	c.Resource.Name = job.Name
	c.Resource.Variant = job.Variant
	c.Resource.Type = ResourceType(pb.ResourceType_value[job.Type])
	c.Schedule = job.Schedule
	return nil
}

func (c EtcdConfig) InitClient() (*clientv3.Client, error) {
	addresses := c.MakeAddresses()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:         addresses,
		AutoSyncInterval:  time.Second * 30,
		DialTimeout:       time.Second * 1,
		DialKeepAliveTime: time.Second * 1,
		Username:          help.GetEnv("ETCD_USERNAME", "root"),
		Password:          help.GetEnv("ETCD_PASSWORD", "secretpassword"),
	})
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}

	return client, nil
}

type EtcdStorage struct {
	Client *clientv3.Client
}

// Create Resource Lookup Using ETCD
type EtcdResourceLookup struct {
	Connection EtcdStorage
}

// Wrapper around Resource/Job messages. Allows top level storage for info about saved value
type EtcdRow struct {
	ResourceType ResourceType //Resource Type. For use when getting stored keys
	//ResourceType string
	StorageType StorageType //Type of storage. Resource or Job
	Message     []byte      //Contents to be stored
}

type EtcdRowTemp struct {
	//ResourceType ResourceType //Resource Type. For use when getting stored keys
	ResourceType ResourceType
	StorageType  StorageType //Type of storage. Resource or Job
	Message      []byte      //Contents to be stored
}

func (config EtcdConfig) MakeAddresses() []string {
	addresses := make([]string, len(config.Nodes))
	for i, node := range config.Nodes {
		addresses[i] = fmt.Sprintf("%s:%s", node.Host, node.Port)
	}
	return addresses
}

// Uses Storage Type as prefix so Resources and Jobs can be queried more easily
func createKey(id ResourceID) string {
	return fmt.Sprintf("%s__%s__%s", id.Type, id.Name, id.Variant)
}

// Puts K/V into ETCD
func (s EtcdStorage) Put(key string, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	_, err := s.Client.Put(ctx, key, value)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (s EtcdStorage) genericGet(key string, withPrefix bool) (*clientv3.GetResponse, error) {
	if key == "" && !withPrefix {
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("key cannot be empty"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	var resp *clientv3.GetResponse
	var err error
	if withPrefix {
		resp, err = s.Client.Get(ctx, key, clientv3.WithPrefix())
	} else {
		resp, err = s.Client.Get(ctx, key)
	}
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return resp, nil
}

// Gets value from ETCD using a key, error if it doesn't exist
func (s EtcdStorage) Get(key string) ([]byte, error) {
	resp, err := s.genericGet(key, false)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, fferr.NewKeyNotFoundError(key, fmt.Errorf("key not found in etcd"))
	}
	return resp.Kvs[0].Value, nil
}

// Gets values from ETCD using a prefix key.
// Any value with a key starting with the 'key' argument will be queried.
// All stored values can be retrieved using an empty string as the 'key'
func (s EtcdStorage) GetWithPrefix(key string) ([][]byte, error) {
	resp, err := s.genericGet(key, true)
	if err != nil {
		return nil, err
	}
	response := make([][]byte, resp.Count)
	for i, res := range resp.Kvs {
		response[i] = res.Value
	}
	return response, nil
}

// Returns number of keys that match key prefix
// See GetWithPrefix for more details on prefix
func (s EtcdStorage) GetCountWithPrefix(key string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	resp, err := s.Client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		wrapped := fferr.NewInternalError(err)
		wrapped.AddDetail("key", key)
		return 0, wrapped
	}
	return resp.Count, nil
}

// Takes a populated ETCD storage struct and a resource
// Checks to make sure the given ETCD Storage Object contains a Resource, not job
// Deserializes Resource value into the provided Resource object
func (s EtcdStorage) ParseResource(res EtcdRow, resType Resource) (Resource, error) {
	if res.StorageType != RESOURCE {
		return nil, fferr.NewInternalError(fmt.Errorf("payload is not resource type"))
	}

	if !resType.Proto().ProtoReflect().IsValid() {
		return nil, fferr.NewInternalError(fmt.Errorf("cannot parse to invalid resource"))
	}

	if res.Message == nil {
		return nil, fferr.NewInternalError(fmt.Errorf("cannot parse invalid message"))
	}

	if err := proto.Unmarshal(res.Message, resType.Proto()); err != nil {
		return nil, fferr.NewInternalError(err)
	}

	return resType, nil
}

// Returns an empty Resource Object of the given type to unmarshal etcd value into
func (lookup EtcdResourceLookup) createEmptyResource(t ResourceType) (Resource, error) {
	var resource Resource
	switch t {
	case FEATURE:
		resource = &featureResource{&pb.Feature{}}
	case FEATURE_VARIANT:
		resource = &featureVariantResource{&pb.FeatureVariant{}}
	case LABEL:
		resource = &labelResource{&pb.Label{}}
	case LABEL_VARIANT:
		resource = &labelVariantResource{&pb.LabelVariant{}}
	case USER:
		resource = &userResource{&pb.User{}}
	case ENTITY:
		resource = &entityResource{&pb.Entity{}}
	case PROVIDER:
		resource = &providerResource{&pb.Provider{}}
	case SOURCE:
		resource = &SourceResource{&pb.Source{}}
	case SOURCE_VARIANT:
		resource = &sourceVariantResource{&pb.SourceVariant{}}
	case TRAINING_SET:
		resource = &trainingSetResource{&pb.TrainingSet{}}
	case TRAINING_SET_VARIANT:
		resource = &trainingSetVariantResource{&pb.TrainingSetVariant{}}
	case MODEL:
		resource = &modelResource{&pb.Model{}}
	case TRIGGER:
		resource = &triggerResource{&pb.Trigger{}}
	default:
		return nil, fferr.NewInvalidArgumentError(fmt.Errorf("invalid resource type: %s", t))
	}
	return resource, nil
}

// Serializes the entire ETCD Storage Object to be put into ETCD
func (lookup EtcdResourceLookup) serializeResource(res Resource) ([]byte, error) {
	p, err := proto.Marshal(res.Proto())
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	msg := EtcdRowTemp{
		ResourceType: res.ID().Type,
		Message:      p,
		StorageType:  RESOURCE,
	}
	serialMsg, err := json.Marshal(msg)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return serialMsg, nil
}

// Deserializes object into ETCD Storage Object
func (lookup EtcdResourceLookup) deserialize(value []byte) (EtcdRow, error) {
	var tmp EtcdRowTemp
	if err := json.Unmarshal(value, &tmp); err != nil {
		return EtcdRow{}, fferr.NewInternalError(err)
	}
	msg := EtcdRow{
		ResourceType: ResourceType(tmp.ResourceType),
		StorageType:  tmp.StorageType,
		Message:      tmp.Message,
	}
	return msg, nil
}

func (lookup EtcdResourceLookup) Lookup(id ResourceID) (Resource, error) {
	logger := logging.NewLogger("lookup")
	key := createKey(id)
	logger.Infow("Get", "key", key)
	resp, err := lookup.Connection.Get(key)
	if err != nil || len(resp) == 0 {
		return nil, err
	}
	logger.Infow("Deserialize", "key", key)
	msg, err := lookup.deserialize(resp)
	if err != nil {
		return nil, err
	}
	logger.Infow("Create empty resource", "key", key)
	resType, err := lookup.createEmptyResource(msg.ResourceType)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to create empty resource: %s", id))
	}
	logger.Infow("Parse resource", "key", key)
	resource, err := lookup.Connection.ParseResource(msg, resType)
	if err != nil {
		return nil, err
	}
	logger.Infow("Return", "key", key)
	return resource, nil
}

func (lookup EtcdResourceLookup) Has(id ResourceID) (bool, error) {
	key := createKey(id)
	count, err := lookup.Connection.GetCountWithPrefix(key)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}
	return true, nil
}

func GetJobKey(id ResourceID) string {
	return fmt.Sprintf("JOB__%s__%s__%s", id.Type, id.Name, id.Variant)
}

func GetScheduleJobKey(id ResourceID) string {
	return fmt.Sprintf("SCHEDULEJOB__%s__%s__%s", id.Type, id.Name, id.Variant)
}

func (lookup EtcdResourceLookup) HasJob(id ResourceID) (bool, error) {
	job_key := GetJobKey(id)
	count, err := lookup.Connection.GetCountWithPrefix(job_key)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}
	return true, nil
}

func (lookup EtcdResourceLookup) SetJob(id ResourceID, schedule string) error {
	if jobAlreadySet, _ := lookup.HasJob(id); jobAlreadySet {
		return fferr.NewJobAlreadyExistsError(GetJobKey(id), nil)
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
	if err := lookup.Connection.Put(jobKey, string(serialized)); err != nil {
		return err
	}
	return nil
}

func (lookup EtcdResourceLookup) SetSchedule(id ResourceID, schedule string) error {
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
	if err := lookup.Connection.Put(jobKey, string(serialized)); err != nil {
		return err
	}
	return nil
}

func (lookup EtcdResourceLookup) Set(id ResourceID, res Resource) error {

	serRes, err := lookup.serializeResource(res)
	if err != nil {
		return err
	}
	key := createKey(id)
	err = lookup.Connection.Put(key, string(serRes))
	if err != nil {
		return err
	}
	return nil
}

func (lookup EtcdResourceLookup) Submap(ids []ResourceID) (ResourceLookup, error) {
	resources := make(LocalResourceLookup, len(ids))

	for _, id := range ids {
		key := createKey(id)
		resp, err := lookup.Connection.Get(key)
		if err != nil {
			return nil, fferr.NewDatasetNotFoundError(id.Name, id.Variant, err)
		}
		etcdStore, err := lookup.deserialize(resp)
		if err != nil {
			return nil, err
		}

		resource, err := lookup.createEmptyResource(etcdStore.ResourceType)
		if err != nil {
			return nil, err
		}

		res, err := lookup.Connection.ParseResource(etcdStore, resource)
		if err != nil {
			return nil, err
		}
		resources[id] = res
	}
	return resources, nil
}

func (lookup EtcdResourceLookup) ListForType(t ResourceType) ([]Resource, error) {
	resources := make([]Resource, 0)
	resp, err := lookup.Connection.GetWithPrefix(t.String())
	if err != nil {
		return nil, err
	}
	for _, res := range resp {
		etcdStore, err := lookup.deserialize(res)
		if err != nil {
			return nil, err
		}
		resource, err := lookup.createEmptyResource(etcdStore.ResourceType)
		if err != nil {
			return nil, err
		}
		resource, err = lookup.Connection.ParseResource(etcdStore, resource)
		if resource.ID().Type == t {
			resources = append(resources, resource)
		}
	}
	return resources, nil
}

func (lookup EtcdResourceLookup) List() ([]Resource, error) {
	resources := make([]Resource, 0)
	resp, err := lookup.Connection.GetWithPrefix("")
	if err != nil {
		return nil, err
	}
	for _, res := range resp {
		etcdStore, err := lookup.deserialize(res)
		if err != nil {
			return nil, err
		}
		resource, err := lookup.createEmptyResource(etcdStore.ResourceType)
		if err != nil {
			return nil, err
		}
		resource, err = lookup.Connection.ParseResource(etcdStore, resource)
		resources = append(resources, resource)
	}
	return resources, nil
}

func (lookup EtcdResourceLookup) SetStatus(id ResourceID, status pb.ResourceStatus) error {
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
