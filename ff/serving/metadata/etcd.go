// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	pb "github.com/featureform/serving/metadata/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"time"
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

//Configuration For ETCD Cluster
type EtcdConfig struct {
	Nodes []EtcdNode
}

type CoordinatorJob struct {
	Attempts int
	Resource ResourceID
}

func (c *CoordinatorJob) Serialize() ([]byte, error) {
	serialized, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

func (c *CoordinatorJob) Deserialize(serialized []byte) error {
	err := json.Unmarshal(serialized, c)
	if err != nil {
		return err
	}
	return nil
}

func (c EtcdConfig) initClient() (*clientv3.Client, error) {
	addresses := c.MakeAddresses()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   addresses,
		DialTimeout: time.Second * 1,
		Username:    "root",
		Password:    "secretpassword",
	})
	if err != nil {
		return nil, err
	}
	return client, nil
}

type EtcdStorage struct {
	Client *clientv3.Client
}

//Create Resource Lookup Using ETCD
type etcdResourceLookup struct {
	connection EtcdStorage
}

//Wrapper around Resource/Job messages. Allows top level storage for info about saved value
type EtcdRow struct {
	ResourceType ResourceType //Resource Type. For use when getting stored keys
	StorageType  StorageType  //Type of storage. Resource or Job
	Message      []byte       //Contents to be stored
}

func (config EtcdConfig) MakeAddresses() []string {
	addresses := make([]string, len(config.Nodes))
	for i, node := range config.Nodes {
		addresses[i] = fmt.Sprintf("%s:%s", node.Host, node.Port)
	}
	return addresses
}

//Uses Storage Type as prefix so Resources and Jobs can be queried more easily
func createKey(id ResourceID) string {
	return fmt.Sprintf("%s_%s_%s", id.Type, id.Name, id.Variant)
}

//Puts K/V into ETCD
func (s EtcdStorage) Put(key string, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	_, err := s.Client.Put(ctx, key, value)
	if err != nil {
		return err
	}
	return nil
}

func (s EtcdStorage) genericGet(key string, withPrefix bool) (*clientv3.GetResponse, error) {
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
		return nil, err
	}
	if resp.Count == 0 {
		return nil, fmt.Errorf("no keys found")
	}
	return resp, nil
}

//Gets value from ETCD using a key
func (s EtcdStorage) Get(key string) ([]byte, error) {
	resp, err := s.genericGet(key, false)
	if err != nil {
		return nil, err
	}
	return resp.Kvs[0].Value, nil
}

//Gets values from ETCD using a prefix key.
//Any value with a key starting with the 'key' argument will be queried.
//All stored values can be retrieved using an empty string as the 'key'
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

//Returns number of keys that match key prefix
//See GetWithPrefix for more details on prefix
func (s EtcdStorage) GetCountWithPrefix(key string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	resp, err := s.Client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

//Takes a populated ETCD storage struct and a resource
//Checks to make sure the given ETCD Storage Object contains a Resource, not job
//Deserializes Resource value into the provided Resource object
func (s EtcdStorage) ParseResource(res EtcdRow, resType Resource) (Resource, error) {
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

//Returns an empty Resource Object of the given type to unmarshal etcd value into
func (lookup etcdResourceLookup) createEmptyResource(t ResourceType) (Resource, error) {
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
		resource = &sourceResource{&pb.Source{}}
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

//Serializes the entire ETCD Storage Object to be put into ETCD
func (lookup etcdResourceLookup) serializeResource(res Resource) ([]byte, error) {
	p, err := proto.Marshal(res.Proto())
	if err != nil {
		return nil, err
	}
	msg := EtcdRow{
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

//Deserializes object into ETCD Storage Object
func (lookup etcdResourceLookup) deserialize(value []byte) (EtcdRow, error) {
	var msg EtcdRow
	if err := json.Unmarshal(value, &msg); err != nil {
		return msg, fmt.Errorf("failed To Parse Resource: %s", err)
	}
	return msg, nil
}

func (lookup etcdResourceLookup) Lookup(id ResourceID) (Resource, error) {
	key := createKey(id)
	resp, err := lookup.connection.Get(key)
	if err != nil {
		return nil, &ResourceNotFound{id}
	}
	msg, err := lookup.deserialize(resp)
	if err != nil {
		return nil, err
	}
	resType, err := lookup.createEmptyResource(msg.ResourceType)
	if err != nil {
		return nil, err
	}
	resource, err := lookup.connection.ParseResource(msg, resType)
	if err != nil {
		return nil, err
	}
	return resource, nil
}

func (lookup etcdResourceLookup) Has(id ResourceID) (bool, error) {
	key := createKey(id)
	count, err := lookup.connection.GetCountWithPrefix(key)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}
	return true, nil
}

func GetJobKey(id ResourceID) string {
	return fmt.Sprintf("JOB_%s_%s_%s", id.Type, id.Name, id.Variant)
}

func (lookup etcdResourceLookup) HasJob(id ResourceID) (bool, error) {
	job_key := GetJobKey(id)
	count, err := lookup.connection.GetCountWithPrefix(job_key)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}
	return true, nil
}

func (lookup etcdResourceLookup) SetJob(id ResourceID) error {
	if jobAlreadySet, _ := lookup.HasJob(id); jobAlreadySet {
		return fmt.Errorf("Job already set")
	}
	coordinatorJob := CoordinatorJob{
		Attempts: 0,
		Resource: id,
	}
	serialized, err := coordinatorJob.Serialize()
	if err != nil {
		return err
	}
	jobKey := GetJobKey(id)
	if err := lookup.connection.Put(jobKey, string(serialized)); err != nil {
		return err
	}
	return nil
}

func (lookup etcdResourceLookup) Set(id ResourceID, res Resource) error {

	serRes, err := lookup.serializeResource(res)
	if err != nil {
		return err
	}
	key := createKey(id)
	fmt.Printf("Setting: %v\n", key)
	err = lookup.connection.Put(key, string(serRes))
	if err != nil {
		return err
	}
	return nil
}

func (lookup etcdResourceLookup) Submap(ids []ResourceID) (ResourceLookup, error) {
	resources := make(localResourceLookup, len(ids))

	for _, id := range ids {
		key := createKey(id)
		value, err := lookup.connection.Get(key)
		if err != nil {
			return nil, &ResourceNotFound{id}
		}
		etcdStore, err := lookup.deserialize(value)
		if err != nil {
			return nil, err
		}

		resource, err := lookup.createEmptyResource(etcdStore.ResourceType)
		if err != nil {
			return nil, err
		}

		res, err := lookup.connection.ParseResource(etcdStore, resource)
		if err != nil {
			return nil, err
		}
		resources[id] = res
	}
	return resources, nil
}

func (lookup etcdResourceLookup) ListForType(t ResourceType) ([]Resource, error) {
	resources := make([]Resource, 0)
	resp, err := lookup.connection.GetWithPrefix(string(t))
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
		resource, err = lookup.connection.ParseResource(etcdStore, resource)
		if resource.ID().Type == t {
			resources = append(resources, resource)
		}
	}
	return resources, nil
}

func (lookup etcdResourceLookup) List() ([]Resource, error) {
	resources := make([]Resource, 0)
	resp, err := lookup.connection.GetWithPrefix("")
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
		resource, err = lookup.connection.ParseResource(etcdStore, resource)
		resources = append(resources, resource)
	}
	return resources, nil
}

func (lookup etcdResourceLookup) SetStatus(id ResourceID, status string) error {
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
