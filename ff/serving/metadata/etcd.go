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

//Create Resource Lookup Using ETCD
type etcdResourceLookup struct {
	connection EtcdConfig
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
func CreateKey(t StorageType, key string) string {
	return fmt.Sprintf("%s_%s", t, key)
}

//Puts K/V into ETCD
func (config EtcdConfig) Put(key string, value string, t StorageType) error {
	k := CreateKey(t, key)
	addresses := config.MakeAddresses()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   addresses,
		DialTimeout: time.Second * 1,
	})
	defer cancel()
	if err != nil {
		return err
	}
	_, err = client.Put(ctx, k, value)
	if err != nil {
		return err
	}
	return nil
}

func (config EtcdConfig) genericGet(key string, withPrefix bool) (*clientv3.GetResponse, error) {
	addresses := config.MakeAddresses()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   addresses,
		DialTimeout: time.Second * 1,
	})
	defer cancel()
	if err != nil {
		return nil, err
	}
	var resp *clientv3.GetResponse
	if withPrefix {
		resp, err = client.Get(ctx, key, clientv3.WithPrefix())
	} else {
		resp, err = client.Get(ctx, key)
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
func (config EtcdConfig) Get(key string, t StorageType) ([]byte, error) {
	k := CreateKey(t, key)
	resp, err := config.genericGet(k, false)
	if err != nil {
		return nil, err
	}
	return resp.Kvs[0].Value, nil
}

//Gets values from ETCD using a prefix key.
//Any value with a key starting with the 'key' argument will be queried.
//All stored values can be retrieved using an empty string as the 'key'
func (config EtcdConfig) GetWithPrefix(key string) ([][]byte, error) {
	resp, err := config.genericGet(key, true)
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
func (config EtcdConfig) GetCountWithPrefix(key string) (int64, error) {
	addresses := config.MakeAddresses()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   addresses,
		DialTimeout: time.Second * 1,
	})
	defer cancel()
	if err != nil {
		return 0, err
	}
	resp, err := client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

//Takes a populated ETCD storage struct and a resource
//Checks to make sure the given ETCD Storage Object contains a Resource, not job
//Deserializes Resource value into the provided Resource object
func (config EtcdConfig) ParseResource(res EtcdRow, resType Resource) (Resource, error) {
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

//Can be implemented to unmarshal EtcdRow Object into format used by Jobs
//Like ParseResource Above
func (config EtcdConfig) ParseJob(res EtcdRow) ([]byte, error) {
	return nil, nil
}

//Returns an empty Resource Object of the given type to unmarshal etcd value into
func (lookup etcdResourceLookup) findResourceType(t ResourceType) (Resource, error) {
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
		// Transformation Not Included, Uncomment Later
	//case TRANSFORMATION:
	//	resource = &transformationResource{&pb.Transformation{}}
	//	break
	//case TRANSFORMATION_VARIANT :
	//	resource = &TransformationVariantResource{&pb.TransformationVariant{}}
	//	break
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
	p, _ := proto.Marshal(res.Proto())
	msg := EtcdRow{
		ResourceType: res.ID().Type,
		Message:      p,
		StorageType:  RESOURCE,
	}
	strmsg, err := json.Marshal(msg)
	if err != nil {
		return strmsg, err
	}
	return strmsg, nil
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
	resp, err := lookup.connection.Get(id.Name, RESOURCE)
	if err != nil {
		return nil, &ResourceNotFound{id}
	}
	msg, err := lookup.deserialize(resp)
	if err != nil {
		return nil, err
	}
	resType, err := lookup.findResourceType(msg.ResourceType)
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
	count, err := lookup.connection.GetCountWithPrefix(id.Name)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, &ResourceNotFound{id}
	}
	return true, nil
}

func (lookup etcdResourceLookup) Set(id ResourceID, res Resource) error {
	serRes, err := lookup.serializeResource(res)
	err = lookup.connection.Put(id.Name, string(serRes), RESOURCE)
	if err != nil {
		return err
	}
	return nil
}

func (lookup etcdResourceLookup) Submap(ids []ResourceID) (ResourceLookup, error) {
	resources := make(localResourceLookup, len(ids))

	for _, id := range ids {

		value, err := lookup.connection.Get(id.Name, RESOURCE)
		if err != nil {
			return nil, &ResourceNotFound{id}
		}
		etcdStore, err := lookup.deserialize(value)
		if err != nil {
			return nil, err
		}

		resource, err := lookup.findResourceType(etcdStore.ResourceType)
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
	resp, err := lookup.connection.GetWithPrefix(string(RESOURCE))
	if err != nil {
		return nil, err
	}
	for _, res := range resp {
		etcdStore, err := lookup.deserialize(res)
		if err != nil {
			return nil, err
		}
		resource, err := lookup.findResourceType(etcdStore.ResourceType)
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
	resp, err := lookup.connection.GetWithPrefix(string(RESOURCE))
	if err != nil {
		return nil, err
	}
	for _, res := range resp {
		etcdStore, err := lookup.deserialize(res)
		if err != nil {
			return nil, err
		}
		resource, err := lookup.findResourceType(etcdStore.ResourceType)
		if err != nil {
			return nil, err
		}
		resource, err = lookup.connection.ParseResource(etcdStore, resource)
		resources = append(resources, resource)
	}
	return resources, nil
}
