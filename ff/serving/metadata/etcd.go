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

type EtcdConfig struct {
	Host string //localhost
	Port string //2379
}

//Create Resource Lookup Using ETCD
type etcdResourceLookup struct {
	connection EtcdConfig
}

type EtcdStorage struct {
	ResourceType ResourceType
	StorageType  StorageType
	Message      []byte
}

func (config EtcdConfig) MakeAddress() string {
	return fmt.Sprintf("%s:%s", config.Host, config.Port)
}

func (config EtcdConfig) Put(key string, value string) error {
	address := config.MakeAddress()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{address},
		DialTimeout: time.Second * 1,
	})
	defer cancel()
	if err != nil {
		return err
	}
	_, err = client.Put(ctx, key, value)
	if err != nil {
		return err
	}
	return nil
}

func (config EtcdConfig) Get(key string) ([]byte, error) {
	address := config.MakeAddress()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{address},
		DialTimeout: time.Second * 1,
	})
	defer cancel()
	if err != nil {
		return nil, err
	}
	resp, err := client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if resp.Count == 0 {
		return nil, fmt.Errorf("no keys found")
	}
	return resp.Kvs[0].Value, nil
}

func (config EtcdConfig) GetWithPrefix(key string) ([][]byte, error) {
	address := config.MakeAddress()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{address},
		DialTimeout: time.Second * 1,
	})
	defer cancel()
	if err != nil {
		return nil, err
	}
	resp, err := client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	if resp.Count == 0 {
		return nil, fmt.Errorf("no keys found")
	}
	response := make([][]byte, resp.Count)
	for i, res := range resp.Kvs {
		response[i] = res.Value
	}
	return response, nil
}

//Returns number of keys that match key prefix
func (config EtcdConfig) GetCountWithPrefix(key string) (int64, error) {
	address := config.MakeAddress()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{address},
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

func (config EtcdConfig) ParseResource(res EtcdStorage, resType Resource) (Resource, error) {
	if res.StorageType != RESOURCE {
		return nil, fmt.Errorf("payload is not resource type")
	}
	if err := proto.Unmarshal(res.Message, resType.Proto()); err != nil {
		return nil, err
	}
	return resType, nil
}

//func (config Etcd) ParseJob(res EtcdStorage) ([][]byte, error) {
//
//}

func (lookup etcdResourceLookup) findType(t ResourceType) (Resource, error) {
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

func (lookup etcdResourceLookup) serializeResource(res Resource) ([]byte, error) {
	p, _ := proto.Marshal(res.Proto())
	msg := EtcdStorage{
		ResourceType: res.ID().Type,
		Message:      p,
	}
	strmsg, err := json.Marshal(msg)
	if err != nil {
		return strmsg, err
	}
	return strmsg, nil
}

func (lookup etcdResourceLookup) deserialize(value []byte) (EtcdStorage, error) {
	var msg EtcdStorage
	if err := json.Unmarshal(value, &msg); err != nil {
		return msg, fmt.Errorf("failed To Parse Resource: %s", err)
	}

	return msg, nil
}

func (lookup etcdResourceLookup) Lookup(id ResourceID) (Resource, error) {
	name := id.Name
	fmt.Printf("Name:%s\n", name)
	resp, err := lookup.connection.Get(name)
	fmt.Printf("resp:%s\n", resp)
	if err != nil {
		return nil, &ResourceNotFound{id}
	}
	msg, err := lookup.deserialize(resp)
	fmt.Printf("msg:%s\n", msg)
	if err != nil {
		return nil, err
	}
	resType, err := lookup.findType(msg.ResourceType)
	if err != nil {
		return nil, err
	}
	fmt.Printf("resType:%s\n", resType.Proto())
	resource, err := lookup.connection.ParseResource(msg, resType)
	if err != nil {
		return nil, err
	}
	fmt.Printf("resource:%s\n", resource.Proto())
	return resource, nil
}

func (lookup etcdResourceLookup) Has(id ResourceID) (bool, error) {
	name := id.Name
	count, err := lookup.connection.GetCountWithPrefix(name)
	if err != nil {
		return false, err
	}
	if count == 0 {
		return false, &ResourceNotFound{id}
	}
	return true, nil
}

func (lookup etcdResourceLookup) Set(id ResourceID, res Resource) error {
	name := id.Name
	serRes, err := lookup.serializeResource(res)
	err = lookup.connection.Put(name, string(serRes))
	if err != nil {
		return err
	}
	return nil
}

func (lookup etcdResourceLookup) Submap(ids []ResourceID) (ResourceLookup, error) {
	resources := make(localResourceLookup, len(ids))

	for _, id := range ids {

		value, err := lookup.connection.Get(id.Name)
		if err != nil {
			return nil, &ResourceNotFound{id}
		}
		etcdStore, err := lookup.deserialize(value)
		if err != nil {
			return nil, err
		}

		resource, err := lookup.findType(etcdStore.ResourceType)
		if err != nil {
			return nil, err
		}

		res, err := lookup.connection.ParseResource(etcdStore, resource)

		resources[id] = res
	}
	return resources, nil
}

func (lookup etcdResourceLookup) ListForType(t ResourceType) ([]Resource, error) {
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
		resource, err := lookup.findType(etcdStore.ResourceType)
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
		resource, err := lookup.findType(etcdStore.ResourceType)
		if err != nil {
			return nil, err
		}
		resource, err = lookup.connection.ParseResource(etcdStore, resource)
		resources = append(resources, resource)
	}
	return resources, nil
}
