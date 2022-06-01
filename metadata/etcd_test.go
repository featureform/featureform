// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	pb "github.com/featureform/metadata/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"reflect"
	"testing"
	"time"
)

type Etcd struct {
	client *clientv3.Client
}

type etcdHelpers interface {
	init() error
	clearDatabase()
}

func (etcd *Etcd) init() {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: time.Second * 1,
	})
	if err != nil {
		log.Fatalf("Could not connect to etcd client: %v", err)
	}
	etcd.client = client
}

func (etcd *Etcd) clearDatabase() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	_, err := etcd.client.Delete(ctx, "", clientv3.WithPrefix())
	cancel()
	if err != nil {
		log.Fatalf("Could not clear database: %v", err)
	}
}

func Test_etcdResourceLookup_Set(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	type fields struct {
		Etcd EtcdConfig
	}
	type args struct {
		id  ResourceID
		res Resource
	}

	args1 := args{
		ResourceID{Name: "test", Variant: FEATURE_VARIANT.String(), Type: FEATURE},
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "featureVariantResource",
			Type:    FEATURE_VARIANT.String(),
			Created: tspb.Now(),
		}},
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"Successful Set", fields{EtcdConfig{[]EtcdNode{{Host: "localhost", Port: "2379"}}}}, args1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := tt.fields.Etcd.initClient()
			store := EtcdStorage{
				Client: client,
			}
			if err != nil {
				t.Fatalf("Set() could not initialize client: %s", err)
			}
			lookup := etcdResourceLookup{
				connection: store,
			}
			if err := lookup.Set(tt.args.id, tt.args.res); (err != nil) != tt.wantErr {
				t.Fatalf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}

			newclient, err := clientv3.New(clientv3.Config{
				Endpoints:   []string{"localhost:2379"},
				DialTimeout: time.Second * 1,
			})
			if err != nil {
				fmt.Println(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
			resp, err := newclient.Get(ctx, createKey(tt.args.id))
			if err != nil {
				t.Fatalf("Could not get from client: %v", err)
			}
			cancel()
			value := resp.Kvs[0].Value
			resource := featureVariantResource{
				&pb.FeatureVariant{},
			}

			var msg EtcdRowTemp
			if err := json.Unmarshal(value, &msg); err != nil {
				log.Fatalln("Failed To Parse Resource", err)
			}
			if err := proto.Unmarshal(msg.Message, resource.Proto()); err != nil {
				log.Fatalln("Failed to parse:", err)
			}
			if !proto.Equal(args1.res.Proto(), resource.Proto()) {
				t.Fatalf("Set() Expected: %v, Received: %v", args1.res.Proto(), resource.Proto())
			}
		})
	}
	connect := Etcd{}
	connect.init()
	t.Cleanup(connect.clearDatabase)
}

func Test_etcdResourceLookup_Lookup(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	doWant := &featureVariantResource{&pb.FeatureVariant{
		Name:    "featureVariant",
		Type:    FEATURE_VARIANT.String(),
		Created: tspb.Now(),
	}}

	args1 := ResourceID{Name: "test2", Type: FEATURE}

	type fields struct {
		Etcd EtcdConfig
	}
	type args struct {
		id ResourceID
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    Resource
		wantErr bool
	}{
		{"Successful Lookup", fields{EtcdConfig{[]EtcdNode{{Host: "localhost", Port: "2379"}}}}, args{args1}, doWant, false},
	}
	for _, tt := range tests {
		newclient, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"localhost:2379"},
			DialTimeout: time.Second * 1,
		})
		if err != nil {
			t.Fatalf("Could not create new etcd client: %v", err)
		}
		p, _ := proto.Marshal(doWant.Proto())
		msg := EtcdRow{
			ResourceType: args1.Type,
			Message:      p,
			StorageType:  RESOURCE,
		}

		strmsg, err := json.Marshal(msg)
		if err != nil {
			t.Fatalf("Could not marshall string message: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		_, err = newclient.Put(ctx, createKey(args1), string(strmsg))
		if err != nil {
			t.Fatalf("Could not put key: %v", err)
		}
		cancel()

		t.Run(tt.name, func(t *testing.T) {
			client, err := tt.fields.Etcd.initClient()
			store := EtcdStorage{
				Client: client,
			}
			if err != nil {
				t.Fatalf("Lookup() could not initialize client: %s", err)
			}
			lookup := etcdResourceLookup{
				connection: store,
			}
			got, err := lookup.Lookup(tt.args.id)

			if (err != nil) != tt.wantErr {
				t.Fatalf("Lookup() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if proto.Equal(got.Proto(), tt.want.Proto()) {
				t.Fatalf("Lookup() got = %v, want %v", got.Proto(), tt.want.Proto())
			}
		})
	}
	connect := Etcd{}
	connect.init()
	t.Cleanup(connect.clearDatabase)
}

func Test_etcdResourceLookup_Has(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	type fields struct {
		Etcd EtcdConfig
	}
	type args struct {
		id ResourceID
	}
	doWant := &featureVariantResource{&pb.FeatureVariant{
		Name:    "resource1",
		Type:    FEATURE_VARIANT.String(),
		Created: tspb.Now(),
	}}
	args1 := args{
		ResourceID{
			Name: "resource1",
			Type: FEATURE,
		},
	}
	args2 := args{
		ResourceID{
			Name: "resource1",
			Type: FEATURE_VARIANT,
		},
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{"Does not have", fields{EtcdConfig{[]EtcdNode{{Host: "localhost", Port: "2379"}}}}, args1, false, false},
		{"Successful Has", fields{EtcdConfig{[]EtcdNode{{Host: "localhost", Port: "2379"}}}}, args2, true, false},
	}
	for _, tt := range tests {
		if tt.want {
			newclient, err := clientv3.New(clientv3.Config{
				Endpoints:   []string{"localhost:2379"},
				DialTimeout: time.Second * 1,
			})
			if err != nil {
				t.Fatalf("Could not connect to client: %v", err)
			}
			p, _ := proto.Marshal(doWant.Proto())
			msg := EtcdRow{
				ResourceType: tt.args.id.Type,
				Message:      p,
				StorageType:  RESOURCE,
			}

			strmsg, err := json.Marshal(msg)
			if err != nil {
				t.Fatalf("Could not marshal string message: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
			_, err = newclient.Put(ctx, createKey(tt.args.id), string(strmsg))
			if err != nil {
				t.Fatalf("Could not put key: %v", err)
			}
			cancel()
		}
		t.Run(tt.name, func(t *testing.T) {
			client, err := tt.fields.Etcd.initClient()
			store := EtcdStorage{
				Client: client,
			}
			if err != nil {
				t.Fatalf("Has() could not initialize client: %s", err)
			}
			lookup := etcdResourceLookup{
				connection: store,
			}
			got, err := lookup.Has(tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Has() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Fatalf("Has() got = %v, want %v", got, tt.want)
			}
		})
	}
	connect := Etcd{}
	connect.init()
	t.Cleanup(connect.clearDatabase)
}

func Test_etcdResourceLookup_ListForType(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	type fields struct {
		Etcd EtcdConfig
	}
	type args struct {
		t ResourceType
	}

	featureResources := []Resource{
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature1",
			Created: tspb.Now(),
		}},
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature2",
			Created: tspb.Now(),
		}},
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature3",
			Created: tspb.Now(),
		}},
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []Resource
		wantErr bool
	}{
		{"Successful ListForType", fields{EtcdConfig{[]EtcdNode{{Host: "localhost", Port: "2379"}}}}, args{FEATURE_VARIANT}, featureResources, false},
	}
	newclient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: time.Second * 1,
	})
	if err != nil {
		t.Fatalf("Could not create new client: %v", err)
	}
	for _, res := range featureResources {
		p, _ := proto.Marshal(res.Proto())
		msg := EtcdRow{
			ResourceType: res.ID().Type,
			Message:      p,
			StorageType:  RESOURCE,
		}
		strmsg, err := json.Marshal(msg)
		if err != nil {
			t.Fatalf("Could not marshal string message: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		_, err = newclient.Put(ctx, createKey(res.ID()), string(strmsg))
		if err != nil {
			t.Fatalf("Could not put key: %v", err)
		}
		cancel()
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := tt.fields.Etcd.initClient()
			store := EtcdStorage{
				Client: client,
			}
			if err != nil {
				t.Fatalf("ListForType() could not initialize client: %s", err)
			}
			lookup := etcdResourceLookup{
				connection: store,
			}
			got, err := lookup.ListForType(tt.args.t)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ListForType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for i, r := range got {
				if !proto.Equal(r.Proto(), tt.want[i].Proto()) {
					t.Fatalf("ListForType() got = %v, want %v", r.Proto(), tt.want[i].Proto())
				}
			}
		})
	}
	connect := Etcd{}
	connect.init()
	t.Cleanup(connect.clearDatabase)
}

func Test_etcdResourceLookup_List(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	newclient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: time.Second * 1,
	})
	if err != nil {
		t.Fatalf("Could not create new client: %v", err)
	}
	type fields struct {
		Etcd EtcdConfig
	}

	featureResources := []Resource{
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature1",
			Created: tspb.Now(),
		}},
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature2",
			Created: tspb.Now(),
		}},
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature3",
			Created: tspb.Now(),
		}},
	}

	tests := []struct {
		name    string
		fields  fields
		want    []Resource
		wantErr bool
	}{
		{"Successful List", fields{EtcdConfig{[]EtcdNode{{Host: "localhost", Port: "2379"}}}}, featureResources, false},
	}
	for _, res := range featureResources {
		p, _ := proto.Marshal(res.Proto())
		msg := EtcdRow{
			ResourceType: res.ID().Type,
			Message:      p,
			StorageType:  RESOURCE,
		}
		strmsg, err := json.Marshal(msg)
		if err != nil {
			t.Fatalf("Could not marshal string message: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		_, err = newclient.Put(ctx, createKey(res.ID()), string(strmsg))
		if err != nil {
			t.Fatalf("Could not put key: %v", err)
		}
		cancel()
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := tt.fields.Etcd.initClient()
			store := EtcdStorage{
				Client: client,
			}
			if err != nil {
				t.Fatalf("List() could not initialize client: %s", err)
			}
			lookup := etcdResourceLookup{
				connection: store,
			}
			got, err := lookup.List()
			if (err != nil) != tt.wantErr {
				t.Fatalf("List() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for i, r := range got {
				if !proto.Equal(r.Proto(), tt.want[i].Proto()) {
					t.Fatalf("List() got = %v, want %v", r.Proto(), tt.want[i].Proto())
				}
			}
		})
	}
	connect := Etcd{}
	connect.init()
	t.Cleanup(connect.clearDatabase)
}

func Test_etcdResourceLookup_Submap(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: time.Second * 1,
	})
	if err != nil {
		t.Fatalf("Could not connect to client: %v", err)
	}
	type fields struct {
		Etcd EtcdConfig
	}
	type args struct {
		ids []ResourceID
	}

	ids := []ResourceID{
		{Name: "feature1", Type: FEATURE_VARIANT},
		{Name: "feature2", Type: FEATURE_VARIANT},
		{Name: "feature3", Type: FEATURE_VARIANT},
	}

	featureResources := []Resource{
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature1",
			Created: tspb.Now(),
		}},
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature2",
			Created: tspb.Now(),
		}},
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature3",
			Created: tspb.Now(),
		}}}

	resources := localResourceLookup{
		ids[0]: featureResources[0],
		ids[1]: featureResources[1],
		ids[2]: featureResources[2],
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    ResourceLookup
		wantErr bool
	}{
		{"Successful Submap", fields{EtcdConfig{[]EtcdNode{{Host: "localhost", Port: "2379"}}}}, args{ids: ids}, resources, false},
	}
	for _, res := range featureResources {
		p, _ := proto.Marshal(res.Proto())
		msg := EtcdRow{
			ResourceType: res.ID().Type,
			Message:      p,
			StorageType:  RESOURCE,
		}
		strmsg, err := json.Marshal(msg)
		if err != nil {
			t.Fatalf("Could not marshal string message %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		_, err = client.Put(ctx, createKey(res.ID()), string(strmsg))
		cancel()
		if err != nil {
			t.Fatalf("Could not put key: %v", err)
		}
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {

			client, err := tt.fields.Etcd.initClient()
			store := EtcdStorage{
				Client: client,
			}
			if err != nil {
				t.Fatalf("Submap() could not initialize client: %s", err)
			}
			lookup := etcdResourceLookup{
				connection: store,
			}

			got, err := lookup.Submap(tt.args.ids)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Submap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			elem, err := got.List()
			if err != nil {
				t.Fatalf("Could not get list from submap: %v", err)
			}

			for _, res := range elem {
				if err != nil {
					t.Fatalf("%s\n", err)
					t.Fatalf("Submap(): Error with lookup:  %v\n", res.Proto())
				}
				has := false
				for _, expected := range featureResources {
					if proto.Equal(res.Proto(), expected.Proto()) {
						has = true
						break
					}
				}
				if !has {
					t.Fatalf("Submap() item not in expected list:\ngot:  %v\n ", res.Proto())
				}

			}
		})
	}
	connect := Etcd{}
	connect.init()
	t.Cleanup(connect.clearDatabase)
}

func Test_etcdResourceLookup_findResourceType(t *testing.T) {
	type fields struct {
		connection EtcdConfig
	}
	type args struct {
		t ResourceType
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    Resource
		wantErr bool
	}{
		{"Test Feature", fields{}, args{FEATURE}, &featureResource{&pb.Feature{}}, false},
		{"Test Feature Variant", fields{}, args{FEATURE_VARIANT}, &featureVariantResource{&pb.FeatureVariant{}}, false},
		{"Test Label", fields{}, args{LABEL}, &labelResource{&pb.Label{}}, false},
		{"Test Label Variant", fields{}, args{LABEL_VARIANT}, &labelVariantResource{&pb.LabelVariant{}}, false},
		{"Test User", fields{}, args{USER}, &userResource{&pb.User{}}, false},
		{"Test Entity", fields{}, args{ENTITY}, &entityResource{&pb.Entity{}}, false},
		{"Test Provider", fields{}, args{PROVIDER}, &providerResource{&pb.Provider{}}, false},
		{"Test Source", fields{}, args{SOURCE}, &sourceResource{&pb.Source{}}, false},
		{"Test Source Variant", fields{}, args{SOURCE_VARIANT}, &sourceVariantResource{&pb.SourceVariant{}}, false},
		{"Test Training Set", fields{}, args{TRAINING_SET}, &trainingSetResource{&pb.TrainingSet{}}, false},
		{"Test Training Set Variant", fields{}, args{TRAINING_SET_VARIANT}, &trainingSetVariantResource{&pb.TrainingSetVariant{}}, false},
		{"Test Model", fields{}, args{MODEL}, &modelResource{&pb.Model{}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := EtcdConfig{[]EtcdNode{{Host: "localhost", Port: "2379"}}}
			client, err := config.initClient()
			store := EtcdStorage{
				Client: client,
			}
			if err != nil {
				t.Fatalf("createEmptyResource() could not initialize client: %s", err)
			}
			lookup := etcdResourceLookup{
				connection: store,
			}
			got, err := lookup.createEmptyResource(tt.args.t)
			if (err != nil) != tt.wantErr {
				t.Fatalf("createEmptyResource() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("createEmptyResource() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEtcdConfig_Put(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	type fields struct {
		Host string
		Port string
	}
	type args struct {
		key   string
		value string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"Test Put Success", fields{"localhost", "2379"}, args{key: "key", value: "value"}, false},
		{"Test Put Error", fields{"localhost", ""}, args{key: "", value: ""}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := EtcdConfig{[]EtcdNode{{Host: tt.fields.Host, Port: tt.fields.Port}}}
			c, err := config.initClient()
			if err != nil && !tt.wantErr {
				t.Errorf("Put() could not initialize client: %v", err)
			} else if err != nil && tt.wantErr {
				return
			}
			client := EtcdStorage{c}
			if err := client.Put(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Fatalf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEtcdConfig_Get(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	type fields struct {
		Host string
		Port string
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{"Test Invalid Server", fields{"localhost", ""}, args{key: ""}, nil, true},
		{"Test Invalid Key", fields{"localhost", "2379"}, args{key: "testkey"}, []byte{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := EtcdConfig{[]EtcdNode{{Host: tt.fields.Host, Port: tt.fields.Port}}}
			c, err := config.initClient()
			if err != nil && !tt.wantErr {
				t.Errorf("Get() could not initialize client: %v", err)
			} else if err != nil && tt.wantErr {
				return
			}
			client := EtcdStorage{c}
			got, err := client.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEtcdConfig_GetWithPrefix(t *testing.T) {
	type fields struct {
		Host string
		Port string
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    [][]byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := EtcdConfig{[]EtcdNode{{Host: "localhost", Port: "2379"}}}
			client, err := config.initClient()
			if err != nil && !tt.wantErr {
				t.Errorf("GetWithPrefix() could not initialize client: %v", err)
			} else if err != nil && tt.wantErr {
				return
			}
			store := EtcdStorage{
				Client: client,
			}
			got, err := store.GetWithPrefix(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Fatalf("GetWithPrefix() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("GetWithPrefix() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEtcdConfig_GetCountWithPrefix(t *testing.T) {
	type fields struct {
		Host string
		Port string
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := EtcdConfig{[]EtcdNode{{Host: "localhost", Port: "2379"}}}
			client, err := config.initClient()
			if err != nil {
				t.Fatalf("GetCountWithPrefix() could not initialize client: %s", err)
			}
			store := EtcdStorage{
				Client: client,
			}
			if err != nil && !tt.wantErr {
				t.Errorf("GetCountWithPrefix() could not initialize client: %v", err)
			} else if err != nil && tt.wantErr {
				return
			}
			got, err := store.GetCountWithPrefix(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Fatalf("GetCountWithPrefix() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Fatalf("GetCountWithPrefix() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEtcdConfig_ParseResource(t *testing.T) {
	type fields struct {
		Host string
		Port string
	}
	type args struct {
		res     EtcdRow
		resType Resource
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    Resource
		wantErr bool
	}{
		{"Test Invalid Type", fields{Host: "", Port: ""}, args{EtcdRow{StorageType: JOB}, &featureResource{}}, nil, true},
		{"Test Nil Message", fields{Host: "", Port: ""}, args{EtcdRow{StorageType: RESOURCE, Message: nil}, &featureResource{}}, nil, true},
		{"Test Failed Message", fields{Host: "", Port: ""}, args{EtcdRow{StorageType: RESOURCE}, &featureResource{}}, nil, true},
		{"Test Failed Resource", fields{Host: "", Port: ""}, args{EtcdRow{StorageType: RESOURCE, Message: []byte("test")}, &featureResource{&pb.Feature{}}}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := EtcdConfig{[]EtcdNode{{Host: "localhost", Port: "2379"}}}
			client, err := config.initClient()
			store := EtcdStorage{
				Client: client,
			}
			if err != nil && !tt.wantErr {
				t.Errorf("ParseResource() could not initialize client: %v", err)
			} else if err != nil && tt.wantErr {
				return
			}
			got, err := store.ParseResource(tt.args.res, tt.args.resType)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseResource() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ParseResource() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCoordinatorScheduleJobSerialize(t *testing.T) {
	resID := ResourceID{Name: "test", Variant: "foo", Type: FEATURE}
	scheduleJob := &CoordinatorScheduleJob{
		Attempts: 0,
		Resource: resID,
		Schedule: "* * * * *",
	}
	serialized, err := scheduleJob.Serialize()
	if err != nil {
		t.Fatalf("Could not serialize schedule job")
	}
	copyScheduleJob := &CoordinatorScheduleJob{}
	if err := copyScheduleJob.Deserialize(serialized); err != nil {
		t.Fatalf("Could not deserialize schedule job")
	}
	if !reflect.DeepEqual(copyScheduleJob, scheduleJob) {
		t.Fatalf("Information changed on serialization and deserialization for schedule job")
	}
}

func TestGetJobKeys(t *testing.T) {
	resID := ResourceID{Name: "test", Variant: "foo", Type: FEATURE}
	expectedJobKey := "JOB__FEATURE__test__foo"
	expectedScheduleJobKey := "SCHEDULEJOB__FEATURE__test__foo"
	jobKey := GetJobKey(resID)
	scheduleJobKey := GetScheduleJobKey(resID)
	if jobKey != expectedJobKey {
		t.Fatalf("Could not generate correct job key")
	}
	if scheduleJobKey != expectedScheduleJobKey {
		t.Fatalf("Could not generate correct schedule job key")
	}
}
