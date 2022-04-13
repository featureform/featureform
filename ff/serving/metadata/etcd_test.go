package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	pb "github.com/featureform/serving/metadata/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
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
		log.Fatal(err)
	}
	etcd.client = client
}

func (etcd *Etcd) clearDatabase() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	_, err := etcd.client.Delete(ctx, "", clientv3.WithPrefix())
	cancel()
	if err != nil {
		log.Fatal(err)
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
		ResourceID{Name: "test", Variant: FEATURE_VARIANT, Type: FEATURE},
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "featureVariantResource",
			Type:    FEATURE_VARIANT,
			Created: time.Now().String(),
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
				t.Errorf("Set() could not initialize client: %s", err)
			}
			lookup := etcdResourceLookup{
				connection: store,
			}
			if err := lookup.Set(tt.args.id, tt.args.res); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
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
				log.Fatal(err)
			}
			cancel()
			value := resp.Kvs[0].Value
			resource := featureVariantResource{
				&pb.FeatureVariant{},
			}

			var msg EtcdRow
			if err := json.Unmarshal(value, &msg); err != nil {
				log.Fatalln("Failed To Parse Resource", err)
			}
			if err := proto.Unmarshal(msg.Message, resource.Proto()); err != nil {
				log.Fatalln("Failed to parse:", err)
			}
			if !proto.Equal(args1.res.Proto(), resource.Proto()) {
				t.Errorf("Set() Expected: %v, Received: %v", args1.res.Proto(), resource.Proto())
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
		Type:    FEATURE_VARIANT,
		Created: time.Now().String(),
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
			fmt.Println(err)
		}
		p, _ := proto.Marshal(doWant.Proto())
		msg := EtcdRow{
			ResourceType: args1.Type,
			Message:      p,
			StorageType:  RESOURCE,
		}
		if err != nil {
			log.Fatal(err)
		}

		strmsg, err := json.Marshal(msg)
		if err != nil {
			log.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		_, err = newclient.Put(ctx, createKey(args1), string(strmsg))
		if err != nil {
			log.Fatal(err)
		}
		cancel()

		t.Run(tt.name, func(t *testing.T) {
			client, err := tt.fields.Etcd.initClient()
			store := EtcdStorage{
				Client: client,
			}
			if err != nil {
				t.Errorf("Lookup() could not initialize client: %s", err)
			}
			lookup := etcdResourceLookup{
				connection: store,
			}
			got, err := lookup.Lookup(tt.args.id)

			if (err != nil) != tt.wantErr {
				t.Errorf("Lookup() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if proto.Equal(got.Proto(), tt.want.Proto()) {
				t.Errorf("Lookup() got = %v, want %v", got.Proto(), tt.want.Proto())
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
		Type:    FEATURE_VARIANT,
		Created: time.Now().String(),
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
				fmt.Println(err)
			}
			p, _ := proto.Marshal(doWant.Proto())
			msg := EtcdRow{
				ResourceType: tt.args.id.Type,
				Message:      p,
				StorageType:  RESOURCE,
			}
			if err != nil {
				log.Fatal(err)
			}

			strmsg, err := json.Marshal(msg)
			if err != nil {
				log.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
			_, err = newclient.Put(ctx, createKey(tt.args.id), string(strmsg))
			if err != nil {
				log.Fatal(err)
			}
			cancel()
		}
		t.Run(tt.name, func(t *testing.T) {
			client, err := tt.fields.Etcd.initClient()
			store := EtcdStorage{
				Client: client,
			}
			if err != nil {
				t.Errorf("Has() could not initialize client: %s", err)
			}
			lookup := etcdResourceLookup{
				connection: store,
			}
			got, err := lookup.Has(tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("Has() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Has() got = %v, want %v", got, tt.want)
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
			Type:    FEATURE_VARIANT,
			Created: time.Now().String(),
		}},
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature2",
			Type:    FEATURE_VARIANT,
			Created: time.Now().String(),
		}},
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature3",
			Type:    FEATURE_VARIANT,
			Created: time.Now().String(),
		}},
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []Resource
		wantErr bool
	}{
		{"Successful ListForType", fields{EtcdConfig{[]EtcdNode{{Host: "localhost", Port: "2379"}}}}, args{FEATURE}, featureResources, false},
	}
	newclient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: time.Second * 1,
	})
	if err != nil {
		fmt.Println(err)
	}
	for _, res := range featureResources {
		p, _ := proto.Marshal(res.Proto())
		msg := EtcdRow{
			ResourceType: res.ID().Type,
			Message:      p,
			StorageType:  RESOURCE,
		}
		if err != nil {
			log.Fatal(err)
		}
		strmsg, err := json.Marshal(msg)
		if err != nil {
			log.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		_, err = newclient.Put(ctx, createKey(res.ID()), string(strmsg))
		if err != nil {
			log.Fatal(err)
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
				t.Errorf("ListForType() could not initialize client: %s", err)
			}
			lookup := etcdResourceLookup{
				connection: store,
			}
			got, err := lookup.ListForType(tt.args.t)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListForType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for i, r := range got {
				if !proto.Equal(r.Proto(), tt.want[i].Proto()) {
					t.Errorf("ListForType() got = %v, want %v", r.Proto(), tt.want[i].Proto())
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
		log.Fatal(err)
	}
	type fields struct {
		Etcd EtcdConfig
	}

	featureResources := []Resource{
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature1",
			Type:    FEATURE_VARIANT,
			Created: time.Now().String(),
		}},
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature2",
			Type:    FEATURE_VARIANT,
			Created: time.Now().String(),
		}},
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature3",
			Type:    FEATURE_VARIANT,
			Created: time.Now().String(),
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
			log.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		_, err = newclient.Put(ctx, createKey(res.ID()), string(strmsg))
		if err != nil {
			log.Fatal(err)
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
				t.Errorf("List() could not initialize client: %s", err)
			}
			lookup := etcdResourceLookup{
				connection: store,
			}
			got, err := lookup.List()
			if (err != nil) != tt.wantErr {
				t.Errorf("List() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for i, r := range got {
				if !proto.Equal(r.Proto(), tt.want[i].Proto()) {
					t.Errorf("List() got = %v, want %v", r.Proto(), tt.want[i].Proto())
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
		log.Fatal(err)
	}
	type fields struct {
		Etcd EtcdConfig
	}
	type args struct {
		ids []ResourceID
	}

	ids := []ResourceID{
		{Name: "feature1", Type: FEATURE},
		{Name: "feature2", Type: FEATURE},
		{Name: "feature3", Type: FEATURE},
	}

	featureResources := []Resource{
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature1",
			Type:    FEATURE_VARIANT,
			Created: time.Now().String(),
		}},
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature2",
			Type:    FEATURE_VARIANT,
			Created: time.Now().String(),
		}},
		&featureVariantResource{&pb.FeatureVariant{
			Name:    "feature3",
			Type:    FEATURE_VARIANT,
			Created: time.Now().String(),
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
		if err != nil {
			log.Fatal(err)
		}

		strmsg, err := json.Marshal(msg)
		if err != nil {
			log.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		_, err = client.Put(ctx, createKey(res.ID()), string(strmsg))
		cancel()
		if err != nil {
			log.Fatal(err)
		}
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {

			client, err := tt.fields.Etcd.initClient()
			store := EtcdStorage{
				Client: client,
			}
			if err != nil {
				t.Errorf("Submap() could not initialize client: %s", err)
			}
			lookup := etcdResourceLookup{
				connection: store,
			}

			got, err := lookup.Submap(tt.args.ids)
			if (err != nil) != tt.wantErr {
				t.Errorf("Submap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			elem, err := got.List()
			if err != nil {
				log.Fatal(err)
			}

			for i, res := range elem {
				if err != nil {
					t.Errorf("%s\n", err)
					t.Errorf("Submap(): Error with lookup:  %v\n", res.Proto())
				}
				if !proto.Equal(res.Proto(), featureResources[i].Proto()) {
					t.Errorf("Submap():\ngot:  %v\nwant: %v", res.Proto(), featureResources[i].Proto())
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
		{"Test Feature", fields{}, args{TRANSFORMATION}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := EtcdConfig{[]EtcdNode{{Host: "localhost", Port: "2379"}}}
			client, err := config.initClient()
			store := EtcdStorage{
				Client: client,
			}
			if err != nil {
				t.Errorf("createEmptyResource() could not initialize client: %s", err)
			}
			lookup := etcdResourceLookup{
				connection: store,
			}
			got, err := lookup.createEmptyResource(tt.args.t)
			if (err != nil) != tt.wantErr {
				t.Errorf("createEmptyResource() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createEmptyResource() got = %v, want %v", got, tt.want)
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
		{"Test Put Success", fields{"localhost", "2379"}, args{key: "", value: ""}, false},
		{"Test Put Error", fields{"localhost", ""}, args{key: "", value: ""}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := EtcdConfig{[]EtcdNode{{Host: tt.fields.Host, Port: tt.fields.Port}}}
			c, err := config.initClient()
			if err != nil {
				t.Errorf("Put() could not initialize client: %s", err)
			}
			client := EtcdStorage{c}
			if err := client.Put(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
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
		{"Test Invalid Key", fields{"localhost", "2379"}, args{key: "testkey"}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := EtcdConfig{[]EtcdNode{{Host: tt.fields.Host, Port: tt.fields.Port}}}
			c, err := config.initClient()
			if err != nil {
				t.Errorf("Get() could not initialize client: %s", err)
			}
			client := EtcdStorage{c}
			got, err := client.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
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
			store := EtcdStorage{
				Client: client,
			}
			if err != nil {
				t.Errorf("GetWithPrefix() could not initialize client: %s", err)
			}
			got, err := store.GetWithPrefix(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetWithPrefix() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetWithPrefix() got = %v, want %v", got, tt.want)
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
			store := EtcdStorage{
				Client: client,
			}
			if err != nil {
				t.Errorf("GetCountWithPrefix() could not initialize client: %s", err)
			}
			got, err := store.GetCountWithPrefix(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetCountWithPrefix() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetCountWithPrefix() got = %v, want %v", got, tt.want)
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
			if err != nil {
				t.Errorf("ParseResource() could not initialize client: %s", err)
			}
			got, err := store.ParseResource(tt.args.res, tt.args.resType)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseResource() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseResource() got = %v, want %v", got, tt.want)
			}
		})
	}
}
