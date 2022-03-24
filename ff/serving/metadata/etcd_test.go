package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	pb "github.com/featureform/serving/metadata/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"log"
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
		{"Successful Set", fields{EtcdConfig{Host: "localhost", Port: "2379"}}, args1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lookup := etcdResourceLookup{
				connection: tt.fields.Etcd,
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
			resp, err := newclient.Get(ctx, tt.args.id.Name)
			if err != nil {
				log.Fatal(err)
			}
			cancel()
			value := resp.Kvs[0].Value
			resource := featureVariantResource{
				&pb.FeatureVariant{},
			}
			var msg EtcdStorage
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
		{"Successful Lookup", fields{EtcdConfig{Host: "localhost", Port: "2379"}}, args{args1}, doWant, false},
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
		msg := EtcdStorage{
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
		_, err = newclient.Put(ctx, args1.Name, string(strmsg))
		if err != nil {
			log.Fatal(err)
		}
		cancel()

		t.Run(tt.name, func(t *testing.T) {
			lookup := etcdResourceLookup{
				connection: tt.fields.Etcd,
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
			Name: "resource2",
			Type: FEATURE,
		},
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{"Failed Has", fields{EtcdConfig{Host: "localhost", Port: "2379"}}, args1, false, true},
		{"Successful Has", fields{EtcdConfig{Host: "localhost", Port: "2379"}}, args2, true, false},
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
			msg := EtcdStorage{
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
			_, err = newclient.Put(ctx, tt.args.id.Name, string(strmsg))
			if err != nil {
				log.Fatal(err)
			}
			cancel()
		}
		t.Run(tt.name, func(t *testing.T) {
			lookup := etcdResourceLookup{
				connection: tt.fields.Etcd,
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
		{"Successful ListForType", fields{EtcdConfig{Host: "localhost", Port: "2379"}}, args{FEATURE}, featureResources, false},
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
		msg := EtcdStorage{
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
		_, err = newclient.Put(ctx, res.ID().Name, string(strmsg))
		if err != nil {
			log.Fatal(err)
		}
		cancel()
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lookup := etcdResourceLookup{
				connection: tt.fields.Etcd,
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
		{"Successful List", fields{EtcdConfig{Host: "localhost", Port: "2379"}}, featureResources, false},
	}
	for _, res := range featureResources {
		p, _ := proto.Marshal(res.Proto())
		msg := EtcdStorage{
			ResourceType: res.ID().Type,
			Message:      p,
			StorageType:  RESOURCE,
		}
		strmsg, err := json.Marshal(msg)
		if err != nil {
			log.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		_, err = newclient.Put(ctx, res.ID().Name, string(strmsg))
		if err != nil {
			log.Fatal(err)
		}
		cancel()
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lookup := etcdResourceLookup{
				connection: tt.fields.Etcd,
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
		{"Successful Submap", fields{EtcdConfig{Host: "localhost", Port: "2379"}}, args{ids: ids}, resources, false},
	}
	for _, res := range featureResources {
		p, _ := proto.Marshal(res.Proto())
		msg := EtcdStorage{
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
		_, err = client.Put(ctx, res.ID().Name, string(strmsg))
		cancel()
		if err != nil {
			log.Fatal(err)
		}
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {

			lookup := etcdResourceLookup{
				connection: tt.fields.Etcd,
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
