package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/featureform/provider"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"io/ioutil"
	"reflect"
	"testing"
	"time"
)

func TestGenerateSnapshotName(t *testing.T) {
	currentTimestamp, _ := time.Parse(time.RFC3339, "2020-11-12T10:05:01Z")
	expectedName := fmt.Sprintf("%s__%s.db", "featureform_etcd_snapshot", "2020-11-12_10:05:01")
	snapshot := GenerateSnapshotName(currentTimestamp)

	if snapshot != expectedName {
		t.Fatalf("the snapshot names do not match. Expected '%s', received '%s'", expectedName, snapshot)
	}
}

func TestBackupGetBackupProvider(t *testing.T) {
	emptyClient := myClient{}

	type fields struct {
		ETCDClient Client
		Provider   provider.FileStoreType
	}
	tests := []struct {
		name    string
		fields  fields
		want    Provider
		wantErr bool
	}{
		{"Get Empty Error", fields{emptyClient, ""}, nil, true},
		{"Get Invalid Error", fields{emptyClient, "Postgres"}, nil, true},
		{"Get Azure", fields{emptyClient, provider.Azure}, &Azure{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BackupManager{
				ETCDClient: tt.fields.ETCDClient,
				Provider:   tt.fields.Provider,
			}
			got, err := b.getBackupProvider()
			if (err != nil) != tt.wantErr {
				t.Errorf("getBackupProvider() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getBackupProvider() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBackup_Save(t *testing.T) {
	emptyClient := myClient{}

	type fields struct {
		ETCDClient Client
		Provider   provider.FileStoreType
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"Error Invalid Provider", fields{emptyClient, ""}, args{""}, true},
		{"Local Provider", fields{emptyClient, provider.FileSystem}, args{"src.json"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BackupManager{
				ETCDClient: tt.fields.ETCDClient,
				Provider:   tt.fields.Provider,
			}
			if err := b.Save(tt.args.name); (err != nil) != tt.wantErr {
				t.Errorf("Save() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

type myClient struct{}

var writeValues = []*mvccpb.KeyValue{
	{
		Key:   []byte("key1"),
		Value: []byte("value1"),
	},
	{
		Key:   []byte("key2"),
		Value: []byte("value2"),
	},
	{
		Key:   []byte("key3"),
		Value: []byte("value3"),
	},
}

func (c myClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	resp := pb.RangeResponse{
		Kvs: writeValues,
	}
	getResp := clientv3.GetResponse(resp)
	return &getResp, nil
}

func TestBackup_takeSnapshot(t *testing.T) {

	client := myClient{}

	type fields struct {
		ETCDClient Client
		Provider   provider.FileStoreType
	}
	type args struct {
		filename string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"",
			fields{
				ETCDClient: client,
				Provider:   provider.FileSystem,
			},
			args{
				"Test.json",
			},
			false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BackupManager{
				ETCDClient: tt.fields.ETCDClient,
				Provider:   tt.fields.Provider,
			}
			if err := b.takeSnapshot(tt.args.filename); (err != nil) != tt.wantErr {
				t.Errorf("takeSnapshot() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
		file, err := ioutil.ReadFile(tt.args.filename)
		if err != nil {
			return
		}
		bFile := []backupRow{}
		err = json.Unmarshal(file, &bFile)
		if err != nil {
			t.Fatalf(err.Error())
		}
		for i, row := range bFile {
			if bytes.Compare(row.Key, writeValues[i].Key) != 0 && bytes.Compare(row.Value, writeValues[i].Value) != 0 {
				t.Errorf("Expected (%s:%s), got (%s:%s)", string(writeValues[i].Key), string(writeValues[i].Value), string(row.Key), string(row.Value))
			}
		}
	}
}
