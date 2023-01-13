package backup

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"reflect"
	"testing"
)

func TestBackupGetBackupProvider(t *testing.T) {
	type fields struct {
		ETCDConfig   clientv3.Config
		ProviderType ProviderType
	}
	tests := []struct {
		name    string
		fields  fields
		want    Provider
		wantErr bool
	}{
		{"Get Empty Error", fields{clientv3.Config{}, ""}, nil, true},
		{"Get Invalid Error", fields{clientv3.Config{}, "Postgres"}, nil, true},
		{"Get Azure", fields{clientv3.Config{}, AZURE}, &Azure{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Backup{
				ETCDConfig:   tt.fields.ETCDConfig,
				ProviderType: tt.fields.ProviderType,
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
	type fields struct {
		ETCDConfig   clientv3.Config
		ProviderType ProviderType
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
		{"Error Invalid Provider", fields{clientv3.Config{}, ""}, args{""}, true},
		{"Error Invalid Provider", fields{clientv3.Config{}, LOCAL}, args{"src.json"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Backup{
				ETCDConfig:   tt.fields.ETCDConfig,
				ProviderType: tt.fields.ProviderType,
			}
			if err := b.Save(tt.args.name); (err != nil) != tt.wantErr {
				t.Errorf("Save() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
