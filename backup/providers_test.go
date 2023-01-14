package backup

import (
	"errors"
	help "github.com/featureform/helpers"
	"github.com/featureform/provider"
	"github.com/joho/godotenv"
	"os"
	"testing"
)

func TestLocalInit(t *testing.T) {
	type fields struct {
		store provider.FileStore
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{"Root Path", fields{nil}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &Local{
				store: tt.fields.store,
			}
			if err := fs.Init(); (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLocalUpload(t *testing.T) {
	type fields struct {
		store provider.FileStore
	}
	type args struct {
		name string
		dest string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"Simple Upload", fields{nil}, args{"src.json", "dest.json"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &Local{
				store: tt.fields.store,
			}
			if f, err := os.Create(tt.args.name); err != nil {
				t.Fatalf("Create error = %v", err)
			} else {
				f.Close()
			}
			if err := fs.Init(); (err != nil) != tt.wantErr {
				t.Fatalf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := fs.Upload(tt.args.name, tt.args.dest); (err != nil) != tt.wantErr {
				t.Errorf("Upload() error = %v, wantErr %v", err, tt.wantErr)
			}
			if _, err := os.Stat(tt.args.dest); errors.Is(err, os.ErrNotExist) {
				t.Errorf("Check error: cannot file destination file: %v", err)
			}
		})
	}
}

func TestAzure_Init(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	_ = godotenv.Load(".env")
	type fields struct {
		AzureStorageAccount string
		AzureStorageKey     string
		AzureContainerName  string
		AzureStoragePath    string
		store               provider.FileStore
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			"Success",
			fields{
				AzureStorageAccount: help.GetEnv("AZURE_STORAGE_ACCOUNT", ""),
				AzureStorageKey:     help.GetEnv("AZURE_STORAGE_TOKEN", ""),
				AzureContainerName:  help.GetEnv("AZURE_CONTAINER_NAME", ""),
				AzureStoragePath:    help.GetEnv("AZURE_STORAGE_PATH", ""),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			az := &Azure{
				store: tt.fields.store,
			}
			if err := az.Init(); (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAzure_Upload(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	_ = godotenv.Load(".env")
	type fields struct {
		AzureStorageAccount string
		AzureStorageKey     string
		AzureContainerName  string
		AzureStoragePath    string
		store               provider.FileStore
	}
	type args struct {
		name string
		dest string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Simple Upload",
			fields{
				AzureStorageAccount: help.GetEnv("AZURE_STORAGE_ACCOUNT", ""),
				AzureStorageKey:     help.GetEnv("AZURE_STORAGE_TOKEN", ""),
				AzureContainerName:  help.GetEnv("AZURE_CONTAINER_NAME", ""),
				AzureStoragePath:    help.GetEnv("AZURE_STORAGE_PATH", ""),
			},
			args{
				"src.json",
				"/uploadtest/dest.json"},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			az := &Azure{
				store: tt.fields.store,
			}
			if f, err := os.Create(tt.args.name); err != nil {
				t.Fatalf("Create error = %v", err)
			} else {
				f.Close()
			}
			if err := az.Init(); (err != nil) != tt.wantErr {
				t.Fatalf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := az.Upload(tt.args.name, tt.args.dest); (err != nil) != tt.wantErr {
				t.Errorf("Upload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
