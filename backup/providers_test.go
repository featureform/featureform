package backup

import (
	"errors"
	"os"
	"testing"

	fs "github.com/featureform/filestore"
	help "github.com/featureform/helpers"
	"github.com/featureform/provider"
	"github.com/joho/godotenv"
)

func TestLocalInit(t *testing.T) {
	type fields struct {
		Path  string
		store provider.FileStore
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{"Root Path", fields{"file://./", nil}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &Local{
				Path:  tt.fields.Path,
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
		Path  string
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
		{"Simple Upload", fields{"file://./", nil}, args{"src.json", "dest.json"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &Local{
				Path:  tt.fields.Path,
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
				AzureStorageAccount: help.GetEnv("AZURE_ACCOUNT_NAME", ""),
				AzureStorageKey:     help.GetEnv("AZURE_ACCOUNT_KEY", ""),
				AzureContainerName:  help.GetEnv("AZURE_CONTAINER_NAME", ""),
				AzureStoragePath:    help.GetEnv("AZURE_BACKUP_STORAGE_PATH", ""),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			az := &Azure{
				AccountName:   tt.fields.AzureStorageAccount,
				AccountKey:    tt.fields.AzureStorageKey,
				ContainerName: tt.fields.AzureContainerName,
				Path:          tt.fields.AzureStoragePath,
				store:         tt.fields.store,
			}
			if err := az.Init(); (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TODO: convert into file-store agnostic test and add cases for other file stores
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
		src  string
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
				AccountName:   tt.fields.AzureStorageAccount,
				AccountKey:    tt.fields.AzureStorageKey,
				ContainerName: tt.fields.AzureContainerName,
				Path:          tt.fields.AzureStoragePath,
				store:         tt.fields.store,
			}
			if f, err := os.Create(tt.args.src); err != nil {
				t.Fatalf("Create error = %v", err)
			} else {
				f.Close()
			}
			if err := az.Init(); (err != nil) != tt.wantErr {
				t.Fatalf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := az.Upload(tt.args.src, tt.args.dest); (err != nil) != tt.wantErr {
				t.Errorf("Upload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAzure_Download(t *testing.T) {
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
		src  string
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
				"downloadtest/src.json",
				"dest.json"},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			az := &Azure{
				AccountName:   tt.fields.AzureStorageAccount,
				AccountKey:    tt.fields.AzureStorageKey,
				ContainerName: tt.fields.AzureContainerName,
				Path:          tt.fields.AzureStoragePath,
				store:         tt.fields.store,
			}
			if err := az.Init(); (err != nil) != tt.wantErr {
				t.Fatalf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
			src, err := az.store.CreateFilePath(tt.args.src)
			if err != nil {
				t.Fatalf("CreateFilePath error = %v", err)
			}
			if err := az.store.Write(src, []byte(`[{"Key":"a2V5MQ==","Value":"dmFsdWUx"},{"Key":"a2V5Mg==","Value":"dmFsdWUy"},{"Key":"a2V5Mw==","Value":"dmFsdWUz"}]`)); err != nil {
				t.Fatalf("Write error = %v", err)
			}
			dest := &fs.LocalFilepath{}
			if err := dest.SetKey(tt.args.dest); err != nil {
				t.Fatalf("SetKey error = %v", err)
			}
			if err := az.Download(src, dest); (err != nil) != tt.wantErr {
				t.Errorf("Upload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
