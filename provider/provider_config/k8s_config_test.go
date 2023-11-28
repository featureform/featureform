package provider_config

import (
	"reflect"
	"testing"

	filestore "github.com/featureform/filestore"
	ss "github.com/featureform/helpers/string_set"
)

func TestK8sConfigMutableFields(t *testing.T) {
	expected := ss.StringSet{
		"ExecutorConfig":   true,
		"Store.AccountKey": true,
	}

	config := K8sConfig{
		ExecutorType: K8s,
		ExecutorConfig: ExecutorConfig{
			DockerImage: "container",
		},
		StoreType: filestore.Azure,
		StoreConfig: &AzureFileStoreConfig{
			AccountName:   "account name",
			AccountKey:    "account key",
			ContainerName: "container name",
			Path:          "container path",
		},
	}
	actual := config.MutableFields()

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v but received %v", expected, actual)
	}
}

func TestK8sConfigDifferingFields(t *testing.T) {
	type args struct {
		a K8sConfig
		b K8sConfig
	}

	tests := []struct {
		name     string
		args     args
		expected ss.StringSet
	}{
		{"No Differing Fields", args{
			a: K8sConfig{
				ExecutorType: "K8S",
				ExecutorConfig: ExecutorConfig{
					DockerImage: "container",
				},
				StoreType: filestore.Azure,
				StoreConfig: &AzureFileStoreConfig{
					AccountName:   "account name",
					AccountKey:    "account key",
					ContainerName: "container name",
					Path:          "container path",
				},
			},
			b: K8sConfig{
				ExecutorType: "K8S",
				ExecutorConfig: ExecutorConfig{
					DockerImage: "container",
				},
				StoreType: filestore.Azure,
				StoreConfig: &AzureFileStoreConfig{
					AccountName:   "account name",
					AccountKey:    "account key",
					ContainerName: "container name",
					Path:          "container path",
				},
			},
		}, ss.StringSet{}},
		{"Differing Fields", args{
			a: K8sConfig{
				ExecutorType: "K8S",
				ExecutorConfig: ExecutorConfig{
					DockerImage: "container",
				},
				StoreType: filestore.Azure,
				StoreConfig: &AzureFileStoreConfig{
					AccountName:   "account name",
					AccountKey:    "account key",
					ContainerName: "container name",
					Path:          "container path",
				},
			},
			b: K8sConfig{
				ExecutorType: "K8S",
				ExecutorConfig: ExecutorConfig{
					DockerImage: "container_v2",
				},
				StoreType: filestore.Azure,
				StoreConfig: &AzureFileStoreConfig{
					AccountName:   "account_name2",
					AccountKey:    "account_key2",
					ContainerName: "container name",
					Path:          "container path",
				},
			},
		}, ss.StringSet{
			"ExecutorConfig":    true,
			"Store.AccountName": true,
			"Store.AccountKey":  true,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := tt.args.a.DifferingFields(tt.args.b)

			if err != nil {
				t.Errorf("Failed to get differing fields due to error: %v", err)
			}

			if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("Expected %v, but instead found %v", tt.expected, actual)
			}

		})
	}
}
