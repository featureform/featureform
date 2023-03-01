package provider_config

import (
	"reflect"
	"testing"

	ss "github.com/featureform/helpers/string_set"
)

func TestSparkConfigMutableFields(t *testing.T) {
	tests := []struct {
		name     string
		arg      SparkConfig
		expected ss.StringSet
	}{
		{
			name: "EMR + S3 Mutable Fields",
			arg: SparkConfig{
				ExecutorType: EMR,
				ExecutorConfig: &EMRConfig{
					Credentials:   AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
					ClusterRegion: "us-east-1",
					ClusterName:   "featureform-clst",
				},
				StoreType: S3,
				StoreConfig: &S3FileStoreConfig{
					Credentials:  AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
					BucketRegion: "us-east-1",
					BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
					Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				},
			},
			expected: ss.StringSet{
				"Executor.Credentials": true,
				"Store.Credentials":    true,
			},
		},
		{
			name: "Databricks + Azure Mutable Fields",
			arg: SparkConfig{
				ExecutorType: Databricks,
				ExecutorConfig: &DatabricksConfig{
					Host:     "https://featureform.cloud.databricks.com",
					Username: "featureformer",
					Password: "password",
					Cluster:  "1115-164516-often242",
					Token:    "dapi1234567890ab1cde2f3ab456c7d89efa",
				},
				StoreType: Azure,
				StoreConfig: &AzureFileStoreConfig{
					AccountName:   "featureform-str",
					AccountKey:    "secret-account-key",
					ContainerName: "transactions_container",
					Path:          "custom/path/in/container",
				},
			},
			expected: ss.StringSet{
				"Executor.Username": true,
				"Executor.Password": true,
				"Executor.Token":    true,
				"Store.AccountName": true,
				"Store.AccountKey":  true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.arg.MutableFields()
			if !reflect.DeepEqual(tt.expected, actual) {
				t.Errorf("Expected %v but received %v", tt.expected, actual)
			}
		})
	}
}

func TestSparkConfigDifferingFields(t *testing.T) {
	type args struct {
		a, b SparkConfig
	}

	tests := []struct {
		name        string
		args        args
		expected    ss.StringSet
		expectedErr bool
	}{
		{"EMR + S3 No Differing Fields", args{
			a: SparkConfig{
				ExecutorType: EMR,
				ExecutorConfig: &EMRConfig{
					Credentials:   AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
					ClusterRegion: "us-east-1",
					ClusterName:   "featureform-clst",
				},
				StoreType: S3,
				StoreConfig: &S3FileStoreConfig{
					Credentials:  AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
					BucketRegion: "us-east-1",
					BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
					Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				},
			},
			b: SparkConfig{
				ExecutorType: EMR,
				ExecutorConfig: &EMRConfig{
					Credentials:   AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
					ClusterRegion: "us-east-1",
					ClusterName:   "featureform-clst",
				},
				StoreType: S3,
				StoreConfig: &S3FileStoreConfig{
					Credentials:  AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
					BucketRegion: "us-east-1",
					BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
					Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				},
			},
		}, ss.StringSet{}, false},
		{"EMR + S3 Differing Fields", args{
			a: SparkConfig{
				ExecutorType: EMR,
				ExecutorConfig: &EMRConfig{
					Credentials:   AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
					ClusterRegion: "us-east-1",
					ClusterName:   "featureform-clst",
				},
				StoreType: S3,
				StoreConfig: &S3FileStoreConfig{
					Credentials:  AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
					BucketRegion: "us-east-1",
					BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
					Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				},
			},
			b: SparkConfig{
				ExecutorType: EMR,
				ExecutorConfig: &EMRConfig{
					Credentials:   AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
					ClusterRegion: "us-west-2",
					ClusterName:   "featureform-clst",
				},
				StoreType: S3,
				StoreConfig: &S3FileStoreConfig{
					Credentials:  AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
					BucketRegion: "us-west-2",
					BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
					Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				},
			},
		}, ss.StringSet{
			"Executor.ClusterRegion": true,
			"Store.BucketRegion":     true,
		}, false},
		{
			"Databricks + Azure No Differing Fields",
			args{
				a: SparkConfig{
					ExecutorType: Databricks,
					ExecutorConfig: &DatabricksConfig{
						Host:     "https://featureform.cloud.databricks.com",
						Username: "featureformer",
						Password: "password",
						Cluster:  "1115-164516-often242",
						Token:    "dapi1234567890ab1cde2f3ab456c7d89efa",
					},
					StoreType: Azure,
					StoreConfig: &AzureFileStoreConfig{
						AccountName:   "featureform-str",
						AccountKey:    "secret-account-key",
						ContainerName: "transactions_container",
						Path:          "custom/path/in/container",
					},
				},
				b: SparkConfig{
					ExecutorType: Databricks,
					ExecutorConfig: &DatabricksConfig{
						Host:     "https://featureform.cloud.databricks.com",
						Username: "featureformer",
						Password: "password",
						Cluster:  "1115-164516-often242",
						Token:    "dapi1234567890ab1cde2f3ab456c7d89efa",
					},
					StoreType: Azure,
					StoreConfig: &AzureFileStoreConfig{
						AccountName:   "featureform-str",
						AccountKey:    "secret-account-key",
						ContainerName: "transactions_container",
						Path:          "custom/path/in/container",
					},
				},
			}, ss.StringSet{}, false,
		},
		{
			"Databricks + Azure Differing Fields",
			args{
				a: SparkConfig{
					ExecutorType: Databricks,
					ExecutorConfig: &DatabricksConfig{
						Host:     "https://featureform.cloud.databricks.com",
						Username: "featureformer",
						Password: "password",
						Cluster:  "1115-164516-often242",
						Token:    "dapi1234567890ab1cde2f3ab456c7d89efa",
					},
					StoreType: Azure,
					StoreConfig: &AzureFileStoreConfig{
						AccountName:   "featureform-str",
						AccountKey:    "secret-account-key",
						ContainerName: "transactions_container",
						Path:          "custom/path/in/container",
					},
				},
				b: SparkConfig{
					ExecutorType: Databricks,
					ExecutorConfig: &DatabricksConfig{
						Host:     "https://featureform.cloud.databricks.com",
						Username: "featureformer2",
						Password: "password2",
						Cluster:  "1115-164516-often242",
						Token:    "dapi1234567890ab1cde2f3ab456c7d89efa",
					},
					StoreType: Azure,
					StoreConfig: &AzureFileStoreConfig{
						AccountName:   "featureform-store",
						AccountKey:    "secret-account-key2",
						ContainerName: "transactions_container",
						Path:          "custom/path/in/container",
					},
				},
			}, ss.StringSet{
				"Executor.Username": true,
				"Executor.Password": true,
				"Store.AccountName": true,
				"Store.AccountKey":  true,
			}, false,
		},
		{"Executor Config Mismatch: EMR -> Databricks", args{
			a: SparkConfig{
				ExecutorType: EMR,
				ExecutorConfig: &EMRConfig{
					Credentials:   AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
					ClusterRegion: "us-east-1",
					ClusterName:   "featureform-clst",
				},
				StoreType: S3,
				StoreConfig: &S3FileStoreConfig{
					Credentials:  AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
					BucketRegion: "us-east-1",
					BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
					Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				},
			},
			b: SparkConfig{
				ExecutorType: Databricks,
				ExecutorConfig: &DatabricksConfig{
					Host:     "https://featureform.cloud.databricks.com",
					Username: "featureformer",
					Password: "password",
					Cluster:  "1115-164516-often242",
					Token:    "dapi1234567890ab1cde2f3ab456c7d89efa",
				},
				StoreType: S3,
				StoreConfig: &S3FileStoreConfig{
					Credentials:  AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
					BucketRegion: "us-west-2",
					BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
					Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				},
			},
		}, ss.StringSet{}, true},
		{"Store Config Mismatch: S3 -> Azure", args{
			a: SparkConfig{
				ExecutorType: EMR,
				ExecutorConfig: &EMRConfig{
					Credentials:   AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
					ClusterRegion: "us-east-1",
					ClusterName:   "featureform-clst",
				},
				StoreType: S3,
				StoreConfig: &S3FileStoreConfig{
					Credentials:  AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
					BucketRegion: "us-east-1",
					BucketPath:   "https://featureform.s3.us-east-1.amazonaws.com/transactions",
					Path:         "https://featureform.s3.us-east-1.amazonaws.com/transactions",
				},
			},
			b: SparkConfig{
				ExecutorType: EMR,
				ExecutorConfig: &EMRConfig{
					Credentials:   AWSCredentials{AWSAccessKeyId: "aws-key", AWSSecretKey: "aws-secret"},
					ClusterRegion: "us-west-2",
					ClusterName:   "featureform-clst",
				},
				StoreType: Azure,
				StoreConfig: &AzureFileStoreConfig{
					AccountName:   "featureform-str",
					AccountKey:    "secret-account-key",
					ContainerName: "transactions_container",
					Path:          "custom/path/in/container",
				},
			},
		}, ss.StringSet{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := tt.args.a.DifferingFields(tt.args.b)

			if (err != nil) != tt.expectedErr {
				t.Errorf("Encountered unexpected error %v", err)
			}

			if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("Expected %v, but instead found %v", tt.expected, actual)
			}

		})
	}

}
