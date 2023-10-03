// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package filestore

import (
	"testing"
)

func TestValidateFilepaths(t *testing.T) {
	testCases := []struct {
		name        string
		storeType   FileStoreType
		filePath    Filepath
		expectError bool
	}{
		{
			"S3 Valid",
			S3,
			&S3Filepath{
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "s3://",
					key:    "my/path/",
				},
			}, false,
		},
		{
			"S3a Valid",
			S3,
			&S3Filepath{
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "s3a://",
					key:    "my/path/",
				},
			}, false,
		},
		{
			"S3 Invalid Bucket",
			S3,
			&S3Filepath{
				FilePath: FilePath{
					bucket: "",
					scheme: "s3://",
					key:    "my/path/",
				},
			}, true,
		},
		{
			"S3 Invalid Scheme",
			S3,
			&S3Filepath{
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "",
					key:    "my/path/",
				},
			}, true,
		},
		{
			"S3 Invalid Key",
			S3,
			&S3Filepath{
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "s3://",
					key:    "",
				},
			}, true,
		},
		{
			"Azure Valid",
			Azure,
			&AzureFilepath{
				StorageAccount: "account",
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "abfss://",
					key:    "my/path/",
				},
			}, false,
		},
		{
			"Azure Invalid Storage Account",
			Azure,
			&AzureFilepath{
				StorageAccount: "",
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "abfss://",
					key:    "my/path/",
				},
			}, true,
		},
		{
			"Azure Invalid Bucket",
			Azure,
			&AzureFilepath{
				StorageAccount: "storageaccount",
				FilePath: FilePath{
					bucket: "",
					scheme: "abfss://",
					key:    "my/path/",
				},
			}, true,
		},
		{
			"Azure Invalid Scheme",
			Azure,
			&AzureFilepath{
				StorageAccount: "storageaccount",
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "",
					key:    "my/path/",
				},
			}, true,
		},
		{
			"Azure Invalid Key",
			Azure,
			&AzureFilepath{
				StorageAccount: "storageaccount",
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "abfss://",
					key:    "",
				},
			}, true,
		},
		{
			"GCS Valid",
			GCS,
			&GCSFilepath{
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "gs://",
					key:    "my/path/",
				},
			}, false,
		},
		{
			"GCS Invalid Bucket",
			GCS,
			&GCSFilepath{
				FilePath: FilePath{
					bucket: "",
					scheme: "gs://",
					key:    "my/path/",
				},
			}, true,
		},
		{
			"GCS Invalid Scheme",
			GCS,
			&GCSFilepath{
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "",
					key:    "my/path/",
				},
			}, true,
		},
		{
			"GCS Invalid Key",
			GCS,
			&S3Filepath{
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "gs://",
					key:    "",
				},
			}, true,
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.filePath.Validate()
			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			} else if !tt.expectError && err != nil {
				t.Errorf("expected no error but got %v", err)
			}
		})
	}
}

func TestNewEmptyPath(t *testing.T) {
	testCases := []struct {
		name              string
		storeType         FileStoreType
		filePath          Filepath
		ExpectedScheme    string
		ExpectedBucket    string
		ExpectedKey       string
		ExpectedKeyPrefix string
		ExpectedIsDir     bool
		ExpectedFullPath  string
		ExpectError       bool
	}{
		{
			"S3",
			S3,
			&S3Filepath{
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "s3://",
					key:    "my/path/something",
					isDir:  false,
				},
			},
			"s3://",
			"bucket",
			"my/path/something",
			"my/path",
			false,
			"s3://bucket/my/path/something",
			false,
		},
		{
			"S3 Extra Slashes",
			S3,
			&S3Filepath{
				FilePath: FilePath{
					bucket: "/bucket/",
					scheme: "s3://",
					key:    "/my/path/something/",
					isDir:  false,
				},
			},
			"s3://",
			"bucket",
			"my/path/something",
			"my/path",
			false,
			"s3://bucket/my/path/something",
			false,
		},
		{
			"S3 Directory",
			S3,
			&S3Filepath{
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "s3://",
					key:    "my/path/something",
					isDir:  true,
				},
			},
			"s3://",
			"bucket",
			"my/path/something",
			"my/path",
			true,
			"s3://bucket/my/path/something",
			false,
		},
		{
			"Azure",
			Azure,
			&AzureFilepath{
				StorageAccount: "storageaccount",
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "abfss://",
					key:    "my/path/something",
					isDir:  false,
				},
			},
			"abfss://",
			"bucket",
			"my/path/something",
			"my/path",
			false,
			"abfss://bucket@storageaccount.dfs.core.windows.net/my/path/something",
			false,
		},
		{
			"Azure Extra Slashes",
			Azure,
			&AzureFilepath{
				StorageAccount: "storageaccount",
				FilePath: FilePath{
					bucket: "/bucket/",
					scheme: "abfss://",
					key:    "/my/path/something/",
					isDir:  false,
				},
			},
			"abfss://",
			"bucket",
			"my/path/something",
			"my/path",
			false,
			"abfss://bucket@storageaccount.dfs.core.windows.net/my/path/something",
			false,
		},
		{
			"Azure Directory",
			Azure,
			&AzureFilepath{
				StorageAccount: "storageaccount",
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "abfss://",
					key:    "my/path/something",
					isDir:  true,
				},
			},
			"abfss://",
			"bucket",
			"my/path/something",
			"my/path",
			true,
			"abfss://bucket@storageaccount.dfs.core.windows.net/my/path/something",
			false,
		},
		{
			"GCS",
			GCS,
			&GCSFilepath{
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "gs://",
					key:    "my/path/something",
					isDir:  false,
				},
			},
			"gs://",
			"bucket",
			"my/path/something",
			"my/path",
			false,
			"gs://bucket/my/path/something",
			false,
		},
		{
			"GCS Extra Slashes",
			S3,
			&GCSFilepath{
				FilePath: FilePath{
					bucket: "/bucket/",
					scheme: "gs://",
					key:    "/my/path/something/",
					isDir:  false,
				},
			},
			"gs://",
			"bucket",
			"my/path/something",
			"my/path",
			false,
			"gs://bucket/my/path/something",
			false,
		},
		{
			"GCS Directory",
			GCS,
			&GCSFilepath{
				FilePath: FilePath{
					bucket: "bucket",
					scheme: "gs://",
					key:    "my/path/something",
					isDir:  true,
				},
			},
			"gs://",
			"bucket",
			"my/path/something",
			"my/path",
			true,
			"gs://bucket/my/path/something",
			false,
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.filePath.Validate()
			if tt.ExpectError && err == nil {
				t.Errorf("expected error but got none")
			} else if !tt.ExpectError && err != nil {
				t.Errorf("expected no error but got %v", err)
			}
			if tt.ExpectedScheme != tt.filePath.Scheme() {
				t.Errorf("expected scheme %s but got %s", tt.ExpectedScheme, tt.filePath.Scheme())
			}
			if tt.ExpectedBucket != tt.filePath.Bucket() {
				t.Errorf("expected bucket %s but got %s", tt.ExpectedBucket, tt.filePath.Bucket())
			}
			if tt.ExpectedKey != tt.filePath.Key() {
				t.Errorf("expected key %s but got %s", tt.ExpectedKey, tt.filePath.Key())
			}
			if tt.ExpectedKeyPrefix != tt.filePath.KeyPrefix() {
				t.Errorf("expected key prefix %s but got %s", tt.ExpectedKeyPrefix, tt.filePath.KeyPrefix())
			}
			if tt.ExpectedIsDir != tt.filePath.IsDir() {
				t.Errorf("expected is dir %t but got %t", tt.ExpectedIsDir, tt.filePath.IsDir())
			}
			if tt.ExpectedFullPath != tt.filePath.ToURI() {
				t.Errorf("expected full path %s but got %s", tt.ExpectedFullPath, tt.filePath.ToURI())
			}
		})
	}
}
