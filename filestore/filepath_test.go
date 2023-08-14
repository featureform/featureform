//go:build filepath
// +build filepath

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package filestore

import (
	"testing"
)

//func TestNewFilepath(t *testing.T) {
//	type TestCase struct {
//		StoreType       pc.FileStoreType
//		Bucket          string
//		Prefix          string
//		Path            string
//		ExpectedPath    Filepath
//		ExpectedFailure bool
//		ExpectedError   error
//	}
//
//	tests := map[string]TestCase{
//		"Local": {
//			StoreType:       FileSystem,
//			Bucket:          "directory",
//			Prefix:          "elasticmapreduce",
//			Path:            "path/to/file",
//			ExpectedPath:    nil,
//			ExpectedFailure: true,
//			ExpectedError:   fmt.Errorf("unknown store type '%s'", FileSystem),
//		},
//		"S3FilePath": {
//			StoreType: S3,
//			Bucket:    "bucket",
//			Prefix:    "elasticmapreduce",
//			Path:      "path/to/file",
//			ExpectedPath: &S3Filepath{
//				FilePath: FilePath{
//					bucket: "bucket",
//					prefix: "elasticmapreduce",
//					path:   "path/to/file",
//				},
//			},
//			ExpectedFailure: false,
//			ExpectedError:   nil,
//		},
//		"S3FilePathTrailingPaths": {
//			StoreType: S3,
//			Bucket:    "bucket/",
//			Prefix:    "/elasticmapreduce/",
//			Path:      "/path/to/file/",
//			ExpectedPath: &S3Filepath{
//				FilePath: FilePath{
//					bucket: "bucket",
//					prefix: "elasticmapreduce",
//					path:   "path/to/file/",
//				},
//			},
//			ExpectedFailure: false,
//			ExpectedError:   nil,
//		},
//		"GSFilePath": {
//			StoreType:       GCS,
//			Bucket:          "bucket",
//			Prefix:          "elasticmapreduce",
//			Path:            "path/to/file",
//			ExpectedPath:    nil,
//			ExpectedFailure: true,
//			ExpectedError:   fmt.Errorf("unknown store type '%s'", GCS),
//		},
//		"AzureBlobStoreFilePath": {
//			StoreType: Azure,
//			Bucket:    "bucket",
//			Prefix:    "elasticmapreduce",
//			Path:      "path/to/file",
//			ExpectedPath: &AzureFilepath{
//				FilePath: FilePath{
//					bucket: "bucket",
//					prefix: "elasticmapreduce",
//					path:   "path/to/file",
//				},
//			},
//			ExpectedFailure: false,
//			ExpectedError:   nil,
//		},
//		"HDFSFilePath": {
//			StoreType:       HDFS,
//			Bucket:          "host",
//			Prefix:          "elasticmapreduce",
//			Path:            "path/to/file",
//			ExpectedPath:    nil,
//			ExpectedFailure: true,
//			ExpectedError:   fmt.Errorf("unknown store type '%s'", HDFS),
//		},
//	}
//
//	runTestCase := func(t *testing.T, test TestCase) {
//		filepath, err := NewFilepath(test.StoreType, test.Bucket, test.Prefix, test.Path)
//		if test.ExpectedFailure && err == nil {
//			t.Fatalf("tested expected failure but passed")
//		} else if !test.ExpectedFailure && err != nil {
//			t.Fatalf("tested expected success but failed: %s", err)
//		} else if test.ExpectedFailure && err != nil {
//			if err.Error() != test.ExpectedError.Error() {
//				t.Fatalf("expected error '%s' but got '%s'", test.ExpectedError, err)
//			}
//		} else if !test.ExpectedFailure && err == nil {
//			if filepath.FullPathWithBucket() != test.ExpectedPath.FullPathWithBucket() {
//				t.Fatalf("FullPathWithBucket failed; expected '%s' but got '%s'", test.ExpectedPath.FullPathWithBucket(), filepath.FullPathWithBucket())
//			}
//			if filepath.FullPathWithoutBucket() != test.ExpectedPath.FullPathWithoutBucket() {
//				t.Fatalf("FullPathWithoutBucket failed; expected '%s' but got '%s'", test.ExpectedPath.FullPathWithoutBucket(), filepath.FullPathWithoutBucket())
//			}
//			if filepath.Bucket() != test.ExpectedPath.Bucket() {
//				t.Fatalf("Bucket failed; expected '%s' but got '%s'", test.ExpectedPath.Bucket(), filepath.Bucket())
//			}
//			if filepath.Prefix() != test.ExpectedPath.Prefix() {
//				t.Fatalf("Prefix failed; expected '%s' but got '%s'", test.ExpectedPath.Prefix(), filepath.Prefix())
//			}
//			if filepath.Path() != test.ExpectedPath.Path() {
//				t.Fatalf("Path failed; expected '%s' but got '%s'", test.ExpectedPath.Path(), filepath.Path())
//			}
//		}
//
//	}
//
//	for name, test := range tests {
//		t.Run(name, func(t *testing.T) {
//			runTestCase(t, test)
//		})
//	}
//}
//
//func TestParseFullPath(t *testing.T) {
//	type TestCase struct {
//		StoreType       pc.FileStoreType
//		FullPath        string
//		ExpectedPath    Filepath
//		ExpectedFailure bool
//		ExpectedError   error
//	}
//
//	tests := map[string]TestCase{
//		"Local": {
//			StoreType:       FileSystem,
//			FullPath:        "directory/elasticmapreduce/path/to/file",
//			ExpectedPath:    nil,
//			ExpectedFailure: false,
//			ExpectedError:   nil,
//		},
//		"S3FilePath": {
//			StoreType: S3,
//			FullPath:  "s3://bucket/elasticmapreduce/path/to/file",
//			ExpectedPath: &S3Filepath{
//				FilePath: FilePath{
//					bucket: "bucket",
//					prefix: "",
//					path:   "elasticmapreduce/path/to/file",
//				},
//			},
//			ExpectedFailure: false,
//			ExpectedError:   nil,
//		},
//		"S3FilePathTrailingPaths": {
//			StoreType: S3,
//			FullPath:  "s3://bucket/elasticmapreduce/path/to/file/",
//			ExpectedPath: &S3Filepath{
//				FilePath: FilePath{
//					bucket: "bucket",
//					prefix: "",
//					path:   "elasticmapreduce/path/to/file/",
//				},
//			},
//			ExpectedFailure: false,
//			ExpectedError:   nil,
//		},
//		"GSFilePath": {
//			StoreType: GCS,
//			FullPath:  "gs://bucket/elasticmapreduce/path/to/file",
//			ExpectedPath: &GCSFilepath{
//				FilePath: FilePath{
//					bucket: "bucket",
//					prefix: "",
//					path:   "elasticmapreduce/path/to/file",
//				},
//			},
//			ExpectedFailure: false,
//			ExpectedError:   nil,
//		},
//		"AzureBlobStoreFilePath": {
//			StoreType: Azure,
//			FullPath:  "abfss://container@account.dfs.core.windows.net/elasticmapreduce/path/to/file",
//			ExpectedPath: &AzureFilepath{
//				StorageAccount: "account",
//				FilePath: FilePath{
//					bucket: "container",
//					prefix: "",
//					path:   "elasticmapreduce/path/to/file",
//				},
//			},
//			ExpectedFailure: false,
//			ExpectedError:   nil,
//		},
//		"AzureBlobStoreFilePathWithTrailingSlash": {
//			StoreType: Azure,
//			FullPath:  "abfss://container@account.dfs.core.windows.net/elasticmapreduce/path/to/file/",
//			ExpectedPath: &AzureFilepath{
//				StorageAccount: "account",
//				FilePath: FilePath{
//					bucket: "container",
//					prefix: "",
//					path:   "elasticmapreduce/path/to/file",
//				},
//			},
//			ExpectedFailure: false,
//			ExpectedError:   nil,
//		},
//		"HDFSFilePath": {
//			StoreType:       HDFS,
//			FullPath:        "hdfs://host/elasticmapreduce/path/to/file",
//			ExpectedPath:    nil,
//			ExpectedFailure: false,
//			ExpectedError:   nil,
//		},
//	}
//
//	runTestCase := func(t *testing.T, test TestCase) {
//		var FilePath Filepath
//		var err error
//		switch test.StoreType {
//		case FileSystem:
//			t.Skip()
//		case S3:
//			FilePath = &S3Filepath{}
//			err = FilePath.ParseFullPath(test.FullPath)
//		case GCS:
//			t.Skip()
//		case Azure:
//			FilePath = &AzureFilepath{}
//			err = FilePath.ParseFullPath(test.FullPath)
//		case HDFS:
//			t.Skip()
//		}
//
//		if test.ExpectedFailure && err == nil {
//			t.Fatalf("tested expected failure but passed")
//		} else if !test.ExpectedFailure && err != nil {
//			t.Fatalf("tested expected success but failed: %s", err)
//		} else if test.ExpectedFailure && err != nil {
//			if err.Error() != test.ExpectedError.Error() {
//				t.Fatalf("expected error '%s' but got '%s'", test.ExpectedError, err)
//			}
//		} else if !test.ExpectedFailure && err == nil {
//			if FilePath.FullPathWithBucket() != test.ExpectedPath.FullPathWithBucket() {
//				t.Fatalf("FullPathWithBucket failed; expected '%s' but got '%s'", test.ExpectedPath.FullPathWithBucket(), FilePath.FullPathWithBucket())
//			}
//			if FilePath.FullPathWithoutBucket() != test.ExpectedPath.FullPathWithoutBucket() {
//				t.Fatalf("FullPathWithoutBucket failed; expected '%s' but got '%s'", test.ExpectedPath.FullPathWithoutBucket(), FilePath.FullPathWithoutBucket())
//			}
//			if FilePath.Bucket() != test.ExpectedPath.Bucket() {
//				t.Fatalf("Bucket failed; expected '%s' but got '%s'", test.ExpectedPath.Bucket(), FilePath.Bucket())
//			}
//			if FilePath.Prefix() != test.ExpectedPath.Prefix() {
//				t.Fatalf("Prefix failed; expected '%s' but got '%s'", test.ExpectedPath.Prefix(), FilePath.Prefix())
//			}
//			if FilePath.Path() != test.ExpectedPath.Path() {
//				t.Fatalf("Path failed; expected '%s' but got '%s'", test.ExpectedPath.Path(), FilePath.Path())
//			}
//		}
//	}
//
//	for name, test := range tests {
//		t.Run(name, func(t *testing.T) {
//			runTestCase(t, test)
//		})
//	}
//}
//
//func TestNewFilepathAndParseFullPath(t *testing.T) {
//	type TestCase struct {
//		StoreType       pc.FileStoreType
//		FullPath        string
//		ExpectedPath    Filepath
//		ExpectedFailure bool
//		ExpectedError   error
//	}
//
//	tests := map[string]TestCase{
//		"AzureBlobStoreFilePathWithTrailingSlash": {
//			StoreType: Azure,
//			FullPath:  "abfss://container@account.dfs.core.windows.net/elasticmapreduce/path/to/file/",
//			ExpectedPath: &AzureFilepath{
//				StorageAccount: "account",
//				FilePath: FilePath{
//					bucket: "container",
//					prefix: "",
//					path:   "elasticmapreduce/path/to/file/",
//				},
//			},
//			ExpectedFailure: false,
//			ExpectedError:   nil,
//		},
//	}
//
//	runTestCase := func(t *testing.T, test TestCase) {
//		a, err := NewFilepath(test.StoreType, test.ExpectedPath.Bucket(), test.ExpectedPath.Prefix(), test.ExpectedPath.Path())
//		if err != nil {
//			t.Fatalf("NewFilepath failed: %s", err)
//		}
//		b := &AzureFilepath{}
//		err = b.ParseFullPath(test.FullPath)
//		if err != nil {
//			t.Fatalf("ParseFullPath failed: %s", err)
//		}
//		if a.FullPathWithoutBucket() != b.FullPathWithoutBucket() {
//			t.Fatalf("FullPathWithoutBucket failed; expected '%s' and '%s' to be equal", a.FullPathWithoutBucket(), b.FullPathWithoutBucket())
//		}
//		if a.Bucket() != b.Bucket() {
//			t.Fatalf("Bucket failed; expected '%s' and '%s' to be equal", a.Bucket(), b.Bucket())
//		}
//		if a.Path() != b.Path() {
//			t.Fatalf("Path failed; expected '%s' and '%s' to be equal", a.Path(), b.Path())
//		}
//	}
//
//	for name, test := range tests {
//		t.Run(name, func(t *testing.T) {
//			runTestCase(t, test)
//		})
//	}
//}

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
			if tt.ExpectedFullPath != tt.filePath.PathWithBucket() {
				t.Errorf("expected full path %s but got %s", tt.ExpectedFullPath, tt.filePath.PathWithBucket())
			}
		})
	}
}
