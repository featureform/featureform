//go:build filepath
// +build filepath

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"fmt"
	"testing"

	pc "github.com/featureform/provider/provider_config"
)

func TestNewFilepath(t *testing.T) {
	type TestCase struct {
		StoreType       pc.FileStoreType
		Bucket          string
		Prefix          string
		Path            string
		ExpectedPath    Filepath
		ExpectedFailure bool
		ExpectedError   error
	}

	tests := map[string]TestCase{
		"Local": {
			StoreType:       FileSystem,
			Bucket:          "directory",
			Prefix:          "elasticmapreduce",
			Path:            "path/to/file",
			ExpectedPath:    nil,
			ExpectedFailure: true,
			ExpectedError:   fmt.Errorf("unknown store type '%s'", FileSystem),
		},
		"S3FilePath": {
			StoreType: S3,
			Bucket:    "bucket",
			Prefix:    "elasticmapreduce",
			Path:      "path/to/file",
			ExpectedPath: &S3Filepath{
				genericFilepath: genericFilepath{
					bucket: "bucket",
					prefix: "elasticmapreduce",
					path:   "path/to/file",
				},
			},
			ExpectedFailure: false,
			ExpectedError:   nil,
		},
		"S3FilePathTrailingPaths": {
			StoreType: S3,
			Bucket:    "bucket/",
			Prefix:    "/elasticmapreduce/",
			Path:      "/path/to/file/",
			ExpectedPath: &S3Filepath{
				genericFilepath: genericFilepath{
					bucket: "bucket",
					prefix: "elasticmapreduce",
					path:   "path/to/file/",
				},
			},
			ExpectedFailure: false,
			ExpectedError:   nil,
		},
		"GSFilePath": {
			StoreType:       GCS,
			Bucket:          "bucket",
			Prefix:          "elasticmapreduce",
			Path:            "path/to/file",
			ExpectedPath:    nil,
			ExpectedFailure: true,
			ExpectedError:   fmt.Errorf("unknown store type '%s'", GCS),
		},
		"AzureBlobStoreFilePath": {
			StoreType:       Azure,
			Bucket:          "bucket",
			Prefix:          "elasticmapreduce",
			Path:            "path/to/file",
			ExpectedPath:    nil,
			ExpectedFailure: true,
			ExpectedError:   fmt.Errorf("unknown store type '%s'", Azure),
		},
		"HDFSFilePath": {
			StoreType:       HDFS,
			Bucket:          "host",
			Prefix:          "elasticmapreduce",
			Path:            "path/to/file",
			ExpectedPath:    nil,
			ExpectedFailure: true,
			ExpectedError:   fmt.Errorf("unknown store type '%s'", HDFS),
		},
	}

	runTestCase := func(t *testing.T, test TestCase) {
		filepath, err := NewFilepath(test.StoreType, test.Bucket, test.Prefix, test.Path)
		if test.ExpectedFailure && err == nil {
			t.Fatalf("tested expected failure but passed")
		} else if !test.ExpectedFailure && err != nil {
			t.Fatalf("tested expected success but failed: %s", err)
		} else if test.ExpectedFailure && err != nil {
			if err.Error() != test.ExpectedError.Error() {
				t.Fatalf("expected error '%s' but got '%s'", test.ExpectedError, err)
			}
		} else if !test.ExpectedFailure && err == nil {
			if filepath.FullPathWithBucket() != test.ExpectedPath.FullPathWithBucket() {
				t.Fatalf("FullPathWithBucket failed; expected '%s' but got '%s'", test.ExpectedPath.FullPathWithBucket(), filepath.FullPathWithBucket())
			}
			if filepath.FullPathWithoutBucket() != test.ExpectedPath.FullPathWithoutBucket() {
				t.Fatalf("FullPathWithoutBucket failed; expected '%s' but got '%s'", test.ExpectedPath.FullPathWithoutBucket(), filepath.FullPathWithoutBucket())
			}
			if filepath.Bucket() != test.ExpectedPath.Bucket() {
				t.Fatalf("Bucket failed; expected '%s' but got '%s'", test.ExpectedPath.Bucket(), filepath.Bucket())
			}
			if filepath.Prefix() != test.ExpectedPath.Prefix() {
				t.Fatalf("Prefix failed; expected '%s' but got '%s'", test.ExpectedPath.Prefix(), filepath.Prefix())
			}
			if filepath.Path() != test.ExpectedPath.Path() {
				t.Fatalf("Path failed; expected '%s' but got '%s'", test.ExpectedPath.Path(), filepath.Path())
			}
		}

	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			runTestCase(t, test)
		})
	}
}

func TestParseFullPath(t *testing.T) {
	type TestCase struct {
		StoreType       pc.FileStoreType
		FullPath        string
		ExpectedPath    Filepath
		ExpectedFailure bool
		ExpectedError   error
	}

	tests := map[string]TestCase{
		"Local": {
			StoreType:       FileSystem,
			FullPath:        "directory/elasticmapreduce/path/to/file",
			ExpectedPath:    nil,
			ExpectedFailure: false,
			ExpectedError:   nil,
		},
		"S3FilePath": {
			StoreType: S3,
			FullPath:  "s3://bucket/elasticmapreduce/path/to/file",
			ExpectedPath: &S3Filepath{
				genericFilepath: genericFilepath{
					bucket: "bucket",
					prefix: "",
					path:   "elasticmapreduce/path/to/file",
				},
			},
			ExpectedFailure: false,
			ExpectedError:   nil,
		},
		"S3FilePathTrailingPaths": {
			StoreType: S3,
			FullPath:  "s3://bucket/elasticmapreduce/path/to/file/",
			ExpectedPath: &S3Filepath{
				genericFilepath: genericFilepath{
					bucket: "bucket",
					prefix: "",
					path:   "elasticmapreduce/path/to/file/",
				},
			},
			ExpectedFailure: false,
			ExpectedError:   nil,
		},
		"GSFilePath": {
			StoreType:       GCS,
			FullPath:        "gs://bucket/elasticmapreduce/path/to/file",
			ExpectedPath:    nil,
			ExpectedFailure: false,
			ExpectedError:   nil,
		},
		"AzureBlobStoreFilePath": {
			StoreType:       Azure,
			FullPath:        "abfss://container@account.dfs.core.windows.net/elasticmapreduce/path/to/file",
			ExpectedPath:    nil,
			ExpectedFailure: false,
			ExpectedError:   nil,
		},
		"HDFSFilePath": {
			StoreType:       HDFS,
			FullPath:        "hdfs://host/elasticmapreduce/path/to/file",
			ExpectedPath:    nil,
			ExpectedFailure: false,
			ExpectedError:   nil,
		},
	}

	runTestCase := func(t *testing.T, test TestCase) {
		var filePath Filepath
		var err error
		switch test.StoreType {
		case FileSystem:
			t.Skip()
		case S3:
			filePath = &S3Filepath{}
			err = filePath.ParseFullPath(test.FullPath)
		case GCS:
			t.Skip()
		case Azure:
			t.Skip()
		case HDFS:
			t.Skip()
		}

		if test.ExpectedFailure && err == nil {
			t.Fatalf("tested expected failure but passed")
		} else if !test.ExpectedFailure && err != nil {
			t.Fatalf("tested expected success but failed: %s", err)
		} else if test.ExpectedFailure && err != nil {
			if err.Error() != test.ExpectedError.Error() {
				t.Fatalf("expected error '%s' but got '%s'", test.ExpectedError, err)
			}
		} else if !test.ExpectedFailure && err == nil {
			if filePath.FullPathWithBucket() != test.ExpectedPath.FullPathWithBucket() {
				t.Fatalf("FullPathWithBucket failed; expected '%s' but got '%s'", test.ExpectedPath.FullPathWithBucket(), filePath.FullPathWithBucket())
			}
			if filePath.FullPathWithoutBucket() != test.ExpectedPath.FullPathWithoutBucket() {
				t.Fatalf("FullPathWithoutBucket failed; expected '%s' but got '%s'", test.ExpectedPath.FullPathWithoutBucket(), filePath.FullPathWithoutBucket())
			}
			if filePath.Bucket() != test.ExpectedPath.Bucket() {
				t.Fatalf("Bucket failed; expected '%s' but got '%s'", test.ExpectedPath.Bucket(), filePath.Bucket())
			}
			if filePath.Prefix() != test.ExpectedPath.Prefix() {
				t.Fatalf("Prefix failed; expected '%s' but got '%s'", test.ExpectedPath.Prefix(), filePath.Prefix())
			}
			if filePath.Path() != test.ExpectedPath.Path() {
				t.Fatalf("Path failed; expected '%s' but got '%s'", test.ExpectedPath.Path(), filePath.Path())
			}
		}
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			runTestCase(t, test)
		})
	}
}
