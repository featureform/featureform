// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package filestore

import (
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"

	pc "github.com/featureform/provider/provider_config"
)

type FileType string

const (
	Parquet FileType = "parquet"
	CSV     FileType = "csv"
	DB      FileType = "db"
)

func (ft FileType) Matches(file string) bool {
	ext := GetFileExtension(file)
	return FileType(ext) == ft
}

func GetFileType(file string) FileType {
	// check to see if its any of the constants
	for _, fileType := range []FileType{Parquet, CSV, DB} {
		if fileType.Matches(file) {
			print(fileType)
			return fileType
		}
	}
	// defaults to parquet
	return Parquet
}

func IsValidFileType(file string) bool {
	for _, fileType := range []FileType{Parquet, CSV, DB} {
		if fileType.Matches(file) {
			return true
		}
	}
	return false
}

func GetFileExtension(file string) string {
	ext := filepath.Ext(file)
	return strings.ReplaceAll(ext, ".", "")
}

type Filepath interface {
	// Returns the name of the bucket (S3) or container (Azure Blob Storage)
	Bucket() string
	KeyPrefix() string
	// Returns the absolute path without a scheme, host, or bucket
	Key() string
	IsDir() bool
	Ext() FileType
	// Returns the key to the object (S3) or blob (Azure Blob Storage)
	//	Path() string
	FullPathWithBucket() string
	FullPathWithoutBucket() string
	// Consumes a URI (e.g. abfss://<container>@<storage_account>/path/to/file) and parses it into
	// the specific parts that the implementation expects.
	ParseFullPath(path string) error
}

// TODO: Add support for additional params, such as service account (Azure Blob Storage)
func NewFilepath(storeType pc.FileStoreType, bucket string, prefix string, path string) (Filepath, error) {
	switch storeType {
	case S3:
		return &S3Filepath{
			filePath: filePath{
				bucket: strings.Trim(bucket, "/"),
				prefix: strings.Trim(prefix, "/"),
				path:   strings.TrimPrefix(path, "/"),
			},
		}, nil
	case Azure:
		return &AzureFilepath{
			filePath: filePath{
				bucket: strings.Trim(bucket, "/"),
				prefix: strings.Trim(prefix, "/"),
				path:   strings.Trim(path, "/"),
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown store type '%s'", storeType)
	}
}

func NewEmptyFilepath(storeType pc.FileStoreType) (Filepath, error) {
	switch storeType {
	case S3:
		return &S3Filepath{}, nil
	case Azure:
		return &AzureFilepath{}, nil
	case GCS:
		return &GCSFilepath{}, nil
	case Memory:
		return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	case FileSystem:
		return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	case pc.DB:
		return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	case HDFS:
		return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	default:
		return nil, fmt.Errorf("unknown store type '%s'", storeType)
	}
}

type filePath struct {
	bucket string
	prefix string
	path   string
}

func (fp *filePath) Bucket() string {
	return fp.bucket
}

func (fp *filePath) Prefix() string {
	return fp.prefix
}

func (fp *filePath) Path() string {
	return fp.path
}

func (fp *filePath) FullPathWithBucket() string {
	prefix := ""
	if fp.prefix != "" {
		prefix = fmt.Sprintf("/%s", fp.prefix)
	}

	return fmt.Sprintf("%s%s/%s", fp.bucket, prefix, fp.path)
}

func (fp *filePath) FullPathWithoutBucket() string {
	prefix := ""
	if fp.prefix != "" {
		prefix = fmt.Sprintf("%s/", fp.prefix)
	}
	return fmt.Sprintf("%s%s", prefix, fp.path)
}

func (fp *filePath) ParseFullPath(fullPath string) error {
	// Parse the URI into a url.URL object.
	u, err := url.Parse(fullPath)
	if err != nil {
		return fmt.Errorf("could not parse fullpath '%s': %v", fullPath, err)
	}

	// Extract the bucket and path components from the URI.
	bucket := u.Host
	path := strings.TrimPrefix(u.Path, "/")

	fp.bucket = bucket
	fp.path = path
	return nil
}

type S3Filepath struct {
	filePath
}

func (s3 *S3Filepath) FullPathWithBucket() string {
	prefix := ""
	if s3.prefix != "" {
		prefix = fmt.Sprintf("/%s", s3.prefix)
	}

	return fmt.Sprintf("s3://%s%s/%s", s3.bucket, prefix, s3.path)
}

type AzureFilepath struct {
	storageAccount string
	filePath
}

func (azure *AzureFilepath) FullPathWithBucket() string {
	return fmt.Sprintf("abfss://%s@%s.dfs.core.windows.net/%s", azure.filePath.bucket, azure.storageAccount, azure.filePath.path)
}

func (azure *AzureFilepath) ParseFullPath(fullPath string) error {
	abfssRegex := regexp.MustCompile(`abfss://(.+?)@(.+?)\.dfs.core.windows.net/(.+)`)
	if matches := abfssRegex.FindStringSubmatch(fullPath); len(matches) != 4 {
		return fmt.Errorf("could not parse full path '%s'; expected format abfss://<container/bucket>@<storage_account>.dfs.core.windows.net/path", fullPath)
	} else {
		azure.filePath.bucket = strings.Trim(matches[1], "/")
		azure.storageAccount = strings.Trim(matches[2], "/")
		azure.filePath.path = strings.Trim(matches[3], "/")
	}
	return nil
}

type GCSFilepath struct {
	filePath
}

func (gcs *GCSFilepath) FullPathWithBucket() string {
	prefix := ""
	if gcs.prefix != "" {
		prefix = fmt.Sprintf("/%s", gcs.prefix)
	}

	return fmt.Sprintf("gs://%s%s/%s", gcs.bucket, prefix, gcs.path)
}
