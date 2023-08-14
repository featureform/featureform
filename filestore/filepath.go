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
)

type FileType string

type FileStoreType string

const (
	Memory     FileStoreType = "MEMORY"
	FileSystem FileStoreType = "LOCAL_FILESYSTEM"
	Azure      FileStoreType = "AZURE"
	S3         FileStoreType = "S3"
	GCS        FileStoreType = "GCS"
	HDFS       FileStoreType = "HDFS"
)

const (
	Parquet FileType = "parquet"
	CSV     FileType = "csv"
	DB      FileType = "db"
)

const (
	gsPrefix        = "gs://"
	s3Prefix        = "s3://"
	s3aPrefix       = "s3a://"
	azureBlobPrefix = "abfss://"
	HDFSPrefix      = "hdfs://"
)

var ValidSchemes = []string{
	gsPrefix, s3Prefix, s3aPrefix, azureBlobPrefix, HDFSPrefix,
}

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
	Scheme() string
	// Returns the name of the bucket (S3) or container (Azure Blob Storage)
	Bucket() string
	// Returns the absolute path without a scheme, host, or bucket
	Key() string
	KeyPrefix() string
	IsDir() bool
	Ext() FileType
	// Returns the key to the object (S3) or blob (Azure Blob Storage)
	//	Path() string
	PathWithBucket() string
	// Consumes a URI (e.g. abfss://<container>@<storage_account>/path/to/file) and parses it into
	// the specific parts that the implementation expects.
	ParseFilePath(path string) error
	ParseDirPath(path string) error
	Validate() error
	IsValid() bool
}

//// TODO: Add support for additional params, such as service account (Azure Blob Storage)
//func NewFilepath(storeType pc.FileStoreType, bucket string, path string) (Filepath, error) {
//	switch storeType {
//	case pc.S3:
//		return &S3Filepath{
//			filePath: filePath{
//				bucket: strings.Trim(bucket, "/"),
//				prefix: strings.Trim(prefix, "/"),
//				path:   strings.TrimPrefix(path, "/"),
//			},
//		}, nil
//	case pc.Azure:
//		return &AzureFilepath{
//			filePath: filePath{
//				bucket: strings.Trim(bucket, "/"),
//				prefix: strings.Trim(prefix, "/"),
//				path:   strings.Trim(path, "/"),
//			},
//		}, nil
//	default:
//		return nil, fmt.Errorf("unknown store type '%s'", storeType)
//	}
//}

func NewEmptyFilepath(storeType FileStoreType) (Filepath, error) {
	switch storeType {
	case S3:
		return &S3Filepath{filePath{isDir: false}}, nil
	case Azure:
		return &AzureFilepath{}, nil
	case GCS:
		return &GCSFilepath{filePath{isDir: false}}, nil
	case Memory:
		return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	case FileSystem:
		return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	//case DB:
	//	return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	case HDFS:
		return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	default:
		return nil, fmt.Errorf("unknown store type '%s'", storeType)
	}
}

func NewEmptyDirpath(storeType FileStoreType) (Filepath, error) {
	switch storeType {
	case S3:
		return &S3Filepath{filePath{isDir: true}}, nil
	case Azure:
		return &AzureFilepath{}, nil
	case GCS:
		return &GCSFilepath{filePath{isDir: true}}, nil
	case Memory:
		return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	case FileSystem:
		return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	//case DB:
	//	return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	case HDFS:
		return nil, fmt.Errorf("currently unsupported file store type '%s'", storeType)
	default:
		return nil, fmt.Errorf("unknown store type '%s'", storeType)
	}
}

type filePath struct {
	scheme  string
	bucket  string
	key     string
	isDir   bool
	isValid bool
}

func (fp *filePath) Scheme() string {
	return fp.scheme
}

func (fp *filePath) Bucket() string {
	return fp.bucket
}

func (fp *filePath) Key() string {
	return fp.key
}

func (fp *filePath) KeyPrefix() string {
	return filepath.Dir(fp.key)
}

func (fp *filePath) Ext() FileType {
	return FileType(filepath.Ext(fp.key))
}

func (fp *filePath) PathWithBucket() string {
	return fp.key
}

func (fp *filePath) IsDir() bool {
	return fp.isDir
}

func (fp *filePath) ParseFilePath(fullPath string) error {
	err := fp.parsePath(fullPath)
	if err != nil {
		return fmt.Errorf("file: %v", err)
	}
	fp.isDir = false
	return nil
}

func (fp *filePath) ParseDirPath(fullPath string) error {
	err := fp.parsePath(fullPath)
	if err != nil {
		return fmt.Errorf("dir: %v", err)
	}
	fp.isDir = true
	return nil
}

func (fp *filePath) checkSchemes(scheme string) error {
	for _, s := range ValidSchemes {
		if s == scheme {
			return nil
		}
	}
	return fmt.Errorf("invalid scheme '%s', must be one of %v", scheme, ValidSchemes)
}

func (fp *filePath) parsePath(fullPath string) error {
	// Parse the URI into a url.URL object.
	u, err := url.Parse(fullPath)
	if err != nil {
		return fmt.Errorf("could not parse fullpath '%s': %v", fullPath, err)
	}

	// Extract the bucket and path components from the URI.
	bucket := u.Host
	path := strings.TrimPrefix(u.Path, "/")

	err = fp.checkSchemes(u.Scheme)
	if err != nil {
		return err
	} else {
		fp.scheme = u.Scheme
	}

	fp.bucket = bucket
	fp.key = path
	return nil
}

func (fp *filePath) IsValid() bool {
	return fp.isValid
}

type S3Filepath struct {
	filePath
}

func (s3 *S3Filepath) Validate() error {
	if s3.scheme != "s3://" && s3.scheme != "s3a://" {
		return fmt.Errorf("invalid scheme '%s', must be 's3:// or 's3a://'", s3.scheme)
	}
	if s3.bucket == "" {
		return fmt.Errorf("bucket cannot be empty")
	} else {
		s3.bucket = strings.Trim(s3.bucket, "/")
	}
	if s3.key == "" {
		return fmt.Errorf("key cannot be empty")
	} else {
		s3.key = strings.Trim(s3.key, "/")
	}

	s3.isValid = true
	return nil
}

func (s3 *S3Filepath) PathWithBucket() string {
	return fmt.Sprintf("%s%s/%s", s3.scheme, s3.bucket, s3.key)
}

type AzureFilepath struct {
	storageAccount string
	filePath
}

func (azure *AzureFilepath) PathWithBucket() string {
	return fmt.Sprintf("abfss://%s@%s.dfs.core.windows.net/%s", azure.bucket, azure.storageAccount, azure.key)
}

func (azure *AzureFilepath) ParseFullPath(fullPath string) error {
	abfssRegex := regexp.MustCompile(`abfss://(.+?)@(.+?)\.dfs.core.windows.net/(.+)`)
	if matches := abfssRegex.FindStringSubmatch(fullPath); len(matches) != 4 {
		return fmt.Errorf("could not parse full path '%s'; expected format abfss://<container/bucket>@<storage_account>.dfs.core.windows.net/path", fullPath)
	} else {
		azure.filePath.bucket = strings.Trim(matches[1], "/")
		azure.storageAccount = strings.Trim(matches[2], "/")
		azure.filePath.key = strings.Trim(matches[3], "/")
	}
	return nil
}

func (azure *AzureFilepath) Validate() error {
	if azure.scheme != "abfss://" {
		return fmt.Errorf("invalid scheme '%s', must be 'abfss://'", azure.scheme)
	}
	if azure.storageAccount == "" {
		return fmt.Errorf("storage account cannot be empty")
	}
	if azure.bucket == "" {
		return fmt.Errorf("bucket cannot be empty")
	} else {
		azure.bucket = strings.Trim(azure.bucket, "/")
	}
	if azure.key == "" {
		return fmt.Errorf("key cannot be empty")
	} else {
		azure.key = strings.Trim(azure.key, "/")
	}
	azure.isValid = true
	return nil
}

type GCSFilepath struct {
	filePath
}

func (gcs *GCSFilepath) PathWithBucket() string {
	return fmt.Sprintf("%s%s/%s", gcs.scheme, gcs.bucket, gcs.key)
}

func (gcs *GCSFilepath) Validate() error {
	if gcs.scheme != "gs://" {
		return fmt.Errorf("invalid scheme '%s', must be 'gs://'", gcs.scheme)
	}
	if gcs.bucket == "" {
		return fmt.Errorf("bucket cannot be empty")
	} else {
		gcs.bucket = strings.Trim(gcs.bucket, "/")
	}
	if gcs.key == "" {
		return fmt.Errorf("key cannot be empty")
	} else {
		gcs.key = strings.Trim(gcs.key, "/")
	}
	gcs.isValid = true
	return nil
}
