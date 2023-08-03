// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package provider

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	pc "github.com/featureform/provider/provider_config"
)

type Filepath interface {
	// TODO: add documentation to explain this should return bucket (S3)/container (Azure Blob Storage) name
	Bucket() string
	Prefix() string
	// TODO: add documentation to explain this should return the object (S3)/key (Azure Blob Storage) name
	Path() string
	FullPathWithBucket() string
	FullPathWithoutBucket() string
	ParseFullPath(path string) error
}

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
		return fmt.Errorf("could not parse full path '%s'", fullPath)
	} else {
		azure.filePath.bucket = matches[1]
		azure.storageAccount = matches[2]
		azure.filePath.path = matches[3]
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
