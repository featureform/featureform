// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider_config

import (
	"encoding/json"

	"github.com/featureform/fferr"

	ss "github.com/featureform/helpers/stringset"
)

type S3FileStoreConfig struct {
	Credentials  AWSCredentials
	BucketRegion string
	// BucketPath is the bucket name, no s3://
	BucketPath string
	// Path is the subpath in the bucket to work in
	Path string
	// Endpoint is used when using a S3 compatible service outside of AWS like localstack
	Endpoint string
}

type s3FileStoreConfigTemp struct {
	BucketRegion string
	BucketPath   string
	Path         string
	Credentials  json.RawMessage
}

func (s *S3FileStoreConfig) Deserialize(config SerializedConfig) error {
	var temp s3FileStoreConfigTemp
	if err := json.Unmarshal(config, &temp); err != nil {
		return fferr.NewInternalError(err)
	}

	s.BucketPath = temp.BucketPath
	s.BucketRegion = temp.BucketRegion
	s.Path = temp.Path

	creds, err := UnmarshalAWSCredentials(temp.Credentials)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	s.Credentials = creds

	return nil
}

func (s *S3FileStoreConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(s)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return conf, nil
}

func (s *S3FileStoreConfig) IsFileStoreConfig() bool {
	return true
}

func (s S3FileStoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}

func (a S3FileStoreConfig) DifferingFields(b S3FileStoreConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
