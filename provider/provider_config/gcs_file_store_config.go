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

type GCSFileStoreConfig struct {
	BucketName  string
	BucketPath  string
	Credentials GCPCredentials
}

func (s *GCSFileStoreConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, s)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (s *GCSFileStoreConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(s)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return conf, nil
}

func (s *GCSFileStoreConfig) IsFileStoreConfig() bool {
	return true
}

func (s GCSFileStoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}

func (a GCSFileStoreConfig) DifferingFields(b GCSFileStoreConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
