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

type AzureFileStoreConfig struct {
	AccountName   string
	AccountKey    string
	ContainerName string
	Path          string
}

func (store *AzureFileStoreConfig) IsFileStoreConfig() bool {
	return true
}

func (store *AzureFileStoreConfig) Serialize() ([]byte, error) {
	data, err := json.Marshal(store)
	if err != nil {
		panic(err)
	}
	return data, nil
}

func (store *AzureFileStoreConfig) Deserialize(data SerializedConfig) error {
	err := json.Unmarshal(data, store)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (store AzureFileStoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"AccountKey": true,
	}
}

func (a AzureFileStoreConfig) DifferingFields(b AzureFileStoreConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
