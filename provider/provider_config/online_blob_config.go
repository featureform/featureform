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

	fs "github.com/featureform/filestore"
)

type OnlineBlobConfig struct {
	Type   fs.FileStoreType
	Config AzureFileStoreConfig
}

func (online OnlineBlobConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(online)
	if err != nil {
		panic(err)
	}
	return config
}

func (online *OnlineBlobConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, online)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}
