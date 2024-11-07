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

type FirestoreConfig struct {
	Collection  string
	ProjectID   string
	Credentials map[string]interface{}
}

func (fs FirestoreConfig) Serialize() SerializedConfig {
	config, err := json.Marshal(fs)
	if err != nil {
		panic(err)
	}
	return config
}

func (fs *FirestoreConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, fs)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (fs FirestoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}

func (a FirestoreConfig) DifferingFields(b FirestoreConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
