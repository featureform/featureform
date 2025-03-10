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

type MySqlConfig struct {
	Host     string `json:"Host"`
	Port     string `json:"Port"`
	Username string `json:"Username"`
	Password string `json:"Password"`
	Database string `json:"Database"`
}

func (my *MySqlConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, my)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (my *MySqlConfig) Serialize() []byte {
	conf, err := json.Marshal(my)
	if err != nil {
		panic(err)
	}
	return conf
}

func (my MySqlConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username": true,
		"Password": true,
		"Host":     true,
		"Port":     true,
		"Database": true,
	}
}

func (a MySqlConfig) DifferingFields(b MySqlConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
