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

type RedisConfig struct {
	Prefix   string
	Addr     string
	Password string
	DB       int
}

func (r RedisConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	return config
}

func (r *RedisConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, r)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (r RedisConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Password": true,
	}
}

func (a RedisConfig) DifferingFields(b RedisConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
