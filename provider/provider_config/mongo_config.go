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

type MongoDBConfig struct {
	Host       string
	Port       string
	Username   string
	Password   string
	Database   string
	Throughput int
}

func (m MongoDBConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return config
}

func (m *MongoDBConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, m)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (m MongoDBConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username":   true,
		"Password":   true,
		"Port":       true,
		"Throughput": true,
	}
}

func (a MongoDBConfig) DifferingFields(b MongoDBConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
