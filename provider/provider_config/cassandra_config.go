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

type CassandraConfig struct {
	Keyspace    string
	Addr        string
	Username    string
	Password    string
	Consistency string
	Replication int
}

func (cass CassandraConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(cass)
	if err != nil {
		panic(err)
	}
	return config
}

func (cass *CassandraConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, cass)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (cass CassandraConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username":    true,
		"Password":    true,
		"Consistency": true,
		"Replication": true,
	}
}

func (a CassandraConfig) DifferingFields(b CassandraConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
