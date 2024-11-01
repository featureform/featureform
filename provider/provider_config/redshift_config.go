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

type RedshiftConfig struct {
	Host     string
	Port     string
	Database string
	Username string
	Password string
	SSLMode  string
}

func (rs *RedshiftConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, rs)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (rs *RedshiftConfig) Serialize() []byte {
	conf, err := json.Marshal(rs)
	if err != nil {
		panic(err)
	}
	return conf
}

func (rs RedshiftConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username": true,
		"Password": true,
		"Port":     true,
		"SSLMode":  true,
	}
}

func (a RedshiftConfig) DifferingFields(b RedshiftConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
