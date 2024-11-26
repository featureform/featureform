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

type UnitTestConfig struct {
	Username string `json:"Username"`
	Password string `json:"Password"`
}

func (pg *UnitTestConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, pg)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (pg *UnitTestConfig) Serialize() []byte {
	conf, err := json.Marshal(pg)
	if err != nil {
		panic(err)
	}
	return conf
}

func (pg UnitTestConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username": true,
		"Password": true,
	}
}

func (a UnitTestConfig) DifferingFields(b UnitTestConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
