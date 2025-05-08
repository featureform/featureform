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

type DataprocConfig struct {
	ProjectID string
	Region    string
	JSONCreds map[string]string
}

func (d *DataprocConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, d)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (d *DataprocConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(d)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return conf, nil
}

func (d *DataprocConfig) IsExecutorConfig() bool {
	return true
}

func (d DataprocConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username": true,
		"Password": true,
		"Token":    true,
	}
}

func (a DataprocConfig) DifferingFields(b DataprocConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
