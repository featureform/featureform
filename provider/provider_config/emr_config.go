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

type EMRConfig struct {
	Credentials   AWSCredentials
	ClusterRegion string
	ClusterName   string
}

type emrConfigTemp struct {
	ClusterRegion string
	ClusterName   string
	Credentials   json.RawMessage
}

func (e *EMRConfig) Deserialize(config SerializedConfig) error {
	var temp emrConfigTemp
	if err := json.Unmarshal(config, &temp); err != nil {
		return fferr.NewInternalError(err)
	}

	e.ClusterRegion = temp.ClusterRegion
	e.ClusterName = temp.ClusterName

	creds, err := UnmarshalAWSCredentials(temp.Credentials)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	e.Credentials = creds

	return nil
}

func (e *EMRConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(e)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return conf, nil
}

func (e *EMRConfig) IsExecutorConfig() bool {
	return true
}

func (e EMRConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials":   true,
		"ClusterName":   true,
		"ClusterRegion": true,
	}
}

func (a EMRConfig) DifferingFields(b EMRConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
