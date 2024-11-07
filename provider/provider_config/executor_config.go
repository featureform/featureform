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

	cfg "github.com/featureform/config"
	ss "github.com/featureform/helpers/stringset"
)

type ExecutorConfig struct {
	DockerImage string `json:"docker_image"`
}

func (c *ExecutorConfig) Serialize() ([]byte, error) {
	serialized, err := json.Marshal(c)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return serialized, nil
}

func (c *ExecutorConfig) Deserialize(config []byte) error {
	err := json.Unmarshal(config, &c)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (c *ExecutorConfig) GetImage() string {
	if c.DockerImage == "" {
		return cfg.GetPandasRunnerImage()
	} else {
		return c.DockerImage
	}
}

func (c ExecutorConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"DockerImage": true,
	}
}

func (a ExecutorConfig) DifferingFields(b ExecutorConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
