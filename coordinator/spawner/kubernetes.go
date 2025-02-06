// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package spawner

import (
	"encoding/json"
	"time"

	"github.com/featureform/fferr"
	"github.com/featureform/metadata"
)

type Config []byte

type ResourceUpdatedEvent struct {
	ResourceID metadata.ResourceID
	Completed  time.Time
}

func (c *ResourceUpdatedEvent) Serialize() (Config, error) {
	config, err := json.Marshal(c)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return config, nil
}

func (c *ResourceUpdatedEvent) Deserialize(config Config) error {
	err := json.Unmarshal(config, c)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}
