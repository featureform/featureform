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

type DynamodbConfig struct {
	Prefix             string
	Region             string
	Credentials        AWSCredentials
	Endpoint           string
	StronglyConsistent bool
	Tags               map[string]string
}

type dynamodbConfigTemp struct {
	Prefix             string
	Region             string
	Credentials        json.RawMessage
	Endpoint           string
	StronglyConsistent bool
	Tags               map[string]string
}

func (d DynamodbConfig) Serialized() SerializedConfig {
	config, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	return config
}

func (d *DynamodbConfig) Deserialize(config []byte) error {
	var temp dynamodbConfigTemp
	if err := json.Unmarshal(config, &temp); err != nil {
		return fferr.NewInternalError(err)
	}

	d.Prefix = temp.Prefix
	d.Region = temp.Region
	d.StronglyConsistent = temp.StronglyConsistent
	d.Tags = temp.Tags

	creds, err := UnmarshalAWSCredentials(temp.Credentials)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	d.Credentials = creds

	return nil
}

func (d DynamodbConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
		"Tags":        true,
	}
}

func (a DynamodbConfig) DifferingFields(b DynamodbConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
