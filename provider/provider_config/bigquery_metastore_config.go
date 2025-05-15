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
	"github.com/featureform/filestore"
	ss "github.com/featureform/helpers/stringset"
)

type BigQueryMetastoreConfig struct {
	ProjectID   string
	Region      string
	Warehouse   filestore.GCSFilepath
	Credentials map[string]any
}

type bigqueryMetastoreConfigTemp struct {
	ProjectID   string
	Region      string
	Bucket      string
	BucketDir   string
	Credentials map[string]any
}

func (bq *BigQueryMetastoreConfig) Deserialize(config SerializedConfig) error {
	var temp bigqueryMetastoreConfigTemp
	if err := json.Unmarshal(config, &temp); err != nil {
		return fferr.NewInternalError(err)
	}
	bq.ProjectID = temp.ProjectID
	bq.Region = temp.Region
	bq.Credentials = temp.Credentials

	path, err := filestore.NewGCSFilepath(temp.Bucket, temp.BucketDir, true)
	if err != nil {
		return fferr.NewProviderConfigError("Invalid bucket name or path", err)
	}
	bq.Warehouse = *path
	return nil
}

func (bq *BigQueryMetastoreConfig) Serialize() ([]byte, error) {
	temp := bigqueryMetastoreConfigTemp{
		ProjectID:   bq.ProjectID,
		Region:      bq.Region,
		Bucket:      bq.Warehouse.Bucket(),
		BucketDir:   bq.Warehouse.Key(),
		Credentials: bq.Credentials,
	}
	conf, err := json.Marshal(temp)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return conf, nil
}

func (bq *BigQueryMetastoreConfig) IsFileStoreConfig() bool {
	return true
}

func (bq BigQueryMetastoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Credentials": true,
	}
}

func (a BigQueryMetastoreConfig) DifferingFields(b BigQueryMetastoreConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
