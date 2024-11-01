// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider_config

import (
	"encoding/json"
	"fmt"

	"github.com/featureform/fferr"
	"github.com/featureform/provider/provider_type"

	ss "github.com/featureform/helpers/stringset"
	sr "github.com/featureform/helpers/struct_iterator"
)

type SnowflakeTableConfig struct {
	TargetLag   string
	RefreshMode string
	Initialize  string
}

type SnowflakeCatalogConfig struct {
	ExternalVolume string
	BaseLocation   string
	TableConfig    SnowflakeTableConfig
}

type SnowflakeConfig struct {
	Username       string
	Password       string
	AccountLocator string
	Organization   string
	Account        string
	Database       string
	Schema         string
	Warehouse      string `snowflake:"warehouse"`
	Role           string `snowflake:"role"`
	Catalog        *SnowflakeCatalogConfig
}

func (sf *SnowflakeConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, sf)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (sf *SnowflakeConfig) Serialize() []byte {
	conf, err := json.Marshal(sf)
	if err != nil {
		panic(err)
	}
	return conf
}

func (sf SnowflakeConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username":  true,
		"Password":  true,
		"Role":      true,
		"Schema":    true,
		"Database":  true,
		"Warehouse": true,
		// TODO: (Erik) consider the implications of allowing the catalog config to be mutable
	}
}

func (a SnowflakeConfig) DifferingFields(b SnowflakeConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}

func (sf *SnowflakeConfig) HasLegacyCredentials() bool {
	return sf.AccountLocator != ""
}

func (sf *SnowflakeConfig) HasCurrentCredentials() (bool, error) {
	if (sf.Account != "" && sf.Organization == "") || (sf.Account == "" && sf.Organization != "") {
		return false, fferr.NewProviderConfigError(string(provider_type.SnowflakeOffline), fmt.Errorf("credentials must include both Account and Organization"))
	} else {
		return sf.Account != "" && sf.Organization != "", nil
	}
}

func (sf *SnowflakeConfig) ConnectionString(database, schema string) (string, error) {
	connString, err := sf.buildConnectionString(database, schema)
	if err != nil {
		return "", err
	}
	return connString, nil
}

func (sf *SnowflakeConfig) buildConnectionString(database, schema string) (string, error) {
	base, err := sf.getBaseConnection(database, schema)
	if err != nil {
		return "", err
	}
	parameters, err := sf.getConnectionParameters()
	if err != nil {
		return "", err
	}
	return sf.makeFullConnection(base, parameters), nil
}

func (sf *SnowflakeConfig) makeFullConnection(base, parameters string) string {
	return fmt.Sprintf("%s%s", base, parameters)
}

const emptyParameters = "?"

func (sf *SnowflakeConfig) getConnectionParameters() (string, error) {
	base := emptyParameters

	// Adds all fields with a snowflake tag on the struct to the connection string
	iter, err := sr.NewStructIterator(*sf)
	if err != nil {
		return "", err
	}
	for iter.Next() {
		if tag := iter.Tag("snowflake"); tag != "" {
			base = sf.addParameter(base, tag, iter.Value())
		}
	}

	if base == emptyParameters {
		return "", nil
	}
	return base, nil
}

func (sf *SnowflakeConfig) addParameter(base, key string, val interface{}) string {
	if val == "" {
		return base
	}
	if base != emptyParameters {
		base += "&"
	}
	base += fmt.Sprintf("%s=%s", key, val)
	return base
}

func (sf *SnowflakeConfig) getBaseConnection(database, schema string) (string, error) {
	isLegacy := sf.HasLegacyCredentials()
	isCurrent, err := sf.HasCurrentCredentials()
	if err != nil {
		return "", err
	}

	if database == "" {
		database = sf.Database
	}
	if schema == "" {
		schema = sf.schema()
	}

	if isLegacy && isCurrent {
		return "", fferr.NewProviderConfigError(string(provider_type.SnowflakeOffline), fmt.Errorf("cannot use both legacy and current credentials"))
	}

	if isLegacy {
		return fmt.Sprintf("%s:%s@%s/%s/%s", sf.Username, sf.Password, sf.AccountLocator, database, schema), nil
	}

	if isCurrent {
		return fmt.Sprintf("%s:%s@%s-%s/%s/%s", sf.Username, sf.Password, sf.Organization, sf.Account, database, schema), nil
	}

	return "", fferr.NewProviderConfigError(string(provider_type.SnowflakeOffline), fmt.Errorf("credentials not found"))
}

func (sf *SnowflakeConfig) schema() string {
	if sf.Schema == "" {
		return "PUBLIC"
	}
	return sf.Schema
}
