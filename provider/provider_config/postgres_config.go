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
	r "github.com/featureform/provider/retriever"

	ss "github.com/featureform/helpers/stringset"
)

type PostgresConfig struct {
	Host     string          `json:"Host"`
	Port     string          `json:"Port"`
	Username string          `json:"Username"`
	Password r.Value[string] `json:"Password"`
	Database string          `json:"Database"`
	Schema   string          `json:"Schema"`
	SSLMode  string          `json:"SSLMode"`
}

func (pg *PostgresConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, pg)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (pg *PostgresConfig) UnmarshalJSON(data []byte) error {
	type Alias PostgresConfig

	aux := struct {
		Password json.RawMessage `json:"Password"`
		*Alias
	}{
		Alias: (*Alias)(pg), // Use the alias to avoid calling UnmarshalJSON recursively
	}

	err := json.Unmarshal(data, &aux)
	if err != nil {
		return err
	}

	if aux.Password != nil {
		pass, err := r.DeserializeValue[string](aux.Password)
		if err != nil {
			return err
		}
		pg.Password = pass
	} else {
		pg.Password = r.NewStaticValue[string]("")
	}

	if pg.Schema == "" {
		pg.Schema = "public"
	}

	return nil
}

func (pg *PostgresConfig) Serialize() []byte {
	conf, err := json.Marshal(pg)
	if err != nil {
		panic(err)
	}
	return conf
}

func (pg PostgresConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Username": true,
		"Password": true,
		"Port":     true,
		"SSLMode":  true,
	}
}

func (a PostgresConfig) DifferingFields(b PostgresConfig) (ss.StringSet, error) {
	return differingFields(a, b)
}
