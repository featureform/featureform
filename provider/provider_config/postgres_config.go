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
	"github.com/featureform/secrets"
)

const defaultSchema = "public"

type PostgresConfig struct {
	Host     string `json:"Host"`
	Port     string `json:"Port"`
	Username string `json:"Username"`
	Password string `json:"Password"`
	Database string `json:"Database"`
	Schema   string `json:"Schema"`
	SSLMode  string `json:"SSLMode"`
}

type unresolvedPostgresConfig struct {
	Host     string                 `json:"Host"`
	Port     string                 `json:"Port"`
	Username secrets.Secret[string] `json:"Username"`
	Password secrets.Secret[string] `json:"Password"`
	Database string                 `json:"Database"`
	Schema   string                 `json:"Schema"`
	SSLMode  string                 `json:"SSLMode"`
}

func (u *unresolvedPostgresConfig) UnmarshalJSON(data []byte) error {

	type Alias unresolvedPostgresConfig
	aux := struct {
		Password json.RawMessage `json:"Password"`
		Username json.RawMessage `json:"Username"`
		*Alias
	}{
		Alias: (*Alias)(u),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	ea := &secrets.ErrorGuard{}
	u.Username = secrets.TryDeserializeSecret[string](ea, aux.Username)
	u.Password = secrets.TryDeserializeSecret[string](ea, aux.Password)

	if err := ea.Err(); err != nil {
		return err
	}

	return nil
}

func (u *unresolvedPostgresConfig) Resolve(secretsManager secrets.Manager) (*PostgresConfig, error) {
	if u == nil {
		return nil, fferr.NewInternalErrorf("unresolved config is nil")
	}

	ea := &secrets.ErrorGuard{}
	pw := secrets.TryResolveSecret[string](ea, u.Password, secretsManager)
	un := secrets.TryResolveSecret[string](ea, u.Username, secretsManager)

	if err := ea.Err(); err != nil {
		return nil, err
	}

	cfg := &PostgresConfig{
		Host:     u.Host,
		Port:     u.Port,
		Username: un,
		Password: pw,
		Database: u.Database,
		Schema:   u.Schema,
		SSLMode:  u.SSLMode,
	}

	if cfg.Schema == "" {
		cfg.Schema = defaultSchema
	}

	return cfg, nil
}

func (pg *PostgresConfig) Serialize() ([]byte, error) {
	return json.Marshal(pg)
}

func (pg *PostgresConfig) Deserialize(config SerializedConfig) error {
	if err := pg.DeserializeAndResolve(config, nil); err != nil {
		return err
	}

	if pg.Schema == "" {
		pg.Schema = defaultSchema
	}

	return nil
}

func (pg *PostgresConfig) DeserializeAndResolve(config SerializedConfig, manager secrets.Manager) error {
	unresolved := &unresolvedPostgresConfig{}
	if err := json.Unmarshal(config, unresolved); err != nil {
		return err
	}

	resolved, err := unresolved.Resolve(manager)
	if err != nil {
		return err
	}

	*pg = *resolved
	return nil
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
