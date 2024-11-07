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
	ss "github.com/featureform/helpers/stringset"
	"github.com/mitchellh/mapstructure"
)

type CredentialType string

const (
	BasicCredential    CredentialType = "BasicCredential"
	KerberosCredential CredentialType = "KerberosCredential"
)

type HDFSCredentialConfig interface {
	Serialize() ([]byte, error)
	Deserialize(config SerializedConfig) error
	IsHDFSCredential() bool
}

type BasicCredentialConfig struct {
	Username string
	Password string
}

func (s *BasicCredentialConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, s)
	if err != nil {
		return err
	}
	return nil
}

func (s *BasicCredentialConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func (s *BasicCredentialConfig) IsHDFSCredential() bool {
	return true
}

type KerberosCredentialConfig struct {
	Username string
	Password string
	Krb5Conf string
}

func (s *KerberosCredentialConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, s)
	if err != nil {
		return err
	}
	return nil
}

func (s *KerberosCredentialConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func (s *KerberosCredentialConfig) IsHDFSCredential() bool {
	return true
}

type HDFSFileStoreConfig struct {
	Host             string
	Port             string
	Path             string
	HDFSSiteConf     string
	CoreSiteConf     string
	CredentialType   CredentialType
	CredentialConfig HDFSCredentialConfig
}

func (s *HDFSFileStoreConfig) Deserialize(config SerializedConfig) error {
	err := json.Unmarshal(config, s)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	return nil
}

func (s *HDFSFileStoreConfig) Serialize() ([]byte, error) {
	conf, err := json.Marshal(s)
	if err != nil {
		return nil, fferr.NewInternalError(err)
	}
	return conf, nil
}

func (s *HDFSFileStoreConfig) UnmarshalJSON(data []byte) error {
	type tempConfig struct {
		Host             string
		Port             string
		Path             string
		HDFSSiteConf     string
		CoreSiteConf     string
		CredentialType   CredentialType
		CredentialConfig map[string]interface{}
	}

	var temp tempConfig
	err := json.Unmarshal(data, &temp)
	if err != nil {
		return fferr.NewInternalError(err)
	}

	s.Host = temp.Host
	s.Port = temp.Port
	s.Path = temp.Path
	s.HDFSSiteConf = temp.HDFSSiteConf
	s.CoreSiteConf = temp.CoreSiteConf
	s.CredentialType = temp.CredentialType

	err = s.decodeCredentials(temp.CredentialType, temp.CredentialConfig)
	if err != nil {
		return err
	}

	return nil
}

func (s *HDFSFileStoreConfig) decodeCredentials(credType CredentialType, credConfigMap map[string]interface{}) error {
	var credentialConfig HDFSCredentialConfig
	switch credType {
	case BasicCredential:
		credentialConfig = &BasicCredentialConfig{}
	case KerberosCredential:
		credentialConfig = &KerberosCredentialConfig{}
	default:
		return fferr.NewInvalidArgumentError(fmt.Errorf("unknown credential type: %s", credType))
	}

	err := mapstructure.Decode(credConfigMap, credentialConfig)
	if err != nil {
		return fferr.NewInternalError(err)
	}
	s.CredentialConfig = credentialConfig
	return nil
}

func (s *HDFSFileStoreConfig) IsFileStoreConfig() bool {
	return true
}

func (s *HDFSFileStoreConfig) DifferingFields(b HDFSFileStoreConfig) (ss.StringSet, error) {
	return differingFields(s, b)
}

func (s HDFSFileStoreConfig) MutableFields() ss.StringSet {
	return ss.StringSet{
		"Host":             true,
		"Port":             true,
		"Path":             true,
		"HDFSSiteConf":     true,
		"CoreSiteConf":     true,
		"CredentialType":   true,
		"CredentialConfig": true,
	}
}
