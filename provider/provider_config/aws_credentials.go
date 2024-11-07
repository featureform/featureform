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
)

type AWSCredentialsType string

const (
	AWSStaticCredentialsType     AWSCredentialsType = "AWS_STATIC_CREDENTIALS"
	AWSAssumeRoleCredentialsType AWSCredentialsType = "AWS_ASSUME_ROLE_CREDENTIALS"
)

type AWSStaticCredentials struct {
	AccessKeyId string
	SecretKey   string
}

func (c AWSStaticCredentials) CredentialType() AWSCredentialsType {
	return AWSStaticCredentialsType
}

func (c AWSStaticCredentials) MarshalJSON() ([]byte, error) {
	type Alias AWSStaticCredentials // Prevents recursion
	return json.Marshal(&struct {
		Type AWSCredentialsType
		*Alias
	}{
		Type:  c.CredentialType(),
		Alias: (*Alias)(&c),
	})
}

type AWSAssumeRoleCredentials struct{}

func (c AWSAssumeRoleCredentials) CredentialType() AWSCredentialsType {
	return AWSAssumeRoleCredentialsType
}

func (c AWSAssumeRoleCredentials) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Type AWSCredentialsType
	}{
		Type: c.CredentialType(),
	})
}

type AWSCredentials interface {
	CredentialType() AWSCredentialsType
	MarshalJSON() ([]byte, error)
}

type AWSUnmarshalHelper struct {
	Type AWSCredentialsType
}

func UnmarshalAWSCredentials(data []byte) (AWSCredentials, error) {
	var helper AWSUnmarshalHelper
	if err := json.Unmarshal(data, &helper); err != nil {
		return nil, err
	}

	switch helper.Type {
	case AWSStaticCredentialsType:
		var creds AWSStaticCredentials
		if err := json.Unmarshal(data, &creds); err != nil {
			return nil, err
		}
		return creds, nil
	case AWSAssumeRoleCredentialsType:
		return AWSAssumeRoleCredentials{}, nil
	default:
		return nil, fferr.NewInvalidArgumentErrorf("Unknown credential type: %s", helper.Type)
	}
}
