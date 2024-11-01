// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider_config

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAWSStaticCredentials_MarshalUnmarshal(t *testing.T) {
	creds := AWSStaticCredentials{
		AccessKeyId: "testAccessKey",
		SecretKey:   "testSecretKey",
	}

	marshaled, err := json.Marshal(creds)
	if err != nil {
		t.Errorf("Marshaling failed: %v", err)
	}

	var unmarshaled AWSStaticCredentials
	err = json.Unmarshal(marshaled, &unmarshaled)
	if err != nil {
		t.Errorf("Unmarshaling failed: %v", err)
	}

	if creds != unmarshaled {
		t.Errorf("Expected creds to be %v, got %v", creds, unmarshaled)
	}
}

func TestAWSAssumeRoleCredentials_Marshal(t *testing.T) {
	creds := AWSAssumeRoleCredentials{}

	marshaled, err := json.Marshal(creds)
	if err != nil {
		t.Errorf("Marshaling failed: %v", err)
	}

	if !strings.Contains(string(marshaled), string(AWSAssumeRoleCredentialsType)) {
		t.Errorf("Marshalled data does not contain '%s'", AWSAssumeRoleCredentialsType)
	}
}

func TestUnmarshalAWSCredentials(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		wantType AWSCredentialsType
		wantErr  bool
	}{
		{
			name:     "static credentials",
			jsonData: `{"Type":"AWS_STATIC_CREDENTIALS","AccessKeyId":"AKIA1234567890","SecretKey":"secret"}`,
			wantType: AWSStaticCredentialsType,
			wantErr:  false,
		},
		{
			name:     "assume role credentials",
			jsonData: `{"Type":"AWS_ASSUME_ROLE_CREDENTIALS"}`,
			wantType: AWSAssumeRoleCredentialsType,
			wantErr:  false,
		},
		{
			name:     "invalid type",
			jsonData: `{"Type":"UNKNOWN"}`,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			creds, err := UnmarshalAWSCredentials([]byte(tt.jsonData))
			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalAWSCredentials() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && creds.CredentialType() != tt.wantType {
				t.Errorf("UnmarshalAWSCredentials() gotType = %v, want %v", creds.CredentialType(), tt.wantType)
			}
		})
	}
}
