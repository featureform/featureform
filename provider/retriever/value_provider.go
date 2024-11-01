// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package retriever

import (
	"encoding/json"
	"fmt"
	"os"
)

const (
	ValueProviderTypeEnvironment = "environment"
)

type ValueProvider interface {
	GetValue(key string) (string, error)
	Serialize() ([]byte, error)
	Deserialize([]byte) error
}

type EnvironmentValueProvider struct{}

func (e *EnvironmentValueProvider) GetValue(key string) (string, error) {
	value, has := os.LookupEnv(key)
	if !has {
		return "", fmt.Errorf("environment variable %s not set", key)
	}
	return value, nil
}

func (e *EnvironmentValueProvider) Serialize() ([]byte, error) {
	return json.Marshal(e)
}

func (e *EnvironmentValueProvider) Deserialize(data []byte) error {
	return nil
}

func (e *EnvironmentValueProvider) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"type": ValueProviderTypeEnvironment,
	})
}

func DeserializeValueProvider(data []byte) (ValueProvider, error) {
	var typeHolder struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &typeHolder); err != nil || typeHolder.Type == "" {
		return nil, fmt.Errorf("unable to determine the type of ValueProvider")
	}

	// Use type to identify which concrete type to instantiate
	switch typeHolder.Type {
	case ValueProviderTypeEnvironment:
		return &EnvironmentValueProvider{}, nil
	default:
		return nil, fmt.Errorf("unknown type %s", typeHolder.Type)
	}
}
