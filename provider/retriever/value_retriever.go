// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package retriever

import (
	"encoding/json"

	"github.com/featureform/fferr"
)

const (
	ValueRetrieverTypeStatic      = "static"
	ValueRetrieverTypeEnvironment = "environment"
)

type Value[T SupportedTypes] interface {
	Get() (T, error)
	Serialize() ([]byte, error)
	Deserialize([]byte) error
}

type EnvironmentValue[T SupportedTypes] struct {
	Key      string        `json:"key"`
	Provider ValueProvider `json:"-"`
}

func (e *EnvironmentValue[T]) Get() (T, error) {
	valueStr, err := e.Provider.GetValue(e.Key)
	if err != nil {
		return *new(T), err
	}
	return convertStringToType[T](valueStr)
}

func (e *EnvironmentValue[T]) Serialize() ([]byte, error) {
	return json.Marshal(e)
}

func (e *EnvironmentValue[T]) Deserialize(data []byte) error {
	if err := json.Unmarshal(data, e); err != nil {
		return err
	}
	return nil
}

func (e *EnvironmentValue[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"type": ValueRetrieverTypeEnvironment,
		"key":  e.Key,
	})
}

func (e *EnvironmentValue[T]) UnmarshalJSON(data []byte) error {
	var temp struct {
		Type string `json:"type"`
		Key  string `json:"key"`
	}

	if err := json.Unmarshal(data, &temp); err != nil {
		return fferr.NewInternalErrorf("failed to unmarshal into temporary struct: %w", err)
	}

	if temp.Type != ValueRetrieverTypeEnvironment {
		return fferr.NewInternalErrorf("expected type %s, got %s", ValueRetrieverTypeEnvironment, temp.Type)
	}

	e.Key = temp.Key
	e.Provider = &EnvironmentValueProvider{}

	return nil
}

type StaticValue[T SupportedTypes] struct {
	Value T `json:"value"`
}

func NewStaticValue[T SupportedTypes](value T) *StaticValue[T] {
	return &StaticValue[T]{Value: value}
}

func (s *StaticValue[T]) Get() (T, error) {
	return s.Value, nil
}

func (s *StaticValue[T]) Serialize() ([]byte, error) {
	return json.Marshal(s)
}

func (s *StaticValue[T]) Deserialize(data []byte) error {
	return json.Unmarshal(data, &s)
}

func (s *StaticValue[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.Value)
}

func (s *StaticValue[T]) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &s.Value)
}

func DeserializeValue[T SupportedTypes](data []byte) (Value[T], error) {
	if data == nil {
		return nil, fferr.NewInternalErrorf("data is nil")
	}

	var typeHolder struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &typeHolder); err != nil || typeHolder.Type == "" {
		// Fallback to StaticValue
		var value T
		if err := json.Unmarshal(data, &value); err == nil {
			return &StaticValue[T]{Value: value}, nil
		}
		return nil, fferr.NewInternalErrorf("unable to determine the type of Value")
	}

	switch typeHolder.Type {
	case ValueRetrieverTypeStatic:
		var retriever StaticValue[T]
		if err := retriever.Deserialize(data); err != nil {
			return nil, err
		}
		return &retriever, nil
	case ValueRetrieverTypeEnvironment:
		var retriever EnvironmentValue[T]
		if err := retriever.Deserialize(data); err != nil {
			return nil, err
		}
		return &retriever, nil
	default:
		return nil, fferr.NewInternalErrorf("unknown type: %s", typeHolder.Type)
	}
}
