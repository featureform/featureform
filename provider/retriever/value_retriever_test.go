// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package retriever

import (
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func setEnvVariable(t *testing.T, key, value string) {
	err := os.Setenv(key, value)
	if err != nil {
		t.Fatalf("Failed to set environment variable: %v", err)
	}
	t.Cleanup(func() {
		os.Unsetenv(key)
	})
}

func testEnvironmentValueRetriever[T SupportedTypes](t *testing.T, expectedValue T, envValue string) {
	key := uuid.New().String()

	setEnvVariable(t, key, envValue)

	retriever := &EnvironmentValue[T]{
		Key:      key,
		Provider: &EnvironmentValueProvider{},
	}

	value, err := retriever.Get()
	assert.NoError(t, err)
	assert.Equal(t, expectedValue, value)
}

func TestEnvironmentValueRetrieverString(t *testing.T) {
	testEnvironmentValueRetriever[string](t, "env-value", "env-value")
}

func TestEnvironmentValueRetrieverInt(t *testing.T) {
	testEnvironmentValueRetriever[int](t, 42, "42")
}

func TestEnvironmentValueRetrieverBool(t *testing.T) {
	testEnvironmentValueRetriever[bool](t, true, "true")
}

func TestEnvironmentValueRetrieverMissingEnv(t *testing.T) {
	key := uuid.New().String()

	retriever := &EnvironmentValue[string]{
		Key:      key,
		Provider: &EnvironmentValueProvider{},
	}

	_, err := retriever.Get()
	assert.Error(t, err)
}

func TestEnvironmentValueRetriever_Serde(t *testing.T) {
	key := uuid.New().String()
	retriever := &EnvironmentValue[string]{
		Key:      key,
		Provider: &EnvironmentValueProvider{},
	}

	data, err := retriever.Serialize()
	assert.NoError(t, err)

	deser := &EnvironmentValue[string]{}
	err = deser.Deserialize(data)
	assert.NoError(t, err)

	assert.Equal(t, retriever, deser)
}

func TestStaticValueRetriever_Serde(t *testing.T) {
	value := "test-value"
	retriever := NewStaticValue(value)

	data, err := retriever.Serialize()
	assert.NoError(t, err)

	deser := &StaticValue[string]{}
	err = deser.Deserialize(data)
	assert.NoError(t, err)

	assert.Equal(t, retriever, deser)
}
