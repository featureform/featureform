//go:build provider
// +build provider

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
package provider

import (
	"fmt"
	"reflect"
	"testing"
)

var mockConfig SerializedConfig = SerializedConfig("abc")

func mockFactory(c SerializedConfig) (Provider, error) {
	if !reflect.DeepEqual(c, mockConfig) {
		return nil, fmt.Errorf("Not mock config")
	}
	return nil, nil
}

func TestFactory(t *testing.T) {
	mockType := Type("mock")
	if err := RegisterFactory(mockType, mockFactory); err != nil {
		t.Fatalf("Failed to register factory: %s", err)
	}
	if _, err := Get(mockType, mockConfig); err != nil {
		t.Fatalf("Failed to get provider: %s", err)
	}

}

func TestFactoryExists(t *testing.T) {
	mockType := Type("already exists")
	if err := RegisterFactory(mockType, mockFactory); err != nil {
		t.Fatalf("Failed to register factory: %s", err)
	}
	if err := RegisterFactory(mockType, mockFactory); err == nil {
		t.Fatalf("Succeeded in registering factory twice")
	}
}

func TestFactoryDoesntExists(t *testing.T) {
	if provider, err := Get(Type("Doesnt exist"), mockConfig); err == nil {
		t.Fatalf("Succeeded in getting unregistered provider: %v", provider)
	}
}

func TestBaseProvider(t *testing.T) {
	type MockProvider struct {
		BaseProvider
	}
	mockType := Type("mock")
	var mock Provider = &MockProvider{
		BaseProvider{
			ProviderType:   mockType,
			ProviderConfig: mockConfig,
		},
	}
	if _, err := mock.AsOnlineStore(); err == nil {
		t.Fatalf("BaseProvider succeeded in OnlineStore cast")
	}
	if _, err := mock.AsOfflineStore(); err == nil {
		t.Fatalf("BaseProvider succeeded in OfflineStore cast")
	}
	if !reflect.DeepEqual(mock.Type(), mockType) {
		t.Fatalf("Type not passed down to provider")
	}
	if !reflect.DeepEqual(mock.Config(), mockConfig) {
		t.Fatalf("Config not passed down to provider")
	}
}
