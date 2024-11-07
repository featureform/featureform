// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/featureform/filestore"
	pl "github.com/featureform/provider/location"
	pc "github.com/featureform/provider/provider_config"
	pt "github.com/featureform/provider/provider_type"
)

var mockConfig pc.SerializedConfig = pc.SerializedConfig("abc")

func mockFactory(c pc.SerializedConfig) (Provider, error) {
	if !reflect.DeepEqual(c, mockConfig) {
		return nil, fmt.Errorf("Not mock config")
	}
	return nil, nil
}

func TestFactory(t *testing.T) {
	mockType := pt.Type("mock")
	if err := RegisterFactory(mockType, mockFactory); err != nil {
		t.Fatalf("Failed to register factory: %s", err)
	}
	if _, err := Get(mockType, mockConfig); err != nil {
		t.Fatalf("Failed to get provider: %s", err)
	}

}

func TestFactoryExists(t *testing.T) {
	mockType := pt.Type("already exists")
	if err := RegisterFactory(mockType, mockFactory); err != nil {
		t.Fatalf("Failed to register factory: %s", err)
	}
	if err := RegisterFactory(mockType, mockFactory); err == nil {
		t.Fatalf("Succeeded in registering factory twice")
	}
}

func TestFactoryDoesntExists(t *testing.T) {
	if provider, err := Get(pt.Type("Doesnt exist"), mockConfig); err == nil {
		t.Fatalf("Succeeded in getting unregistered provider: %v", provider)
	}
}

func TestBaseProvider(t *testing.T) {
	type MockProvider struct {
		BaseProvider
	}
	mockType := pt.Type("mock")
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

func TestLocationInterface(t *testing.T) {
	tests := []struct {
		name           string
		locationType   string
		locationString string
	}{
		{
			name:           "TestSQLLocation",
			locationType:   "sql",
			locationString: "test_table",
		},
		{
			name:           "TestFileLocation",
			locationType:   "s3File",
			locationString: "s3://test_bucket/test_file.csv",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			switch test.locationType {
			case "sql":
				location := pl.NewSQLLocation(test.locationString)
				if location.Location() != test.locationString {
					t.Fatalf("SQLLocation failed to return correct location: Expected: %s, Got: %s", test.locationString, location.Location())
				}
			case "s3File":
				s3FilePath, err := filestore.NewEmptyFilepath(filestore.S3)
				if err != nil {
					t.Fatalf("Failed to create S3 filepath: %s", err)
				}
				err = s3FilePath.ParseFilePath(test.locationString)
				if err != nil {
					t.Fatalf("Failed to parse S3 filepath: %s", err)
				}

				location := pl.NewFileLocation(s3FilePath)
				if location.Location() != test.locationString {
					t.Fatalf("FileLocation failed to return correct location: Expected: %s, Got: %s", test.locationString, location.Location())
				}
			}
		})
	}
}
