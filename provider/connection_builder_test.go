// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"testing"

	pc "github.com/featureform/provider/provider_config"
	"github.com/stretchr/testify/assert"
)

// Mocked PostgresConfig for testing
func mockPostgresConfig() pc.PostgresConfig {
	return pc.PostgresConfig{
		Username: "test_user",
		Password: "test_password",
		Host:     "localhost",
		Port:     "5432",
		SSLMode:  "disable",
		Database: "default",
	}
}

// TestGetConnectionBuilderFunc verifies that getConnectionBuilderFunc constructs the correct connection URL
func TestGetConnectionBuilderFunc(t *testing.T) {
	tests := []struct {
		name        string
		database    string
		schema      string
		expectedURL string
	}{
		{
			name:        "Default settings",
			database:    "default_db",
			schema:      "",
			expectedURL: "postgres://test_user:test_password@localhost:5432/default_db?sslmode=disable&search_path=%22public%22",
		},
		{
			name:        "Custom database and schema",
			database:    "custom_db",
			schema:      "custom_schema",
			expectedURL: "postgres://test_user:test_password@localhost:5432/custom_db?sslmode=disable&search_path=%22custom_schema%22",
		},
		{
			name:        "Empty schema defaults to public",
			database:    "custom_db",
			schema:      "",
			expectedURL: "postgres://test_user:test_password@localhost:5432/custom_db?sslmode=disable&search_path=%22public%22",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := mockPostgresConfig()
			connectionBuilder := PostgresConnectionBuilderFunc(config)

			actualURL, err := connectionBuilder(tt.database, tt.schema)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedURL, actualURL)
		})
	}
}
