// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package provider

import (
	"testing"

	"github.com/featureform/metadata"
)

func TestGenericInsertQuery(t *testing.T) {
	tests := []struct {
		name     string
		location string
		columns  []string
		expected string
	}{
		{
			name:     "Test Generic Insert Query",
			location: "test_table",
			columns:  []string{"col1", "col2"},
			expected: `INSERT INTO "test_table" (col1, col2) VALUES (?, ?), (?, ?)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := snowflakeSQLQueries{}.genericInsert(tt.location, tt.columns, 2)
			if actual != tt.expected {
				t.Errorf("Expected %v, but instead found %v", tt.expected, actual)
			}

		})
	}

}

func TestSnowflakeDynamicIcebergTableQuery(t *testing.T) {
	tests := []struct {
		name     string
		table    string
		query    string
		config   metadata.ResourceSnowflakeConfig
		expected string
	}{
		{
			name:  "Test Dynamic Iceberg Table Query",
			table: "test_table",
			query: "SELECT * FROM raw_table",
			config: metadata.ResourceSnowflakeConfig{
				DynamicTableConfig: &metadata.SnowflakeDynamicTableConfig{
					ExternalVolume: "s3://my-bucket",
					BaseLocation:   "/base",
					TargetLag:      "1 hours",
					RefreshMode:    metadata.AutoRefresh,
					Initialize:     metadata.InitializeOnCreate,
				},
				Warehouse: "my_warehouse",
			},
			expected: "CREATE OR REPLACE DYNAMIC ICEBERG TABLE \"test_table\" TARGET_LAG = '1 hours' WAREHOUSE = 'my_warehouse' EXTERNAL_VOLUME = 's3://my-bucket' CATALOG = 'SNOWFLAKE' BASE_LOCATION = '/base' REFRESH_MODE = AUTO INITIALIZE = ON_CREATE AS SELECT * FROM raw_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := snowflakeSQLQueries{}.dynamicIcebergTableCreate(tt.table, tt.query, tt.config)

			if actual != tt.expected {
				t.Errorf("Expected %v, but instead found %v", tt.expected, actual)
			}

		})
	}

}
