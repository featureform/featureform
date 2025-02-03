// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package location

import (
	"testing"
)

func TestSQLLocation_TableLocation(t *testing.T) {
	tests := []struct {
		name           string
		database       string
		schema         string
		table          string
		expectedResult string
	}{
		{
			name:           "All components present",
			database:       "test_db",
			schema:         "test_schema",
			table:          "test_table",
			expectedResult: "test_db.test_schema.test_table",
		},
		{
			name:           "Schema and table present, database missing",
			database:       "",
			schema:         "test_schema",
			table:          "test_table",
			expectedResult: "test_schema.test_table",
		},
		{
			name:           "Only table present",
			database:       "",
			schema:         "",
			table:          "test_table",
			expectedResult: "test_table",
		},
		{
			name:           "Database and table present, schema missing",
			database:       "test_db",
			schema:         "",
			table:          "test_table",
			expectedResult: "test_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loc := &SQLLocation{
				database: tt.database,
				schema:   tt.schema,
				table:    tt.table,
			}

			result := loc.TableLocation().String()

			if result != tt.expectedResult {
				t.Errorf("TableLocation() = %v, want %v", result, tt.expectedResult)
			}
		})
	}
}

func TestSQLLocation_AbsoluteRelative(t *testing.T) {
	tests := []struct {
		name       string
		database   string
		schema     string
		table      string
		isAbsolute bool
	}{
		{
			name:       "Fully qualified absolute",
			database:   "test_db",
			schema:     "test_schema",
			table:      "test_table",
			isAbsolute: true,
		},
		{
			name:       "Absolute even without table",
			database:   "test_db",
			schema:     "test_schema",
			table:      "",
			isAbsolute: true,
		},
		{
			name:       "Relative without database",
			database:   "",
			schema:     "test_schema",
			table:      "test_table",
			isAbsolute: false,
		},
		{
			name:       "Relative without schema",
			database:   "test_db",
			schema:     "",
			table:      "test_table",
			isAbsolute: false,
		},
		{
			name:       "Relative without database or schema",
			database:   "",
			schema:     "",
			table:      "test_table",
			isAbsolute: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loc := &SQLLocation{
				database: tt.database,
				schema:   tt.schema,
				table:    tt.table,
			}

			result := loc.IsAbsolute()

			if result != tt.isAbsolute {
				t.Errorf("Location %v, IsAbsolute() = %v, want %v", loc, result, tt.isAbsolute)
			}
		})
	}
}

func TestSQLLocation_GetTableFromRoot(t *testing.T) {
	tests := []struct {
		name     string
		root     SQLLocation
		table    SQLLocation
		expected SQLLocation
	}{
		{
			name: "table is already absolute",
			root: SQLLocation{
				database: "root_database",
				schema:   "root_schema",
				table:    "",
			},
			table: SQLLocation{
				database: "table_database",
				schema:   "table_schema",
				table:    "table",
			},
			expected: SQLLocation{
				database: "table_database",
				schema:   "table_schema",
				table:    "table",
			},
		},
		{
			name: "table is missing database",
			root: SQLLocation{
				database: "root_database",
				schema:   "root_schema",
				table:    "",
			},
			table: SQLLocation{
				database: "",
				schema:   "table_schema",
				table:    "table",
			},
			expected: SQLLocation{
				database: "root_database",
				schema:   "root_schema",
				table:    "table",
			},
		},
		{
			name: "table is missing schema",
			root: SQLLocation{
				database: "root_database",
				schema:   "root_schema",
				table:    "",
			},
			table: SQLLocation{
				database: "table_database",
				schema:   "",
				table:    "table",
			},
			expected: SQLLocation{
				database: "root_database",
				schema:   "root_schema",
				table:    "table",
			},
		},
		{
			name: "table is missing database and schema",
			root: SQLLocation{
				database: "root_database",
				schema:   "root_schema",
				table:    "",
			},
			table: SQLLocation{
				database: "",
				schema:   "",
				table:    "table",
			},
			expected: SQLLocation{
				database: "root_database",
				schema:   "root_schema",
				table:    "table",
			},
		},
		{
			name: "table is missing schema, pulls empty defaults from root",
			root: SQLLocation{
				database: "root_database",
				schema:   "",
				table:    "",
			},
			table: SQLLocation{
				database: "table_database",
				schema:   "",
				table:    "table",
			},
			expected: SQLLocation{
				database: "root_database",
				schema:   "",
				table:    "table",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.root.GetTableFromRoot(&tt.table)

			if *result != tt.expected {
				t.Errorf("Root %v, Table %v, GetTableFromRoot() = %v, want %v", tt.root, tt.table, result, tt.expected)
			}
		})
	}
}
