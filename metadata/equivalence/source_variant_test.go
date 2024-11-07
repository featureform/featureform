// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package equivalence

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrimaryDataIsEquivalent(t *testing.T) {
	tests := []struct {
		name     string
		pd1      primaryData
		pd2      Equivalencer
		expected bool
	}{
		{
			name: "Identical primaryData",
			pd1: primaryData{
				Location: &sqlTable{
					Name:     "table1",
					Database: "db1",
					Schema:   "public",
				},
				TimestampColumn: "timestamp1",
			},
			pd2: primaryData{
				Location: &sqlTable{
					Name:     "table1",
					Database: "db1",
					Schema:   "public",
				},
				TimestampColumn: "timestamp1",
			},
			expected: true,
		},
		{
			name: "Different Locations",
			pd1: primaryData{
				Location: &sqlTable{
					Name:     "table1",
					Database: "db1",
					Schema:   "public",
				},
				TimestampColumn: "timestamp1",
			},
			pd2: primaryData{
				Location: &sqlTable{
					Name:     "table2",
					Database: "db1",
					Schema:   "public",
				},
				TimestampColumn: "timestamp1",
			},
			expected: false,
		},
		{
			name: "Different TimestampColumns",
			pd1: primaryData{
				Location: &sqlTable{
					Name:     "table1",
					Database: "db1",
					Schema:   "public",
				},
				TimestampColumn: "timestamp1",
			},
			pd2: primaryData{
				Location: &sqlTable{
					Name:     "table1",
					Database: "db1",
					Schema:   "public",
				},
				TimestampColumn: "timestamp2",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.pd1.IsEquivalent(tt.pd2)
			assert.Equal(t, tt.expected, result, "IsEquivalent() mismatch in test case: %s", tt.name)
		})
	}
}

func TestTransformationIsEquivalent(t *testing.T) {
	tests := []struct {
		name     string
		t1       transformation
		t2       Equivalencer
		expected bool
	}{
		{
			name: "Identical transformations",
			t1: transformation{
				Type: sqlTransformation{Query: "SELECT * FROM table"},
				Args: kubernetesArgs{image: "my-image:latest"},
			},
			t2: transformation{
				Type: sqlTransformation{Query: "SELECT * FROM table"},
				Args: kubernetesArgs{image: "my-image:latest"},
			},
			expected: true,
		},
		{
			name: "Different Args",
			t1: transformation{
				Type: sqlTransformation{Query: "SELECT * FROM table"},
				Args: kubernetesArgs{image: "my-image:latest"},
			},
			t2: transformation{
				Type: sqlTransformation{Query: "SELECT * FROM table"},
				Args: kubernetesArgs{image: "my-image:v2"}, // Different image
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.t1.IsEquivalent(tt.t2)
			assert.Equal(t, tt.expected, result, "IsEquivalent() mismatch in test case: %s", tt.name)
		})
	}
}

func TestSourceVariantIsEquivalent(t *testing.T) {
	tests := []struct {
		name     string
		sv1      sourceVariant
		sv2      Equivalencer
		expected bool
	}{
		{
			name: "Identical sourceVariants",
			sv1: sourceVariant{
				Name:       "variant1",
				Definition: primaryData{Location: &sqlTable{Name: "table1"}},
				Provider:   "provider1",
			},
			sv2: sourceVariant{
				Name:       "variant1",
				Definition: primaryData{Location: &sqlTable{Name: "table1"}},
				Provider:   "provider1",
			},
			expected: true,
		},
		{
			name: "Different Names",
			sv1: sourceVariant{
				Name:       "variant1",
				Definition: primaryData{Location: &sqlTable{Name: "table1"}},
				Provider:   "provider1",
			},
			sv2: sourceVariant{
				Name:       "variant2", // Different name
				Definition: primaryData{Location: &sqlTable{Name: "table1"}},
				Provider:   "provider1",
			},
			expected: false,
		},
		{
			name: "Different Definitions",
			sv1: sourceVariant{
				Name:       "variant1",
				Definition: primaryData{Location: &sqlTable{Name: "table1"}},
				Provider:   "provider1",
			},
			sv2: sourceVariant{
				Name:       "variant1",
				Definition: primaryData{Location: &sqlTable{Name: "table2"}}, // Different definition
				Provider:   "provider1",
			},
			expected: false,
		},
		{
			name: "Different Providers",
			sv1: sourceVariant{
				Name:       "variant1",
				Definition: primaryData{Location: &sqlTable{Name: "table1"}},
				Provider:   "provider1",
			},
			sv2: sourceVariant{
				Name:       "variant1",
				Definition: primaryData{Location: &sqlTable{Name: "table1"}},
				Provider:   "provider2", // Different provider
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.sv1.IsEquivalent(tt.sv2)
			assert.Equal(t, tt.expected, result, "IsEquivalent() mismatch in test case: %s", tt.name)
		})
	}
}

func TestDfTransformationIsEquivalent(t *testing.T) {
	tests := []struct {
		name     string
		df1      dfTransformation
		df2      dfTransformation
		expected bool
	}{
		{
			name: "Identical transformations",
			df1: dfTransformation{
				CanonicalFuncText: "SELECT * FROM table",
				Inputs: []nameVariant{
					{Name: "input1", Variant: "v1"},
					{Name: "input2", Variant: "v1"},
				},
				IncrementalSources: []nameVariant{
					{Name: "inc_input1", Variant: "v1"},
				},
			},
			df2: dfTransformation{
				CanonicalFuncText: "SELECT * FROM table",
				Inputs: []nameVariant{
					{Name: "input1", Variant: "v1"},
					{Name: "input2", Variant: "v1"},
				},
				IncrementalSources: []nameVariant{
					{Name: "inc_input1", Variant: "v1"},
				},
			},
			expected: true,
		},
		{
			name: "Different Inputs order",
			df1: dfTransformation{
				CanonicalFuncText: "SELECT * FROM table",
				Inputs: []nameVariant{
					{Name: "input1", Variant: "v1"},
					{Name: "input2", Variant: "v1"},
				},
				IncrementalSources: []nameVariant{
					{Name: "inc_input1", Variant: "v1"},
				},
			},
			df2: dfTransformation{
				CanonicalFuncText: "SELECT * FROM table",
				Inputs: []nameVariant{
					{Name: "input2", Variant: "v1"}, // Different order
					{Name: "input1", Variant: "v1"},
				},
				IncrementalSources: []nameVariant{
					{Name: "inc_input1", Variant: "v1"},
				},
			},
			expected: true, // Assuming order doesn't matter
		},
		{
			name: "Different SourceText",
			df1: dfTransformation{
				CanonicalFuncText: "SELECT * FROM table1",
				Inputs: []nameVariant{
					{Name: "input1", Variant: "v1"},
				},
				IncrementalSources: []nameVariant{
					{Name: "inc_input1", Variant: "v1"},
				},
			},
			df2: dfTransformation{
				CanonicalFuncText: "SELECT * FROM table2", // Different SourceText
				Inputs: []nameVariant{
					{Name: "input1", Variant: "v1"},
				},
				IncrementalSources: []nameVariant{
					{Name: "inc_input1", Variant: "v1"},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.df1.IsEquivalent(tt.df2)
			assert.Equal(t, tt.expected, result, "IsEquivalent() mismatch in test case: %s", tt.name)
		})
	}
}

func TestSqlTransformationIsEquivalent(t *testing.T) {
	tests := []struct {
		name     string
		sql1     sqlTransformation
		sql2     sqlTransformation
		expected bool
	}{
		{
			name: "Identical transformations",
			sql1: sqlTransformation{
				Sources: []nameVariant{
					{Name: "source1", Variant: "v1"},
					{Name: "source2", Variant: "v1"},
				},
			},
			sql2: sqlTransformation{
				Sources: []nameVariant{
					{Name: "source1", Variant: "v1"},
					{Name: "source2", Variant: "v1"},
				},
			},
			expected: true,
		},
		{
			name: "Different Query strings",
			sql1: sqlTransformation{
				Query: "SELECT * FROM table1",
				Sources: []nameVariant{
					{Name: "source1", Variant: "v1"},
				},
			},
			sql2: sqlTransformation{
				Query: "SELECT * FROM table2", // Different Query
				Sources: []nameVariant{
					{Name: "source1", Variant: "v1"},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.sql1.IsEquivalent(tt.sql2)
			assert.Equal(t, tt.expected, result, "IsEquivalent() mismatch in test case: %s", tt.name)
		})
	}
}

func TestSqlTableIsEquivalent(t *testing.T) {
	tests := []struct {
		name     string
		table1   *sqlTable
		table2   Equivalencer
		expected bool
	}{
		{
			name: "Identical sqlTables",
			table1: &sqlTable{
				Name:     "users",
				Database: "db1",
				Schema:   "public",
			},
			table2: &sqlTable{
				Name:     "users",
				Database: "db1",
				Schema:   "public",
			},
			expected: true,
		},
		{
			name: "Different Names",
			table1: &sqlTable{
				Name:     "users",
				Database: "db1",
				Schema:   "public",
			},
			table2: &sqlTable{
				Name:     "customers", // Different Name
				Database: "db1",
				Schema:   "public",
			},
			expected: false,
		},
		{
			name: "Different Databases",
			table1: &sqlTable{
				Name:     "users",
				Database: "db1",
				Schema:   "public",
			},
			table2: &sqlTable{
				Name:     "users",
				Database: "db2", // Different database
				Schema:   "public",
			},
			expected: false,
		},
		{
			name: "Different Schemas",
			table1: &sqlTable{
				Name:     "users",
				Database: "db1",
				Schema:   "public",
			},
			table2: &sqlTable{
				Name:     "users",
				Database: "db1",
				Schema:   "private", // Different schema
			},
			expected: false,
		},
		{
			name: "Different Types",
			table1: &sqlTable{
				Name:     "users",
				Database: "db1",
				Schema:   "public",
			},
			table2: &fileStoreTable{ // Different type
				Path: "/data/users",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.table1.IsEquivalent(tt.table2)
			assert.Equal(t, tt.expected, result, "IsEquivalent() mismatch in test case: %s", tt.name)
		})
	}
}

func TestFileStoreTableIsEquivalent(t *testing.T) {
	tests := []struct {
		name     string
		table1   *fileStoreTable
		table2   Equivalencer
		expected bool
	}{
		{
			name: "Identical fileStoreTables",
			table1: &fileStoreTable{
				Path: "/data/users",
			},
			table2: &fileStoreTable{
				Path: "/data/users",
			},
			expected: true,
		},
		{
			name: "Different Paths",
			table1: &fileStoreTable{
				Path: "/data/users",
			},
			table2: &fileStoreTable{
				Path: "/data/customers", // Different path
			},
			expected: false,
		},
		{
			name: "Different Types",
			table1: &fileStoreTable{
				Path: "/data/users",
			},
			table2: &sqlTable{
				Name:     "users",
				Database: "db1",
				Schema:   "public",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.table1.IsEquivalent(tt.table2)
			assert.Equal(t, tt.expected, result, "IsEquivalent() mismatch in test case: %s", tt.name)
		})
	}
}

func TestCatalogTableIsEquivalent(t *testing.T) {
	tests := []struct {
		name     string
		table1   *catalogTable
		table2   Equivalencer
		expected bool
	}{
		{
			name: "Identical catalogTables",
			table1: &catalogTable{
				Database:    "db1",
				Table:       "users",
				TableFormat: "parquet",
			},
			table2: &catalogTable{
				Database:    "db1",
				Table:       "users",
				TableFormat: "parquet",
			},
			expected: true,
		},
		{
			name: "Different Databases",
			table1: &catalogTable{
				Database:    "db1",
				Table:       "users",
				TableFormat: "parquet",
			},
			table2: &catalogTable{
				Database:    "db2", // Different database
				Table:       "users",
				TableFormat: "parquet",
			},
			expected: false,
		},
		{
			name: "Different Tables",
			table1: &catalogTable{
				Database:    "db1",
				Table:       "users",
				TableFormat: "parquet",
			},
			table2: &catalogTable{
				Database:    "db1",
				Table:       "customers", // Different table
				TableFormat: "parquet",
			},
			expected: false,
		},
		{
			name: "Different TableFormats",
			table1: &catalogTable{
				Database:    "db1",
				Table:       "users",
				TableFormat: "parquet",
			},
			table2: &catalogTable{
				Database:    "db1",
				Table:       "users",
				TableFormat: "orc", // Different format
			},
			expected: false,
		},
		{
			name: "Different Types",
			table1: &catalogTable{
				Database:    "db1",
				Table:       "users",
				TableFormat: "parquet",
			},
			table2: &fileStoreTable{
				Path: "/data/users",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.table1.IsEquivalent(tt.table2)
			assert.Equal(t, tt.expected, result, "IsEquivalent() mismatch in test case: %s", tt.name)
		})
	}
}

func TestKubernetesArgsIsEquivalent(t *testing.T) {
	tests := []struct {
		name     string
		args1    kubernetesArgs
		args2    Equivalencer
		expected bool
	}{
		{
			name: "Identical kubernetesArgs",
			args1: kubernetesArgs{
				image:         "my-image:latest",
				CPURequest:    "500m",
				CPULimit:      "1",
				MemoryRequest: "512Mi",
				MemoryLimit:   "1Gi",
			},
			args2: kubernetesArgs{
				image:         "my-image:latest",
				CPURequest:    "500m",
				CPULimit:      "1",
				MemoryRequest: "512Mi",
				MemoryLimit:   "1Gi",
			},
			expected: true,
		},
		{
			name: "Different Images",
			args1: kubernetesArgs{
				image:         "my-image:latest",
				CPURequest:    "500m",
				CPULimit:      "1",
				MemoryRequest: "512Mi",
				MemoryLimit:   "1Gi",
			},
			args2: kubernetesArgs{
				image:         "my-image:v2", // Different image
				CPURequest:    "500m",
				CPULimit:      "1",
				MemoryRequest: "512Mi",
				MemoryLimit:   "1Gi",
			},
			expected: false,
		},
		{
			name: "Different CPU Requests",
			args1: kubernetesArgs{
				image:         "my-image:latest",
				CPURequest:    "500m",
				CPULimit:      "1",
				MemoryRequest: "512Mi",
				MemoryLimit:   "1Gi",
			},
			args2: kubernetesArgs{
				image:         "my-image:latest",
				CPURequest:    "250m", // Different CPURequest
				CPULimit:      "1",
				MemoryRequest: "512Mi",
				MemoryLimit:   "1Gi",
			},
			expected: false,
		},
		{
			name: "Different Memory Limits",
			args1: kubernetesArgs{
				image:         "my-image:latest",
				CPURequest:    "500m",
				CPULimit:      "1",
				MemoryRequest: "512Mi",
				MemoryLimit:   "1Gi",
			},
			args2: kubernetesArgs{
				image:         "my-image:latest",
				CPURequest:    "500m",
				CPULimit:      "1",
				MemoryRequest: "512Mi",
				MemoryLimit:   "2Gi", // Different MemoryLimit
			},
			expected: false,
		},
		{
			name: "Different Types",
			args1: kubernetesArgs{
				image:         "my-image:latest",
				CPURequest:    "500m",
				CPULimit:      "1",
				MemoryRequest: "512Mi",
				MemoryLimit:   "1Gi",
			},
			args2: &sqlTable{
				Name:     "users",
				Database: "db1",
				Schema:   "public",
			},
			expected: false,
		},
		{
			name: "One Nil, One Non-Nil",
			args1: kubernetesArgs{
				image:         "my-image:latest",
				CPURequest:    "500m",
				CPULimit:      "1",
				MemoryRequest: "512Mi",
				MemoryLimit:   "1Gi",
			},
			args2:    nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.args1.IsEquivalent(tt.args2)
			assert.Equal(t, tt.expected, result, "IsEquivalent() mismatch in test case: %s", tt.name)
		})
	}
}

// TestIsSqlEqual tests the isSqlEqual function.
func TestIsSqlEqual(t *testing.T) {
	tests := []struct {
		name     string
		thisSql  string
		otherSql string
		want     bool
	}{
		{
			name:     "equal SQL with single spaces",
			thisSql:  "SELECT * FROM table",
			otherSql: "SELECT * FROM table",
			want:     true,
		},
		{
			name:     "equal SQL with multiple spaces",
			thisSql:  "SELECT  *  FROM  table",
			otherSql: "SELECT * FROM table",
			want:     true,
		},
		{
			name:     "different SQL statements",
			thisSql:  "SELECT * FROM table1",
			otherSql: "SELECT * FROM table2",
			want:     false,
		},
		{
			name:     "equal SQL with different whitespace",
			thisSql:  "SELECT   *    FROM table",
			otherSql: "SELECT * FROM   table",
			want:     true,
		},
		{
			name:     "SQL with leading/trailing whitespace",
			thisSql:  "  SELECT * FROM table  ",
			otherSql: "SELECT * FROM table",
			want:     true,
		},
		{
			name:     "SQL with tabs and newlines",
			thisSql:  "SELECT\t*\nFROM table",
			otherSql: "SELECT * FROM table",
			want:     true,
		},
		{
			name:     "SQL with mixed whitespace types",
			thisSql:  "SELECT\t  *\n  FROM  \ttable",
			otherSql: "SELECT * FROM table",
			want:     true,
		},
		{
			name:     "SQL with leading and trailing mixed whitespace",
			thisSql:  "\t  SELECT * FROM table  \n",
			otherSql: "SELECT * FROM table",
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isSqlEqual(tt.thisSql, tt.otherSql); got != tt.want {
				t.Errorf("isSqlEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}
