// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package sqlgen

import (
	"testing"

	"github.com/featureform/storage/query"
)

type ColumnTest struct {
	Column   query.Column
	Expected string
}

func TestCompileColumn(t *testing.T) {
	tests := map[string]ColumnTest{
		"simple JSON column": {
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "id"}},
				Type: query.String,
			},
			Expected: "(value::json->>'id')",
		},
		"nested JSON column": {
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "target"}, {Key: "name"}},
				Type: query.String,
			},
			Expected: "(value::json->'target'->>'name')",
		},
		"nested JSON Message column": {
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "name"}},
				Type: query.String,
			},
			Expected: "((value::json->>'Message')::json->>'name')",
		},
		"int cast JSON column": {
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "id"}},
				Type: query.Int,
			},
			Expected: "(value::json->>'id')::int",
		},
		"timestamp cast JSON column": {
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "dateCreated"}},
				Type: query.Timestamp,
			},
			Expected: "(value::json->>'dateCreated')::timestamp",
		},
		"main resource double nested json status": {
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "status"}, {Key: "status"}},
				Type: query.String,
			},
			Expected: "((value::json->>'Message')::json->'status'->>'status')",
		},
		"json encoded string triple nested json object": {
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "status"}, {Key: "statusCode"}, {Key: "statusCodeValue"}},
				Type: query.String,
			},
			Expected: "((value::json->>'Message')::json->'status'->'statusCode'->>'statusCodeValue')",
		},
		"triple nested json object": {
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "target"}, {Key: "status"}, {Key: "statusCode"}, {Key: "statusCodeValue"}},
				Type: query.String,
			},
			Expected: "(value::json->'target'->'status'->'statusCode'->>'statusCodeValue')",
		},
		"inner nested json encoded string": {
			Column: query.JSONColumn{
				Path: []query.JSONPathStep{{Key: "target"}, {Key: "status"}, {Key: "statusCode", IsJsonString: true}, {Key: "statusCodeValue"}},
				Type: query.String,
			},
			Expected: "((value::json->'target'->'status'->>'statusCode')::json->>'statusCodeValue')",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			qry, err := compileColumn(test.Column)
			if err != nil {
				t.Fatalf("Failed to compile column: %v\n%s", test.Column, err)
			}
			if qry != test.Expected {
				t.Fatalf("SQL not equal\n%s\n%s", qry, test.Expected)
			}
		})
	}
}

type MultiColumnTest struct {
	Columns  []query.Column
	Expected string
}

func TestCompileMultiColumns(t *testing.T) {
	tests := map[string]MultiColumnTest{
		"single computed column with alias": {
			Columns: []query.Column{
				query.SQLColumn{
					Column: "(json_array_elements((VALUE::json->>'Message')::json->'tags'->'tag'))::text",
					Alias:  "tag",
				},
			},
			Expected: "(json_array_elements((VALUE::json->>'Message')::json->'tags'->'tag'))::text AS tag",
		},
		"count column with alias": {
			Columns: []query.Column{
				query.SQLColumn{
					Column: "COUNT(*)",
					Alias:  "total_count",
				},
			},
			Expected: "COUNT(*) AS total_count",
		},
		"multiple columns with aliases": {
			Columns: []query.Column{
				query.SQLColumn{
					Column: "(json_array_elements((VALUE::json->>'Message')::json->'tags'->'tag'))::text",
					Alias:  "tag",
				},
				query.SQLColumn{
					Column: "COUNT(*)",
					Alias:  "total_count",
				},
			},
			Expected: "(json_array_elements((VALUE::json->>'Message')::json->'tags'->'tag'))::text AS tag, COUNT(*) AS total_count",
		},
		"default column without alias": {
			Columns: []query.Column{
				query.SQLColumn{
					Column: "default_column",
				},
			},
			Expected: "default_column",
		},
		"default column with alias": {
			Columns: []query.Column{
				query.SQLColumn{
					Column: "default_column",
					Alias:  "alias_column",
				},
			},
			Expected: "default_column AS alias_column",
		},
		"multiple SQL columns with & without aliases": {
			Columns: []query.Column{
				query.SQLColumn{
					Column: "key",
				},
				query.SQLColumn{
					Column: "value",
				},
				query.SQLColumn{
					Column: "newColumn1",
					Alias:  "additional1",
				},
				query.SQLColumn{
					Column: "newColumn2",
					Alias:  "additional2",
				},
			},
			Expected: "key, value, newColumn1 AS additional1, newColumn2 AS additional2",
		},
		"columns with no entries returns default key, value": {
			Columns:  []query.Column{},
			Expected: "key, value",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			list := &List{
				Columns: test.Columns,
			}
			result, err := list.compileColumns(false)
			if err != nil {
				t.Fatalf("%s: Failed to compile columns: %v\n%s", name, test.Columns, err)
			}
			if result != test.Expected {
				t.Errorf("%s: SQL not equal\nFound: %s\nExpected: %s", name, result, test.Expected)
			}
		})
	}
}
