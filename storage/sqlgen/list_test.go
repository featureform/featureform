// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package sqlgen

import (
	"reflect"
	"testing"

	"github.com/featureform/provider/provider_type"
	"github.com/featureform/storage/query"
)

type ListTest struct {
	Opts         []query.Query
	Columns      []query.Column
	CountOnly    bool
	Expected     string
	ExpectedArgs []any
}

func TestCompileLists(t *testing.T) {
	tests := map[string]ListTest{
		"nil opts": {
			Opts:         nil,
			Expected:     "SELECT key, value FROM table",
			ExpectedArgs: []any{},
		},
		"no opts": {
			Opts:         []query.Query{},
			Expected:     "SELECT key, value FROM table",
			ExpectedArgs: []any{},
		},
		"count": {
			Opts:         []query.Query{},
			CountOnly:    true,
			Expected:     "SELECT COUNT(*) FROM table",
			ExpectedArgs: []any{},
		},
		"status types": {
			Opts: []query.Query{query.ValueIn{
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "type"}},
					Type: query.String,
				},
				Values: []any{provider_type.PostgresOffline, provider_type.AZURE}}},
			CountOnly:    true,
			Expected:     "SELECT COUNT(*) FROM table WHERE ((value::json->>'Message')::json->>'type') IN ($1,$2)",
			ExpectedArgs: []any{provider_type.PostgresOffline, provider_type.AZURE},
		},
		"complex query": {
			Opts: []query.Query{
				query.KeyPrefix{Not: true, Prefix: "FEATURE__VARIANT__"},
				query.KeyPrefix{Prefix: "FEATURE__"},
				query.ValueEquals{
					Column: query.JSONColumn{
						Path: []query.JSONPathStep{{Key: "target"}, {Key: "name"}},
						Type: query.String,
					},
					Value: "transactions",
				},
				query.ValueEquals{
					Column: query.JSONColumn{
						Path: []query.JSONPathStep{{Key: "status"}},
						Type: query.String,
					},
					Value: "READY",
				},
				query.ValueSort{
					Column: query.JSONColumn{
						Path: []query.JSONPathStep{{Key: "taskID"}},
						Type: query.Int,
					},
					Dir: query.Desc,
				},
				query.Limit{
					Limit:  25,
					Offset: 50,
				},
			},
			Expected: "SELECT key, value FROM table WHERE key NOT LIKE $1 AND key LIKE $2 AND (value::json->'target'->>'name') = $3 AND (value::json->>'status') = $4 ORDER BY (value::json->>'taskID')::int DESC LIMIT 25 OFFSET 50",
			ExpectedArgs: []any{
				"FEATURE__VARIANT__%",
				"FEATURE__%",
				"transactions",
				"READY",
			},
		},
		"complex query via columns": {
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
			Opts: []query.Query{
				query.GroupBy{
					Name: "tag",
				},
				query.ValueSort{
					Column: query.SQLColumn{
						Column: "total_count",
					},
					Dir: query.Desc,
				},
				query.ValueEquals{
					Column: query.JSONColumn{
						Path: []query.JSONPathStep{{Key: "SerializedVersion"}},
						Type: query.String,
					},
					Value: "1",
				},
				query.Limit{
					Limit: 8,
				},
			},
			Expected: "SELECT (json_array_elements((VALUE::json->>'Message')::json->'tags'->'tag'))::text AS tag, COUNT(*) AS total_count FROM table WHERE (value::json->>'SerializedVersion') = $1 group by tag ORDER BY total_count DESC LIMIT 8",
			ExpectedArgs: []any{
				"1",
			},
		},
		"complex query via columns and uses array contains": {
			Columns: []query.Column{
				query.SQLColumn{
					Column: "key",
					Alias:  "usuaKey",
				},
				query.SQLColumn{
					Column: "(value::json->>'Message')::json->>'name'",
					Alias:  "myNameColumn",
				},
			},
			Opts: []query.Query{
				query.ValueEquals{
					Column: query.JSONColumn{
						Path: []query.JSONPathStep{{Key: "SerializedVersion"}},
						Type: query.String,
					},
					Value: "1",
				},
				query.Limit{
					Limit: 8,
				},
				query.ArrayContains{
					Column: query.JSONColumn{
						Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "tags"}, {Key: "tag"}},
						Type: query.String,
					},
					Values: []any{"myTag", "yourTag"},
				},
			},
			Expected: "SELECT key AS usuaKey, (value::json->>'Message')::json->>'name' AS myNameColumn FROM table WHERE (value::json->>'SerializedVersion') = $1 AND (((value::json->>'Message')::json->'tags'->>'tag'))::jsonb ?| array[$2, $3] LIMIT 8",
			ExpectedArgs: []any{
				"1",
				"myTag",
				"yourTag",
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			list, err := NewListQuery("table", test.Opts, test.Columns...)
			if err != nil {
				t.Fatalf("%s:Failed to compile list: %v\n%s", name, test.Opts, err)
			}
			var qry string
			var args []any
			var compileErr error
			if test.CountOnly {
				qry, args, compileErr = list.CompileCount()
			} else {
				qry, args, compileErr = list.Compile()
			}
			if compileErr != nil {
				t.Fatalf("%s:Failed to compile list query: %v\n%s", name, list, compileErr)
			}
			if !reflect.DeepEqual(test.ExpectedArgs, args) {
				t.Errorf("%s:Args not equal\nFound: %v\nExpected: %v", name, args, test.ExpectedArgs)
			}
			if qry != test.Expected {
				t.Errorf("%s:SQL not equal\n%s\n%s", name, qry, test.Expected)
			}
		})
	}
}
