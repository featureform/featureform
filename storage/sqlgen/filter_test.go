// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package sqlgen

import (
	"errors"
	"reflect"
	"testing"

	"github.com/featureform/fferr"
	"github.com/featureform/storage/query"
)

type MultiFilterTest struct {
	Filters      []query.Query
	Expected     string
	ExpectedArgs []any
}

func TestCompileFilters(t *testing.T) {
	tests := map[string]MultiFilterTest{
		"Nil filters": {
			Filters:      nil,
			Expected:     "",
			ExpectedArgs: []any{},
		},
		"No filters": {
			Filters:      []query.Query{},
			Expected:     "",
			ExpectedArgs: []any{},
		},
		"One filter": {
			Filters: []query.Query{
				query.KeyPrefix{Prefix: "FEATURE__"},
			},
			Expected:     "WHERE key LIKE $1",
			ExpectedArgs: []any{"FEATURE__%"},
		},
		"Many filters": {
			Filters: []query.Query{
				query.KeyPrefix{Not: true, Prefix: "FEATURE__VARIANT__"},
				query.KeyPrefix{Prefix: "FEATURE__"},
				query.ValueEquals{
					Column: query.JSONColumn{
						Path: []query.JSONPathStep{{Key: "target"}, {Key: "name"}},
						Type: query.String,
					},
					Value: "transactions",
				},
				query.ValueIn{
					Column: query.JSONColumn{
						Path: []query.JSONPathStep{{Key: "statusInt"}},
						Type: query.Int,
					},
					Values: []any{2, 5},
				},
				query.ValueEquals{
					Column: query.JSONColumn{
						Path: []query.JSONPathStep{{Key: "status"}},
						Type: query.String,
					},
					Value: "READY",
				},
				query.ConditionalOR{
					Filters: []query.Query{query.ValueEquals{
						Column: query.JSONColumn{
							Path: []query.JSONPathStep{{Key: "SerializedVersion"}},
							Type: query.String,
						},
						Value: "1",
					},
						query.ValueLike{
							Column: query.JSONColumn{
								Path: []query.JSONPathStep{{Key: "target"}, {Key: "otherCol"}},
								Type: query.String,
							},
							Value: "random",
						},
					},
				},
				query.ObjectArrayContains{
					Column: query.JSONColumn{
						Path: []query.JSONPathStep{{Key: "firstStep"}, {Key: "secondStep", IsJsonString: true}, {Key: "thirdStep"}},
						Type: query.Object,
					},
					Values:      []any{"testValue"},
					SearchField: "final",
				},
			},
			Expected: "WHERE key NOT LIKE $1 AND key LIKE $2 AND (value::json->'target'->>'name') = $3 AND (value::json->>'statusInt')::int IN ($4,$5) AND (value::json->>'status') = $6 AND ((value::json->>'SerializedVersion') = $7 OR (value::json->'target'->>'otherCol') like $8) AND EXISTS (SELECT 1 FROM jsonb_array_elements((value::json->'firstStep'->>'secondStep')::jsonb->'thirdStep') AS elem WHERE elem->>'final' IN ($9))",
			ExpectedArgs: []any{
				"FEATURE__VARIANT__%",
				"FEATURE__%",
				"transactions",
				2, 5,
				"READY",
				"1",
				"%random%",
				"testValue",
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			qry, args, err := compileFilters(test.Filters)
			if err != nil {
				t.Fatalf("%s: Failed to compile sort: %v\n%s", name, test.Filters, err)
			}
			if !reflect.DeepEqual(test.ExpectedArgs, args) {
				t.Errorf("Args not equal\nFound: %v\nExpected: %v", args, test.ExpectedArgs)
			}
			if qry != test.Expected {
				t.Errorf("SQL not equal\n%s\n%s", qry, test.Expected)
			}
		})
	}
}

type SingleFilterTest struct {
	Filter      query.Query
	Expected    string
	ExpectedArg []any
	ExpectedErr error
}

func TestCompileFilter(t *testing.T) {
	tests := map[string]SingleFilterTest{
		"key prefix like": {
			Filter:      query.KeyPrefix{Prefix: "FEATURE__"},
			Expected:    "key LIKE $1",
			ExpectedArg: []any{"FEATURE__%"},
		},
		"key prefix not like": {
			Filter:      query.KeyPrefix{Not: true, Prefix: "LABEL__"},
			Expected:    "key NOT LIKE $1",
			ExpectedArg: []any{"LABEL__%"},
		},
		"JSON value equals": {
			Filter: query.ValueEquals{
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "target"}, {Key: "name"}},
					Type: query.String,
				},
				Value: "READY",
			},
			Expected:    "(value::json->'target'->>'name') = $1",
			ExpectedArg: []any{"READY"},
		},
		"JSON values equals NOT NULL": {
			Filter: query.ValueEquals{
				Not: true,
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "primaryData"}},
					Type: query.String,
				},
				Value: "NULL",
			},
			Expected:    "((value::json->>'Message')::json->>'primaryData') IS NOT NULL",
			ExpectedArg: []any{},
		},
		"JSON values equals NOT NULL sql transformation": {
			Filter: query.ValueEquals{
				Not: true,
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "transformation", IsJsonString: true}, {Key: "SQLTransformation", IsJsonString: true}},
					Type: query.String,
				},
				Value: "NULL",
			},
			Expected:    "(((value::json->>'Message')::json->>'transformation')::json->>'SQLTransformation') IS NOT NULL",
			ExpectedArg: []any{},
		},
		"JSON values equals NOT NULL df transformation": {
			Filter: query.ValueEquals{
				Not: true,
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "transformation", IsJsonString: true}, {Key: "DFTransformation", IsJsonString: true}},
					Type: query.String,
				},
				Value: "NULL",
			},
			Expected:    "(((value::json->>'Message')::json->>'transformation')::json->>'DFTransformation') IS NOT NULL",
			ExpectedArg: []any{},
		},
		"JSON value like": {
			Filter: query.ValueLike{
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "target"}, {Key: "name"}},
					Type: query.String,
				},
				Value: "Partial",
			},
			Expected:    "(value::json->'target'->>'name') like $1",
			ExpectedArg: []any{"%Partial%"},
		},
		"JSON value in": {
			Filter: query.ValueIn{
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "status"}},
					Type: query.Int,
				},
				Values: []any{2, 5},
			},
			Expected:    "(value::json->>'status')::int IN ($1,$2)",
			ExpectedArg: []any{2, 5},
		},
		"JSON serialized version": {
			Filter: query.ValueEquals{
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "SerializedVersion"}},
					Type: query.String,
				},
				Value: "1",
			},
			Expected:    "(value::json->>'SerializedVersion') = $1",
			ExpectedArg: []any{"1"},
		},
		"JSON Array Contains single entry": {
			Filter: query.ArrayContains{
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "tags"}, {Key: "tag"}},
					Type: query.String,
				},
				Values: []any{"crazy"},
			},
			Expected:    "(((value::json->>'Message')::json->'tags'->>'tag'))::jsonb ?| array[$1]",
			ExpectedArg: []any{"crazy"},
		},
		"JSON Array Contains multiple entries": {
			Filter: query.ArrayContains{
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "tags"}, {Key: "tag"}},
					Type: query.String,
				},
				Values: []any{"test", "v1", "crazy"},
			},
			Expected:    "(((value::json->>'Message')::json->'tags'->>'tag'))::jsonb ?| array[$1, $2, $3]",
			ExpectedArg: []any{"test", "v1", "crazy"},
		},
		"JSON Object Array Contains": {
			Filter: query.ObjectArrayContains{
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "labels"}},
					Type: query.Object,
				},
				Values:      []any{"test"},
				SearchField: "name",
			},
			Expected:    "EXISTS (SELECT 1 FROM jsonb_array_elements((value::json->>'Message')::jsonb->'labels') AS elem WHERE elem->>'name' IN ($1))",
			ExpectedArg: []any{"test"},
		},
		"JSON similar to with no entries returns error": {
			Filter: query.ArrayContains{
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "Message", IsJsonString: true}, {Key: "tags"}, {Key: "tag"}},
					Type: query.String,
				},
				Values: []any{},
			},
			Expected:    "",
			ExpectedArg: []any{},
			ExpectedErr: fferr.NewInternalError(errors.New("Cannot compile Array Contains with an empty values array")),
		},
		"JSON Multiple conditionals with OR": {
			Filter: query.ConditionalOR{
				Filters: []query.Query{query.ValueEquals{
					Column: query.JSONColumn{
						Path: []query.JSONPathStep{{Key: "SerializedVersion"}},
						Type: query.String,
					},
					Value: "1",
				},
					query.ValueLike{
						Column: query.JSONColumn{
							Path: []query.JSONPathStep{{Key: "target"}, {Key: "name"}},
							Type: query.String,
						},
						Value: "Partial",
					},
				},
			},
			Expected:    "((value::json->>'SerializedVersion') = $1 OR (value::json->'target'->>'name') like $2)",
			ExpectedArg: []any{"1", "%Partial%"},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			qry, arg, err := compileFilter(test.Filter, 1)
			if err != nil && err.Error() != test.ExpectedErr.Error() {
				t.Fatalf("Failed to compile filter: %v\n%s", test.Filter, err)
			}

			if len(test.ExpectedArg) == 0 && len(arg) == 0 {
				// skip deepEqual in cases where expArg and Arg length is zero
			} else if !reflect.DeepEqual(test.ExpectedArg, arg) {
				t.Fatalf("Args not equal\nFound: %v\nExpected: %v", arg, test.ExpectedArg)
			}
			if qry != test.Expected {
				t.Fatalf("SQL not equal\nFound: %s\nExpected: %s", qry, test.Expected)
			}
		})
	}
}
