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

type SortTest struct {
	Sort     query.Sort
	Expected string
}

func TestCompileSort(t *testing.T) {
	tests := map[string]SortTest{
		"key default dir": {
			Sort:     query.KeySort{},
			Expected: "ORDER BY key ASC",
		},
		"json value desc": {
			Sort: query.ValueSort{
				Column: query.JSONColumn{
					Path: []query.JSONPathStep{{Key: "taskID"}},
					Type: query.Int,
				},
				Dir: query.Desc,
			},
			Expected: "ORDER BY (value::json->>'taskID')::int DESC",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			qry, err := compileSort(test.Sort)
			if err != nil {
				t.Fatalf("Failed to compile sort: %v\n%s", test.Sort, err)
			}
			if qry != test.Expected {
				t.Fatalf("SQL not equal\n%s\n%s", qry, test.Expected)
			}
		})
	}
}
