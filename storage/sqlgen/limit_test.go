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

type LimitTest struct {
	Limit    query.Limit
	Expected string
}

func TestCompileLimit(t *testing.T) {
	tests := map[string]LimitTest{
		"no limit or offset": {
			Limit:    query.Limit{},
			Expected: "",
		},
		"limit, no offset": {
			Limit:    query.Limit{Limit: 10},
			Expected: "LIMIT 10",
		},
		"offset, no limit": {
			Limit:    query.Limit{Offset: 10},
			Expected: "OFFSET 10",
		},
		"both limit and offset ": {
			Limit:    query.Limit{Limit: 5, Offset: 10},
			Expected: "LIMIT 5 OFFSET 10",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			qry, err := compileLimit(test.Limit)
			if err != nil {
				t.Fatalf("Failed to compile sort: %v\n%s", test.Limit, err)
			}
			if qry != test.Expected {
				t.Fatalf("SQL not equal\n%s\n%s", qry, test.Expected)
			}
		})
	}
}
