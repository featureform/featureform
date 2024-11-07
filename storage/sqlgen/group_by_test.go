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

type GroupByTest struct {
	GroupBy  query.GroupBy
	Expected string
}

func TestCompileGroupBy(t *testing.T) {
	tests := map[string]GroupByTest{
		"basic name": {
			GroupBy:  query.GroupBy{Name: "total_count"},
			Expected: "group by total_count",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			qry, err := compileGroupBy(tt.GroupBy)
			if err != nil {
				t.Fatalf("failed to compile group by: %v\n%s", tt.GroupBy, err)
			}
			if qry != tt.Expected {
				t.Fatalf("SQL not equal\n%s\n%s", qry, tt.Expected)
			}
		})
	}
}
