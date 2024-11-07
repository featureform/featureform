// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package sqlgen

import (
	"fmt"

	"github.com/featureform/fferr"
	"github.com/featureform/storage/query"
)

func compileColumn(clm query.Column) (string, error) {
	switch casted := clm.(type) {
	case query.JSONColumn:
		return compileJSONColumn(casted)
	case query.SQLColumn:
		return casted.Column, nil
	default:
		return "", fferr.NewInternalErrorf("Unsupported column type in SQL storage: %T", casted)
	}
}

func compileJSONColumn(clm query.JSONColumn) (string, error) {
	qry := "value::json"

	for i, step := range clm.Path {
		operator := "->"
		isLastStep := i == len(clm.Path)-1

		// use "->>" (scalar operator) to convert the final item into a scalar. We'll then cast it if needed.
		// In the case of a jsonString we need to convert it to a scalar string, then cast it into JSON to parse it.
		// If the last step is an Object, we dont need to convert it into a scalar string.
		if clm.Type == query.Object && isLastStep {
			operator = "b->"
		} else if step.IsJsonString || isLastStep {
			operator = "->>"
		}

		// add the operator and key
		qry += fmt.Sprintf("%s'%s'", operator, step.Key)

		// in the case where we have a JSON string that isn't the last step. We'll need to parse it into JSON
		// so that we can continue to path into it. This parses the JSON string into a JSON object that we can
		// traverse using the standard -> and ->> operators in SQL.
		if step.IsJsonString && !isLastStep {
			qry = fmt.Sprintf("(%s)::json", qry)
		}
	}

	switch clm.Type {
	case query.String:
		qry = fmt.Sprintf("(%s)", qry)
	case query.Object:
		qry = fmt.Sprintf("(%s)", qry)
	case query.Int:
		qry = fmt.Sprintf("(%s)::int", qry)
	case query.Timestamp:
		qry = fmt.Sprintf("(%s)::timestamp", qry)
	default:
		return "", fferr.NewInternalErrorf("Unsupported JSON value type in SQL storage: %s", clm.Type)
	}

	return qry, nil
}
