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

func compileSort(sort query.Sort) (string, error) {
	if sort == nil {
		return "", nil
	}
	switch casted := sort.(type) {
	case query.KeySort:
		return compileKeySort(casted)
	case query.ValueSort:
		return compileValueSort(casted)
	default:
		return "", fferr.NewInternalErrorf("Unsupported sort type in SQL storage: %T", casted)
	}
}

func compileKeySort(sort query.KeySort) (string, error) {
	dirStr, err := compileSortDirection(sort.Direction())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ORDER BY key %s", dirStr), nil
}

func compileValueSort(sort query.ValueSort) (string, error) {
	dirStr, err := compileSortDirection(sort.Direction())
	if err != nil {
		return "", err
	}
	var clmStr string
	if sort.Column == nil {
		clmStr = "value"
	} else {
		compiledClm, err := compileColumn(sort.Column)
		if err != nil {
			return "", err
		}
		clmStr = compiledClm
	}
	return fmt.Sprintf("ORDER BY %s %s", clmStr, dirStr), nil
}

func compileSortDirection(dir query.SortDirection) (string, error) {
	switch dir {
	case query.Asc:
		return "ASC", nil
	case query.Desc:
		return "DESC", nil
	default:
		return "", fferr.NewInternalErrorf("Unknown sort type in SQL storage: %s", dir)
	}
}
