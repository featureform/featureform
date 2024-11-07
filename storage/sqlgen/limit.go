// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package sqlgen

import (
	"fmt"
	"strings"

	"github.com/featureform/fferr"
	"github.com/featureform/storage/query"
)

func compileLimit(limit query.Limit) (string, error) {
	if limit.Offset < 0 || limit.Limit < 0 {
		return "", fferr.NewInternalErrorf("Offset and Limit cannot be negative: %+v", limit)
	}
	// We use parts and strings.Join to gracefully handle not having trailing spaces.
	queryParts := make([]string, 0, 2)
	if limit.Limit != 0 {
		query := fmt.Sprintf("LIMIT %d", limit.Limit)
		queryParts = append(queryParts, query)
	}
	if limit.Offset != 0 {
		query := fmt.Sprintf("OFFSET %d", limit.Offset)
		queryParts = append(queryParts, query)
	}
	return strings.Join(queryParts, " "), nil
}
