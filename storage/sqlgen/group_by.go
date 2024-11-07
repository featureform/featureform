// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package sqlgen

import (
	"fmt"

	"github.com/featureform/storage/query"
)

func compileGroupBy(groupBy query.GroupBy) (string, error) {
	// list passes in a non-nil groupBy struct, if name is empty, dimiss.
	if groupBy.Name == "" {
		return "", nil
	}
	query := fmt.Sprintf("group by %s", groupBy.Name)
	return query, nil
}
