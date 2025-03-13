// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2025 FeatureForm Inc.
//

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package arrow

import (
	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	"github.com/featureform/provider/dataset"
	"github.com/featureform/provider/location"
	"github.com/featureform/types"

	arrowlib "github.com/apache/arrow/go/v18/arrow"
)

type Dataset struct {
	loc    location.Location
	schema types.Schema
}

func (ds *Dataset) Location() location.Location {
	return ds.loc
}

func (ds *Dataset) Iterator() (dataset.Iterator, error) {
	return nil, nil
}

func (ds *Dataset) Schema() (types.Schema, error) {
	return ds.schema, nil
}
