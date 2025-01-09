// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package serialization

import (
	vt "github.com/featureform/provider/types"
	"strconv"
)

// SerializeVersion is used to specify what method of serializing and deserializing values
// that we're using.
type SerializeVersion int

func (v SerializeVersion) String() string {
	return strconv.Itoa(int(v))
}

// Serializer provides methods to serialize and deserialize values.
type Serializer[T any] interface {
	Version() SerializeVersion
	Serialize(t vt.ValueType, value any) (T, error)
	Deserialize(t vt.ValueType, value T) (any, error)
}
