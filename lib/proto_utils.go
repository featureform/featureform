// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package lib

import (
	"github.com/featureform/fferr"
	"github.com/repeale/fp-go"
	"google.golang.org/protobuf/proto"
)

// EqualProtoContents compares two slices of proto messages to check that the contents are the same.
// It excludes the order and dedupes.
func EqualProtoContents[T proto.Message](a, b []T) (bool, error) {
	// We marshal the proto messages to strings so that we can compare them in a set
	var errors error
	marshaledA := fp.Map[T, string](func(x T) string {
		marshal, err := proto.Marshal(x)
		if err != nil {
			errors = err
		}
		return string(marshal)
	})(a)

	marshaledB := fp.Map[T, string](func(x T) string {
		marshal, err := proto.Marshal(x)
		if err != nil {
			errors = err
		}
		return string(marshal)
	})(b)

	if errors != nil {
		return false, fferr.NewInternalErrorf("errors marshaling proto messages: %v", errors)
	}

	setA := ToSet[string](marshaledA)
	setB := ToSet[string](marshaledB)
	return setA.Equal(setB), nil
}

func EqualProtoSlices[T proto.Message](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}

	for i, x := range a {
		if !proto.Equal(x, b[i]) {
			return false
		}
	}
	return true
}
