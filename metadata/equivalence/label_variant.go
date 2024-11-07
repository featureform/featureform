// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package equivalence

import (
	pb "github.com/featureform/metadata/proto"
	"github.com/google/go-cmp/cmp"
)

type labelVariant struct {
	Name    string
	Source  nameVariant
	Columns column
	Entity  string
	Type    string
}

func LabelVariantFromProto(proto *pb.LabelVariant) (labelVariant, error) {
	return labelVariant{
		Name:   proto.Name,
		Source: nameVariantFromProto(proto.Source),
		Entity: proto.Entity,
		Type:   proto.Type.String(),
	}, nil
}

func (l labelVariant) IsEquivalent(other Equivalencer) bool {
	otherLabelVariant, ok := other.(labelVariant)
	if !ok {
		return false
	}

	opts := cmp.Options{
		cmp.Comparer(func(l1, l2 labelVariant) bool {
			return l1.Name == l2.Name &&
				l1.Source.IsEquivalent(l2.Source) &&
				l1.Entity == l2.Entity &&
				l1.Type == l2.Type
		}),
	}

	isEqual := cmp.Equal(l, otherLabelVariant, opts)

	if !isEqual {
		diff := cmp.Diff(l, otherLabelVariant, opts)
		logger.With("type", "label_variant").Debug("Unequal label variants", diff)
	}

	return isEqual
}
