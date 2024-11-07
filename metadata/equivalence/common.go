// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package equivalence

import (
	"github.com/featureform/logging"
	pb "github.com/featureform/metadata/proto"
)

var logger = logging.NewLogger("equivalence")

type nameVariant struct {
	Name    string
	Variant string
}

func nameVariantFromProto(proto *pb.NameVariant) nameVariant {
	return nameVariant{
		Name:    proto.Name,
		Variant: proto.Variant,
	}
}

func nameVariantsFromProto(proto []*pb.NameVariant) []nameVariant {
	var nameVariants []nameVariant
	for _, nv := range proto {
		nameVariants = append(nameVariants, nameVariantFromProto(nv))
	}
	return nameVariants
}

func (n nameVariant) IsEquivalent(other Equivalencer) bool {
	otherNameVariant, ok := other.(nameVariant)
	if !ok {
		return false
	}
	return n.Name == otherNameVariant.Name && n.Variant == otherNameVariant.Variant
}
