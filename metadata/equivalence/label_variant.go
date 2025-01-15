// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package equivalence

import (
	"github.com/featureform/fferr"
	pb "github.com/featureform/metadata/proto"
	"github.com/featureform/provider/types"
	"github.com/google/go-cmp/cmp"
	"reflect"
)

type labelVariant struct {
	Name                    string
	Source                  nameVariant
	Columns                 column
	Entity                  string
	Type                    types.ValueType
	ResourceSnowflakeConfig resourceSnowflakeConfig
}

func LabelVariantFromProto(proto *pb.LabelVariant) (labelVariant, error) {
	valueType, err := types.ValueTypeFromProto(proto.Type)
	if err != nil {
		return labelVariant{}, fferr.NewParsingError(err)
	}

	return labelVariant{
		Name:                    proto.Name,
		Source:                  nameVariantFromProto(proto.Source),
		Entity:                  proto.Entity,
		Type:                    valueType,
		ResourceSnowflakeConfig: resourceSnowflakeConfigFromProto(proto.ResourceSnowflakeConfig),
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
				l1.Type == l2.Type &&
				reflect.DeepEqual(l1.ResourceSnowflakeConfig, l2.ResourceSnowflakeConfig)
		}),
	}

	isEqual := cmp.Equal(l, otherLabelVariant, opts)

	if !isEqual {
		diff := cmp.Diff(l, otherLabelVariant, opts)
		logger.With("type", "label_variant").Debug("Unequal label variants", diff)
	}

	return isEqual
}
