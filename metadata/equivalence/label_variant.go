// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package equivalence

import (
	"reflect"

	"github.com/featureform/fferr"
	pb "github.com/featureform/metadata/proto"
	"github.com/featureform/provider/types"
	"github.com/google/go-cmp/cmp"
)

type entityMapping struct {
	Name         string
	EntityColumn string
}

type entityMappings struct {
	Mappings        []entityMapping
	ValueColumn     string
	TimestampColumn string
}

func entityMappingsFromProto(proto *pb.EntityMappings) entityMappings {
	if proto == nil {
		return entityMappings{}
	}

	mappings := make([]entityMapping, 0)
	if proto.Mappings != nil {
		for _, mapping := range proto.Mappings {
			if mapping == nil {
				continue
			}
			mappings = append(mappings, entityMapping{
				Name:         mapping.Name,
				EntityColumn: mapping.EntityColumn,
			})
		}
	}

	return entityMappings{
		Mappings:        mappings,
		ValueColumn:     proto.ValueColumn,
		TimestampColumn: proto.TimestampColumn,
	}
}

type labelVariant struct {
	Name                    string
	Source                  nameVariant
	Columns                 column
	Entity                  string
	Type                    types.ValueType
	ResourceSnowflakeConfig resourceSnowflakeConfig
	EntityMappings          entityMappings
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
		EntityMappings:          entityMappingsFromProto(proto.GetEntityMappings()),
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
				reflect.DeepEqual(l1.Columns, l2.Columns) &&
				reflect.DeepEqual(l1.ResourceSnowflakeConfig, l2.ResourceSnowflakeConfig) &&
				reflect.DeepEqual(l1.EntityMappings, l2.EntityMappings)
		}),
	}

	isEqual := cmp.Equal(l, otherLabelVariant, opts)

	if !isEqual {
		diff := cmp.Diff(l, otherLabelVariant, opts)
		logger.With("type", "label_variant").Debug("Unequal label variants", diff)
	}

	return isEqual
}
