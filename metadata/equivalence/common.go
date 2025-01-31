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

type resourceSnowflakeConfig struct {
	DynamicTableConfig snowflakeDynamicTableConfig
	Warehouse          string
}

type snowflakeDynamicTableConfig struct {
	TargetLag   string
	RefreshMode string
	Initialize  string
}

func resourceSnowflakeConfigFromProto(proto *pb.ResourceSnowflakeConfig) resourceSnowflakeConfig {
	if proto == nil {
		return resourceSnowflakeConfig{}
	}

	dynamicTableConfig := proto.DynamicTableConfig
	if dynamicTableConfig == nil {
		return resourceSnowflakeConfig{
			Warehouse: proto.Warehouse,
		}
	}

	return resourceSnowflakeConfig{
		DynamicTableConfig: snowflakeDynamicTableConfig{
			TargetLag:   dynamicTableConfig.TargetLag,
			RefreshMode: dynamicTableConfig.RefreshMode.String(),
			Initialize:  dynamicTableConfig.Initialize.String(),
		},
		Warehouse: proto.Warehouse,
	}
}

func (s snowflakeDynamicTableConfig) IsEquivalent(other Equivalencer) bool {
	otherConfig, ok := other.(snowflakeDynamicTableConfig)
	if !ok {
		return false
	}
	return s.TargetLag == otherConfig.TargetLag &&
		s.RefreshMode == otherConfig.RefreshMode &&
		s.Initialize == otherConfig.Initialize
}
