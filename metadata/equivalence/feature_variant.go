// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package equivalence

import (
	"reflect"

	"github.com/google/go-cmp/cmp"

	"github.com/featureform/fferr"
	pb "github.com/featureform/metadata/proto"
	"github.com/featureform/provider/types"
)

type featureVariant struct {
	Name                    string
	Provider                string
	ValueType               types.ValueType
	Entity                  string
	ComputationMode         string // TODO move definition from metadata to common
	Location                featureLocation
	ResourceSnowflakeConfig resourceSnowflakeConfig
}

func FeatureVariantFromProto(proto *pb.FeatureVariant) (featureVariant, error) {
	var location featureLocation
	switch l := proto.Location.(type) {
	case *pb.FeatureVariant_Columns:
		location = column{
			Entity: l.Columns.Entity,
			Value:  l.Columns.Value,
			Ts:     l.Columns.Ts,
		}
	case *pb.FeatureVariant_Function:
		location = pythonFunction{Query: l.Function.Query}
	case *pb.FeatureVariant_Stream:
		location = stream{OfflineProvider: l.Stream.OfflineProvider}
	}

	valueType, err := types.ValueTypeFromProto(proto.Type)
	if err != nil {
		return featureVariant{}, fferr.NewParsingError(err)
	}

	return featureVariant{
		Name:                    proto.Name,
		Provider:                proto.Provider,
		ValueType:               valueType,
		Entity:                  proto.Entity,
		ComputationMode:         proto.Mode.String(),
		Location:                location,
		ResourceSnowflakeConfig: resourceSnowflakeConfigFromProto(proto.ResourceSnowflakeConfig),
	}, nil
}

func (f featureVariant) IsEquivalent(other Equivalencer) bool {
	otherFeatureVariant, ok := other.(featureVariant)
	if !ok {
		return false
	}

	opts := cmp.Options{
		cmp.Comparer(func(f1, f2 featureVariant) bool {
			return f1.Name == f2.Name &&
				f1.Provider == f2.Provider &&
				f1.ValueType == f2.ValueType &&
				f1.Entity == f2.Entity &&
				f1.ComputationMode == f2.ComputationMode &&
				f1.Location.IsEquivalent(f2.Location) &&
				reflect.DeepEqual(f1.ResourceSnowflakeConfig, f2.ResourceSnowflakeConfig)
		}),
	}

	isEqual := cmp.Equal(f, otherFeatureVariant, opts)

	if !isEqual {
		diff := cmp.Diff(f, otherFeatureVariant, opts)
		logger.With("type", "feature_variant").Debug("Unequal feature variants", diff)
	}

	return isEqual
}

type featureLocation interface {
	Equivalencer
	IsFeatureLocation()
}

type column struct {
	Entity string
	Value  string
	Ts     string
}

func (c column) IsFeatureLocation() {}

func (c column) IsEquivalent(other Equivalencer) bool {
	otherColumn, ok := other.(column)
	if !ok {
		return false
	}
	return c.Entity == otherColumn.Entity &&
		c.Value == otherColumn.Value &&
		c.Ts == otherColumn.Ts
}

type pythonFunction struct {
	Query []byte
}

func (p pythonFunction) IsFeatureLocation() {}

func (p pythonFunction) IsEquivalent(other Equivalencer) bool {
	otherPythonFunction, ok := other.(pythonFunction)
	if !ok {
		return false
	}
	return reflect.DeepEqual(p.Query, otherPythonFunction.Query)
}

type stream struct {
	OfflineProvider string
}

func (s stream) IsFeatureLocation() {}

func (s stream) IsEquivalent(other Equivalencer) bool {
	otherStream, ok := other.(stream)
	if !ok {
		return false
	}
	return s.OfflineProvider == otherStream.OfflineProvider
}
