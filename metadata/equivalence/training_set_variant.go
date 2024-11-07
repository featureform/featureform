// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package equivalence

import (
	"reflect"

	pb "github.com/featureform/metadata/proto"
	"github.com/google/go-cmp/cmp"
)

type trainingSetVariant struct {
	Name        string
	Features    []nameVariant
	Label       nameVariant
	LagFeatures []featureLag
}

func TrainingSetVariantFromProto(proto *pb.TrainingSetVariant) (trainingSetVariant, error) {
	return trainingSetVariant{
		Name:        proto.Name,
		Features:    nameVariantsFromProto(proto.Features),
		Label:       nameVariantFromProto(proto.Label),
		LagFeatures: featureLagsFromProto(proto.FeatureLags),
	}, nil
}

func (t trainingSetVariant) IsEquivalent(other Equivalencer) bool {
	otherTrainingSetVariant, ok := other.(trainingSetVariant)
	if !ok {
		return false
	}

	opts := cmp.Options{
		cmp.Comparer(func(t1, t2 trainingSetVariant) bool {
			return t1.Name == t2.Name &&
				reflect.DeepEqual(t1.Features, t2.Features) &&
				reflect.DeepEqual(t1.LagFeatures, t2.LagFeatures) &&
				t1.Label.IsEquivalent(t2.Label)
		}),
	}

	isEqual := cmp.Equal(t, otherTrainingSetVariant, opts)

	if !isEqual {
		diff := cmp.Diff(t, otherTrainingSetVariant, opts)
		logger.With("type", "training_set_variant").Debug("Unequal training_set variants", diff)
	}

	return isEqual
}

type featureLag struct {
	Feature string
	Name    string
	Variant string
	Lag     string
}

func featureLagFromProto(proto *pb.FeatureLag) featureLag {
	return featureLag{
		Feature: proto.Feature,
		Name:    proto.Name,
		Variant: proto.Variant,
		Lag:     proto.Lag.String(),
	}
}

func featureLagsFromProto(proto []*pb.FeatureLag) []featureLag {
	var featureLags []featureLag
	for _, fl := range proto {
		featureLags = append(featureLags, featureLagFromProto(fl))
	}
	return featureLags
}

func (f featureLag) IsEquivalent(other Equivalencer) bool {
	otherFeatureLag, ok := other.(featureLag)
	if !ok {
		return false
	}
	return f.Feature == otherFeatureLag.Feature &&
		f.Name == otherFeatureLag.Name &&
		f.Variant == otherFeatureLag.Variant &&
		f.Lag == otherFeatureLag.Lag
}
