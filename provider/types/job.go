// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package types

type Job string

const (
	Materialize       Job = "Materialization"
	Transform         Job = "Transformation"
	CreateTrainingSet Job = "Training Set"
	BatchFeatures     Job = "Batch Features"
)
