// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package spawner

import (
	"github.com/featureform/metadata"
	"github.com/featureform/runner"
	"github.com/featureform/types"
)

type JobSpawner interface {
	GetJobRunner(jobName runner.RunnerName, config runner.Config, resourceId metadata.ResourceID) (types.Runner, error)
}
