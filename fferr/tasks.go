// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package fferr

import (
	"fmt"

	"google.golang.org/grpc/codes"
)

type ResourceTaskFailedError struct {
	baseError
}

func NewResourceTaskFailedError(name, variant, resType string, err error) *ResourceTaskFailedError {
	if err == nil {
		err = fmt.Errorf("resource %s (%s) type %s failed to run: %w", name, variant, resType, err)
	}
	baseError := newBaseError(err, RESOURCE_TASK_FAILED, codes.Internal)

	return &ResourceTaskFailedError{
		baseError,
	}
}

type NoRunsForTaskError struct {
	baseError
}

func NewNoRunsForTaskError(taskID string) *NoRunsForTaskError {
	err := fmt.Errorf("no runs found for task %s", taskID)
	baseError := newBaseError(err, NO_RUNS_FOR_TASK, codes.Internal)

	return &NoRunsForTaskError{
		baseError,
	}
}
