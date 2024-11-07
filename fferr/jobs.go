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

func NewJobDoesNotExistError(key string, err error) *JobDoesNotExistError {
	if err == nil {
		err = fmt.Errorf("job does not exist")
	}
	baseError := newBaseError(err, JOB_DOES_NOT_EXIST, codes.NotFound)
	baseError.AddDetail("key", key)

	return &JobDoesNotExistError{
		baseError,
	}
}

type JobDoesNotExistError struct {
	baseError
}

func NewResourceAlreadyCompleteError(resourceName, resourceVariant string, resourceType ResourceType, err error) *ResourceAlreadyCompleteError {
	if err == nil {
		err = fmt.Errorf("resource already complete")
	}
	baseError := newBaseError(err, RESOURCE_ALREADY_COMPLETE, codes.Internal)
	baseError.AddDetail("resource_name", resourceName)
	baseError.AddDetail("resource_variant", resourceVariant)
	baseError.AddDetail("resource_type", string(resourceType))

	return &ResourceAlreadyCompleteError{
		baseError,
	}
}

type ResourceAlreadyCompleteError struct {
	baseError
}

func NewResourceAlreadyFailedError(resourceName, resourceVariant string, resourceType ResourceType, err error) *ResourceAlreadyFailedError {
	if err == nil {
		err = fmt.Errorf("resource already failed")
	}
	baseError := newBaseError(err, RESOURCE_ALREADY_FAILED, codes.Internal)
	baseError.AddDetail("resource_name", resourceName)
	baseError.AddDetail("resource_variant", resourceVariant)
	baseError.AddDetail("resource_type", string(resourceType))

	return &ResourceAlreadyFailedError{
		baseError,
	}
}

type ResourceAlreadyFailedError struct {
	baseError
}

func NewResourceNotReadyError(resourceName, resourceVariant string, resourceType ResourceType, err error) *ResourceNotReadyError {
	if err == nil {
		err = fmt.Errorf("resource not ready")
	}
	baseError := newBaseError(err, RESOURCE_NOT_READY, codes.Internal)
	baseError.AddDetail("resource_name", resourceName)
	baseError.AddDetail("resource_variant", resourceVariant)
	baseError.AddDetail("resource_type", string(resourceType))

	return &ResourceNotReadyError{
		baseError,
	}
}

type ResourceNotReadyError struct {
	baseError
}

func NewResourceFailedError(resourceName, resourceVariant string, resourceType ResourceType, err error) *ResourceFailedError {
	if err == nil {
		err = fmt.Errorf("resource failed")
	}
	baseError := newBaseError(err, RESOURCE_FAILED, codes.Internal)
	baseError.AddDetail("resource_name", resourceName)
	baseError.AddDetail("resource_variant", resourceVariant)
	baseError.AddDetail("resource_type", string(resourceType))

	return &ResourceFailedError{
		baseError,
	}
}

func NewResourceFailedErrorf(resourceName, resourceVariant string, resourceType ResourceType, format string, a ...any) *ResourceFailedError {
	err := fmt.Errorf(format, a...)
	return NewResourceFailedError(resourceName, resourceVariant, resourceType, err)
}

type ResourceFailedError struct {
	baseError
}

func NewDependencyFailedErrorf(format string, a ...any) *DependencyFailedError {
	err := fmt.Errorf(format, a...)
	baseError := newBaseError(err, DEPENDENCY_FAILED, codes.Internal)

	return &DependencyFailedError{
		baseError,
	}
}

type DependencyFailedError struct {
	baseError
}

func NewTaskRunFailedError(taskId string, runId string, err error) *TaskRunFailedError {
	baseError := newBaseError(err, TASK_RUN_FAILED, codes.Internal)
	baseError.AddDetail("task_id", taskId)
	baseError.AddDetail("run_id", runId)

	return &TaskRunFailedError{
		baseError,
	}
}

type TaskRunFailedError struct {
	baseError
}

func NewJobAlreadyExistsError(key string, err error) *JobAlreadyExistsError {
	if err == nil {
		err = fmt.Errorf("job already exists")
	}
	baseError := newBaseError(err, JOB_ALREADY_EXISTS, codes.AlreadyExists)
	baseError.AddDetail("key", key)

	return &JobAlreadyExistsError{
		baseError,
	}
}

type JobAlreadyExistsError struct {
	baseError
}

type InvalidJobTargetError struct {
	baseError
}

func NewInvalidJobTargetError(target interface{}) *InvalidJobTargetError {
	err := fmt.Errorf("invalid target for task")
	baseError := newBaseError(err, INVALID_JOB_TARGET, codes.Internal)
	baseError.AddDetail("type", fmt.Sprintf("%T", target))

	return &InvalidJobTargetError{
		baseError,
	}
}
