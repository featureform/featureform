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

type ResourceFailedError struct {
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
