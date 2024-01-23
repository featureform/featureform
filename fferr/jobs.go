package fferr

import (
	"fmt"

	"google.golang.org/grpc/codes"
)

func NewJobDoesNotExistError(key string, err error) *JobDoesNotExistError {
	if err == nil {
		err = fmt.Errorf("initial job does not exist error")
	}
	baseError := newBaseGRPCError(err, JOB_DOES_NOT_EXIST, codes.NotFound)
	baseError.AddDetail("key", key)

	return &JobDoesNotExistError{
		baseError,
	}
}

type JobDoesNotExistError struct {
	baseGRPCError
}

func NewResourceAlreadyCompleteError(resourceName, resourceVariant string, resourceType ResourceType, err error) *ResourceAlreadyCompleteError {
	if err == nil {
		err = fmt.Errorf("initial resource already complete error")
	}
	baseError := newBaseGRPCError(err, RESOURCE_ALREADY_COMPLETE, codes.Internal)
	baseError.AddDetail("resource_name", resourceName)
	baseError.AddDetail("resource_variant", resourceVariant)
	baseError.AddDetail("resource_type", string(resourceType))

	return &ResourceAlreadyCompleteError{
		baseError,
	}
}

type ResourceAlreadyCompleteError struct {
	baseGRPCError
}

func NewResourceAlreadyFailedError(resourceName, resourceVariant string, resourceType ResourceType, err error) *ResourceAlreadyFailedError {
	if err == nil {
		err = fmt.Errorf("initial resource already failed error")
	}
	baseError := newBaseGRPCError(err, RESOURCE_ALREADY_FAILED, codes.Internal)
	baseError.AddDetail("resource_name", resourceName)
	baseError.AddDetail("resource_variant", resourceVariant)
	baseError.AddDetail("resource_type", string(resourceType))

	return &ResourceAlreadyFailedError{
		baseError,
	}
}

type ResourceAlreadyFailedError struct {
	baseGRPCError
}

func NewResourceNotReadyError(resourceName, resourceVariant string, resourceType ResourceType, err error) *ResourceNotReadyError {
	if err == nil {
		err = fmt.Errorf("initial resource not ready error")
	}
	baseError := newBaseGRPCError(err, RESOURCE_NOT_READY, codes.Internal)
	baseError.AddDetail("resource_name", resourceName)
	baseError.AddDetail("resource_variant", resourceVariant)
	baseError.AddDetail("resource_type", string(resourceType))

	return &ResourceNotReadyError{
		baseError,
	}
}

type ResourceNotReadyError struct {
	baseGRPCError
}

func NewResourceFailedError(resourceName, resourceVariant string, resourceType ResourceType, err error) *ResourceFailedError {
	if err == nil {
		err = fmt.Errorf("initial resource failed error")
	}
	baseError := newBaseGRPCError(err, RESOURCE_FAILED, codes.Internal)
	baseError.AddDetail("resource_name", resourceName)
	baseError.AddDetail("resource_variant", resourceVariant)
	baseError.AddDetail("resource_type", string(resourceType))

	return &ResourceFailedError{
		baseError,
	}
}

type ResourceFailedError struct {
	baseGRPCError
}

func NewJobAlreadyExistsError(key string, err error) *JobAlreadyExistsError {
	if err == nil {
		err = fmt.Errorf("initial job already exists error")
	}
	baseError := newBaseGRPCError(err, JOB_ALREADY_EXISTS, codes.AlreadyExists)
	baseError.AddDetail("key", key)

	return &JobAlreadyExistsError{
		baseError,
	}
}

type JobAlreadyExistsError struct {
	baseGRPCError
}
